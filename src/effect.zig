// Algebraic effects with compile-time type safety.
//
// Declare effects as types:
//   const Write = vt.Perform(Request, WriteResult);
//   const Log = vt.Emit(LogEntry);
//
// The effect type encodes kind, value type, and resume type.
// Handler binding is type-checked at compile time.

const std = @import("std");
const fiber_mod = @import("fiber.zig");

// ============================================================
// §1. Effect type declarations
// ============================================================

pub fn Perform(comptime T: type, comptime R: type) type {
    return struct {
        pub const Value = T;
        pub const Resume = R;
        pub const kind: EffectKind = .perform;
    };
}

pub fn Emit(comptime T: type) type {
    return struct {
        pub const Value = T;
        pub const kind: EffectKind = .emit;
    };
}

pub const EffectKind = enum(u8) { perform, emit };

fn effectId(comptime E: type) usize {
    return @intFromPtr(&struct { const _: type = E; }._);
}

// ============================================================
// §2. RawEffect — type-erased, yielded through the fiber
// ============================================================

pub const RawEffect = struct {
    id: usize,
    kind: EffectKind,
    value_ptr: *anyopaque,
    value_size: usize,
    /// For perform: pointer to the resume slot on the performer's
    /// stack frame. The continuation writes the resume value here.
    resume_ptr: ?*anyopaque = null,
};

pub const EffectFiber = fiber_mod.Fiber(RawEffect, void);
pub const EffectBodyFn = *const fn (*EffectContext) void;

/// Create an effect fiber from a body that receives an EffectContext
/// directly. Equivalent to EffectFiber.init with a wrapper that
/// constructs the context.
pub fn initFiber(body: EffectBodyFn, stack_size: usize) !EffectFiber {
    const Static = struct {
        threadlocal var current_body: EffectBodyFn = undefined;
    };
    Static.current_body = body;

    return EffectFiber.init(&struct {
        fn wrapper(h: *EffectFiber.Handle) void {
            const b = Static.current_body;
            var ctx = EffectContext.init(h);
            b(&ctx);
        }
    }.wrapper, stack_size);
}

/// Create an effect fiber with the default stack size.
pub fn initFiberDefault(body: EffectBodyFn) !EffectFiber {
    return initFiber(body, 0);
}

// ============================================================
// §3. Continuation — typed, given to perform handlers
// ============================================================

pub fn Cont(comptime E: type) type {
    const R = E.Resume;

    return struct {
        fiber: *EffectFiber,
        resume_ptr: ?*anyopaque,
        alive: bool = true,
        next_effect: ?RawEffect = null,

        const Self = @This();

        // The continuation is only valid for the duration of the handler
        // call. It is stack-allocated inside the erased wrapper and
        // cannot be stored, returned, or accessed after the handler
        // returns. If the handler returns without calling resume() or
        // drop(), the wrapper auto-drops (destroying the fiber).
        //
        // Handlers may perform blocking or async IO (e.g. via std.Io)
        // before resuming. The fiber's stack is untouched during this
        // time — std.Io operates on the handler's own stack. When the
        // handler's IO completes and it calls resume(), the fiber
        // continues normally.

        /// Resume the performer, delivering a value.
        pub fn @"resume"(self: *Self, val: R) void {
            if (!self.alive) return;
            self.alive = false;
            if (@sizeOf(R) > 0) {
                if (self.resume_ptr) |ptr| {
                    const slot: *R = @ptrCast(@alignCast(ptr));
                    slot.* = val;
                }
            }
            self.next_effect = self.fiber.resumeVoid();
        }

        /// Drop the continuation without resuming. The fiber is
        /// destroyed and its stack freed. The performer never returns
        /// from perform().
        ///
        /// Warning: defers and destructors in the fiber will NOT run.
        /// Resources held across a perform boundary will leak. Use
        /// arena allocators for memory, and avoid holding file handles
        /// or locks across perform calls.
        pub fn drop(self: *Self) void {
            if (!self.alive) return;
            self.alive = false;
            self.fiber.deinit();
        }

        pub fn isAlive(self: *const Self) bool {
            return self.alive;
        }
    };
}

// ============================================================
// §4. EffectContext — used inside effectful fiber bodies
// ============================================================

pub const EffectContext = struct {
    handle: *EffectFiber.Handle,

    pub fn init(handle: *EffectFiber.Handle) EffectContext {
        return .{ .handle = handle };
    }

    /// Perform: transfer ownership, suspend, receive a resume value.
    pub fn perform(self: *EffectContext, comptime E: type, val: E.Value) E.Resume {
        comptime std.debug.assert(E.kind == .perform);
        var storage: E.Value = val;
        var result: E.Resume = undefined;
        _ = self.handle.yield(.{
            .id = effectId(E),
            .kind = .perform,
            .value_ptr = @ptrCast(&storage),
            .value_size = @sizeOf(E.Value),
            .resume_ptr = if (@sizeOf(E.Resume) > 0) @ptrCast(&result) else null,
        });
        return result;
    }

    /// Emit: yield to observers, then resume. Synchronous from
    /// the fiber's perspective — returns once all observers run.
    pub fn emit(self: *EffectContext, comptime E: type, val: E.Value) void {
        comptime std.debug.assert(E.kind == .emit);
        var storage: E.Value = val;
        _ = self.handle.yield(.{
            .id = effectId(E),
            .kind = .emit,
            .value_ptr = @ptrCast(&storage),
            .value_size = @sizeOf(E.Value),
        });
    }
};

// ============================================================
// §5. Handler types
// ============================================================

pub fn PerformHandlerFn(comptime E: type) type {
    return *const fn (value: *E.Value, cont: *Cont(E), ctx: ?*anyopaque) void;
}

pub fn EmitHandlerFn(comptime E: type) type {
    return *const fn (value: *const E.Value, ctx: ?*anyopaque) void;
}

// ============================================================
// §6. HandlerSet — type-safe binding, type-erased storage
// ============================================================

const ErasedHandlerFn = *const fn (raw: *const RawEffect, fiber: *EffectFiber, ctx: ?*anyopaque) ?RawEffect;

const Binding = struct {
    id: usize,
    kind: EffectKind,
    handler: ErasedHandlerFn,
    ctx: ?*anyopaque,
};

pub const HandlerSet = struct {
    bindings: std.ArrayListUnmanaged(Binding),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) HandlerSet {
        return .{ .bindings = .{}, .allocator = allocator };
    }

    pub fn deinit(self: *HandlerSet) void {
        self.bindings.deinit(self.allocator);
    }

    /// Bind a typed perform handler. Compile error if handler
    /// signature doesn't match E's value and resume types.
    pub fn onPerform(self: *HandlerSet, comptime E: type, handler: PerformHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn erased(raw: *const RawEffect, fiber: *EffectFiber, user_ctx: ?*anyopaque) ?RawEffect {
                const val: *E.Value = @ptrCast(@alignCast(raw.value_ptr));
                var cont = Cont(E){
                    .fiber = fiber,
                    .resume_ptr = raw.resume_ptr,
                };
                handler(val, &cont, user_ctx);
                if (cont.alive) {
                    // Handler returned without resuming or dropping.
                    // Drop the fiber to prevent leaks.
                    cont.drop();
                }
                return cont.next_effect;
            }
        };
        self.bindings.append(self.allocator, .{
            .id = effectId(E),
            .kind = .perform,
            .handler = Gen.erased,
            .ctx = ctx,
        }) catch @panic("OOM");
    }

    /// Bind a typed emit observer. Compile error if handler
    /// signature doesn't match E's value type.
    pub fn onEmit(self: *HandlerSet, comptime E: type, handler: EmitHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn erased(raw: *const RawEffect, _: *EffectFiber, user_ctx: ?*anyopaque) ?RawEffect {
                const val: *const E.Value = @ptrCast(@alignCast(raw.value_ptr));
                handler(val, user_ctx);
                return null; // emit handlers don't produce next effects
            }
        };
        self.bindings.append(self.allocator, .{
            .id = effectId(E),
            .kind = .emit,
            .handler = Gen.erased,
            .ctx = ctx,
        }) catch @panic("OOM");
    }
};

// ============================================================
// §7. Runner
// ============================================================

pub fn run(fib: *EffectFiber, handlers: *const HandlerSet) void {
    var maybe_eff = fib.start();
    while (maybe_eff) |eff| {
        switch (eff.kind) {
            .emit => {
                for (handlers.bindings.items) |binding| {
                    if (binding.kind == .emit and binding.id == eff.id) {
                        _ = binding.handler(&eff, fib, binding.ctx);
                    }
                }
                maybe_eff = fib.resumeVoid();
            },
            .perform => {
                for (handlers.bindings.items) |binding| {
                    if (binding.kind == .perform and binding.id == eff.id) {
                        const result = binding.handler(&eff, fib, binding.ctx);
                        maybe_eff = result;
                        break;
                    }
                } else {
                    // No handler found — resume with zeroed value
                    maybe_eff = fib.resumeVoid();
                }
            },
        }
    }
}

// ============================================================
// §8. Tests
// ============================================================

const testing = std.testing;

const GetInt = Perform([]const u8, i32);
const Log = Emit([]const u8);

test "perform: handler resumes with a value" {
    // The fiber performs twice; the handler provides 42 then 100.
    // The fiber emits each value back so the test can observe them.
    const Result = Emit(i32);

    const State = struct { sum: i32 = 0 };
    var state = State{};

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const a = ctx.perform(GetInt, "first");
            ctx.emit(Result, a);
            const b = ctx.perform(GetInt, "second");
            ctx.emit(Result, b);
        }
    }.body);
    defer fib.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    handlers.onPerform(GetInt, &struct {
        fn handle(_: *GetInt.Value, cont: *Cont(GetInt), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);

    handlers.onEmit(Result, &struct {
        fn handle(val: *const Result.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.sum += val.*;
        }
    }.handle, @ptrCast(&state));

    run(&fib, &handlers);

    try testing.expectEqual(@as(i32, 84), state.sum); // 42 + 42
}

test "emit: observers see values" {
    const State = struct { count: usize = 0 };
    var state = State{};

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Log, "one");
            ctx.emit(Log, "two");
            ctx.emit(Log, "three");
        }
    }.body);
    defer fib.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    handlers.onEmit(Log, &struct {
        fn handle(_: *const Log.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.count += 1;
        }
    }.handle, @ptrCast(&state));

    run(&fib, &handlers);

    try testing.expectEqual(@as(usize, 3), state.count);
}

test "mixed perform and emit" {
    const Add = Perform(i32, i32);
    const Trace = Emit(i32);

    const State = struct { trace_sum: i32 = 0 };
    var state = State{};

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Trace, 1);
            const sum = ctx.perform(Add, 10);
            ctx.emit(Trace, sum);
        }
    }.body);
    defer fib.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    handlers.onPerform(Add, &struct {
        fn handle(val: *Add.Value, cont: *Cont(Add), _: ?*anyopaque) void {
            cont.@"resume"(val.* + 5);
        }
    }.handle, null);

    handlers.onEmit(Trace, &struct {
        fn handle(val: *const Trace.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.trace_sum += val.*;
        }
    }.handle, @ptrCast(&state));

    run(&fib, &handlers);

    try testing.expectEqual(@as(i32, 16), state.trace_sum); // 1 + 15
}

test "perform handler can drop continuation" {
    const Abort = Perform(void, void);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.perform(Abort, {});
            @panic("unreachable after drop");
        }
    }.body);
    defer fib.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    handlers.onPerform(Abort, &struct {
        fn handle(_: *Abort.Value, cont: *Cont(Abort), _: ?*anyopaque) void {
            cont.drop();
        }
    }.handle, null);

    run(&fib, &handlers);

    try testing.expect(!fib.isAlive());
}

test "auto-drop when handler forgets to resume" {
    const Oops = Perform(u8, u8);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            _ = ctx.perform(Oops, 1);
            @panic("unreachable after auto-drop");
        }
    }.body);
    defer fib.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    handlers.onPerform(Oops, &struct {
        fn handle(_: *Oops.Value, _: *Cont(Oops), _: ?*anyopaque) void {
            // Intentionally do nothing — should auto-drop.
        }
    }.handle, null);

    run(&fib, &handlers);

    try testing.expect(!fib.isAlive());
}

test "distinct effect types have distinct IDs" {
    const A = Perform(u8, u8);
    const B = Perform(u8, u8);
    const C = Emit(u8);

    try testing.expect(effectId(A) != effectId(B));
    try testing.expect(effectId(A) != effectId(C));
    try testing.expect(effectId(B) != effectId(C));
    // Same type should produce the same ID.
    try testing.expectEqual(effectId(A), effectId(A));
}

test "multiple emit observers for same effect" {
    const Ping = Emit(void);

    const State = struct { a: usize = 0, b: usize = 0 };
    var state = State{};

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Ping, {});
        }
    }.body);
    defer fib.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    handlers.onEmit(Ping, &struct {
        fn handle(_: *const Ping.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.a += 1;
        }
    }.handle, @ptrCast(&state));

    handlers.onEmit(Ping, &struct {
        fn handle(_: *const Ping.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.b += 1;
        }
    }.handle, @ptrCast(&state));

    run(&fib, &handlers);

    try testing.expectEqual(@as(usize, 1), state.a);
    try testing.expectEqual(@as(usize, 1), state.b);
}