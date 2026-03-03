// Cooperative IO scheduling for effect fibers.
//
// FiberIo wraps a std.Io by copying its vtable at runtime and overriding
// only `await` and `cancel`. When a fiber awaits IO, it yields control
// to the IoScheduler, which can run other fibers while the IO completes.
//
// All gated behind std.Io.VTable availability (Zig 0.16+).

const std = @import("std");
const types = @import("effect/types.zig");
const dispatch_mod = @import("effect/dispatch.zig");
const handler_mod = @import("effect/handler.zig");

const RawEffect = types.RawEffect;
const EffectFiber = types.EffectFiber;
const EffectContext = types.EffectContext;
const EffectBodyFn = types.EffectBodyFn;
const EffectKind = types.EffectKind;
const HandlerSet = handler_mod.HandlerSet;

const has_std_io = types.has_std_io;

pub const FiberIo = if (has_std_io) FiberIoImpl else struct {};
pub const IoScheduler = if (has_std_io) IoSchedulerImpl else struct {};

/// Threadlocal holding the active FiberIo for the currently-executing fiber.
/// Set by IoScheduler before each resume, read by interceptAwait/interceptCancel.
pub threadlocal var active_fiber_io: if (has_std_io) ?*FiberIoImpl else void =
    if (has_std_io) null else {};

const FiberIoImpl = struct {
    vtable: std.Io.VTable,
    inner_userdata: ?*anyopaque,
    original_await: *const AwaitFn,
    original_cancel: *const CancelFn,
    fiber_handle: *EffectFiber.Handle,

    const AwaitFn = fn (?*anyopaque, *std.Io.AnyFuture, []u8, std.mem.Alignment) void;
    const CancelFn = fn (?*anyopaque, *std.Io.AnyFuture, []u8, std.mem.Alignment) void;

    pub fn init(inner: std.Io, handle: *EffectFiber.Handle) FiberIoImpl {
        // Copy the entire vtable at runtime
        var vt = inner.vtable.*;
        // Save originals before overriding
        const orig_await = vt.await;
        const orig_cancel = vt.cancel;
        // Override await and cancel with our interceptors
        vt.await = &interceptAwait;
        vt.cancel = &interceptCancel;

        return .{
            .vtable = vt,
            .inner_userdata = inner.userdata,
            .original_await = orig_await,
            .original_cancel = orig_cancel,
            .fiber_handle = handle,
        };
    }

    /// Return a std.Io that uses our modified vtable but the inner userdata.
    /// All 107 unmodified vtable entries work as-is since they receive the
    /// original userdata pointer.
    pub fn io(self: *FiberIoImpl) std.Io {
        return .{ .userdata = self.inner_userdata, .vtable = &self.vtable };
    }

    fn interceptAwait(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result_buf: []u8, alignment: std.mem.Alignment) void {
        const fio = active_fiber_io orelse {
            // No active fiber context — shouldn't happen, but fall back
            @panic("interceptAwait called without active FiberIo");
        };
        // Yield to scheduler: "I'm waiting on IO"
        _ = fio.fiber_handle.yield(.{ .kind = .io_wait });
        // Scheduler resumed us — now collect the IO result
        fio.original_await(userdata, future, result_buf, alignment);
    }

    fn interceptCancel(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result_buf: []u8, alignment: std.mem.Alignment) void {
        const fio = active_fiber_io orelse {
            @panic("interceptCancel called without active FiberIo");
        };
        fio.original_cancel(userdata, future, result_buf, alignment);
    }
};

/// Result of initIoFiber — bundles the fiber with its FiberIo pointer.
pub const IoFiberResult = struct {
    fiber: EffectFiber,
    fio: *FiberIoImpl,
};

/// Create an effect fiber whose EffectContext.io is set to the FiberIo wrapper.
/// The fiber is eagerly started to consume threadlocals, so multiple fibers
/// can be created before running the scheduler.
pub fn initIoFiber(body: EffectBodyFn, inner_io: std.Io, stack_size: usize) !IoFiberResult {
    if (!has_std_io) @compileError("initIoFiber requires std.Io.VTable (Zig 0.16+)");

    const Static = struct {
        threadlocal var current_body: EffectBodyFn = undefined;
        threadlocal var current_io: std.Io = undefined;
    };
    Static.current_body = body;
    Static.current_io = inner_io;

    var fib = try EffectFiber.init(&struct {
        fn wrapper(h: *EffectFiber.Handle) void {
            const b = Static.current_body;
            const inner = Static.current_io;
            var fio = FiberIoImpl.init(inner, h);
            var ctx = EffectContext.initWithIo(h, fio.io());
            active_fiber_io = &fio;
            // Yield immediately to consume threadlocals before caller creates next fiber.
            // The scheduler's start() will resume us past this point.
            _ = h.yield(.{ .kind = .io_wait });
            b(&ctx);
        }
    }.wrapper, stack_size);

    // Eagerly start the fiber so the wrapper runs up to the initial yield,
    // consuming the threadlocals. The fiber is now suspended.
    _ = fib.start();

    // Capture the FiberIo pointer set by the wrapper before it's overwritten
    // by the next initIoFiber call.
    const fio = active_fiber_io orelse @panic("initIoFiber: wrapper did not set active_fiber_io");

    return .{ .fiber = fib, .fio = fio };
}

const IoSchedulerImpl = struct {
    allocator: std.mem.Allocator,
    inner_io: std.Io,
    // Intrusive doubly-linked list of fiber entries
    head: ?*FiberEntry = null,
    tail: ?*FiberEntry = null,
    count: usize = 0,

    const FiberEntry = struct {
        fiber: *EffectFiber,
        handlers: *const HandlerSet,
        fio: ?*FiberIoImpl,
        pending_effect: ?RawEffect,
        next: ?*FiberEntry = null,
        prev: ?*FiberEntry = null,
    };

    pub fn init(allocator: std.mem.Allocator, inner_io: std.Io) IoSchedulerImpl {
        return .{ .allocator = allocator, .inner_io = inner_io };
    }

    pub fn deinit(self: *IoSchedulerImpl) void {
        // Free any remaining entries
        var cur = self.head;
        while (cur) |entry| {
            const next = entry.next;
            self.allocator.destroy(entry);
            cur = next;
        }
        self.head = null;
        self.tail = null;
        self.count = 0;
    }

    pub fn spawn(self: *IoSchedulerImpl, fiber: *EffectFiber, fio: ?*FiberIoImpl, handlers: *const HandlerSet) !void {
        const entry = try self.allocator.create(FiberEntry);
        entry.* = .{
            .fiber = fiber,
            .handlers = handlers,
            .fio = fio,
            .pending_effect = null,
        };
        self.pushBack(entry);
    }

    /// Restore the threadlocal before resuming a fiber.
    fn activateFiber(entry: *FiberEntry) void {
        active_fiber_io = entry.fio;
    }

    /// Capture the threadlocal after a fiber yields/completes.
    fn captureFiber(entry: *FiberEntry) void {
        entry.fio = active_fiber_io;
    }

    pub fn run(self: *IoSchedulerImpl) void {
        // Start all fibers, capture initial effects.
        // IO fibers created with initIoFiber are already suspended (eagerly
        // started); use resumeVoid for those. Regular fibers use start.
        {
            var cur = self.head;
            while (cur) |entry| {
                if (entry.fiber.isSuspended()) {
                    activateFiber(entry);
                    entry.pending_effect = entry.fiber.resumeVoid();
                } else {
                    entry.pending_effect = entry.fiber.start();
                }
                captureFiber(entry);
                cur = entry.next;
            }
        }

        // Round-robin loop
        while (self.head) |entry| {
            self.popFront();

            if (entry.pending_effect) |eff| {
                switch (eff.kind) {
                    .perform => {
                        activateFiber(entry);
                        entry.pending_effect = dispatch_mod.dispatchPerform(&eff, entry.fiber, entry.handlers);
                        captureFiber(entry);
                        self.pushBack(entry);
                    },
                    .emit => {
                        dispatch_mod.dispatchEmit(&eff, entry.fiber, entry.handlers);
                        activateFiber(entry);
                        entry.pending_effect = entry.fiber.resumeVoid();
                        captureFiber(entry);
                        self.pushBack(entry);
                    },
                    .io_wait => {
                        // The fiber yielded because it called await on IO.
                        // Resume it so the interceptor calls original_await.
                        activateFiber(entry);
                        entry.pending_effect = entry.fiber.resumeVoid();
                        captureFiber(entry);
                        self.pushBack(entry);
                    },
                }
            } else {
                // Fiber is dead — remove from queue
                self.allocator.destroy(entry);
            }
        }
    }

    // -- Intrusive linked list operations --

    fn pushBack(self: *IoSchedulerImpl, entry: *FiberEntry) void {
        entry.next = null;
        entry.prev = self.tail;
        if (self.tail) |t| {
            t.next = entry;
        } else {
            self.head = entry;
        }
        self.tail = entry;
        self.count += 1;
    }

    fn popFront(self: *IoSchedulerImpl) void {
        const entry = self.head orelse return;
        self.head = entry.next;
        if (self.head) |h| {
            h.prev = null;
        } else {
            self.tail = null;
        }
        entry.next = null;
        entry.prev = null;
        self.count -= 1;
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "FiberIo: single fiber yields on await then completes" {
    if (!has_std_io) return;

    // Track whether our mock await was called
    const State = struct {
        var await_count: usize = 0;
    };
    State.await_count = 0;

    // Build a minimal mock vtable
    var mock_vt = makeNoopVtable();
    mock_vt.await = &struct {
        fn mock_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {
            State.await_count += 1;
        }
    }.mock_await;

    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    const Result = types.Emit(usize);

    var result_val: usize = 0;

    var res = try initIoFiber(&struct {
        fn body(ctx: *EffectContext) void {
            // Simulate an IO await by calling await through the io interface
            var dummy_future: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy_future), &.{}, .@"1");
            ctx.emit(Result, 42);
        }
    }.body, mock_io, 0);
    defer res.fiber.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onEmit(Result, &struct {
        fn handle(val: *const Result.Value, raw_ctx: ?*anyopaque) void {
            const r: *usize = @ptrCast(@alignCast(raw_ctx.?));
            r.* = val.*;
        }
    }.handle, @ptrCast(&result_val));

    var sched = IoSchedulerImpl.init(testing.allocator, mock_io);
    defer sched.deinit();
    try sched.spawn(&res.fiber, res.fio, &handlers);
    sched.run();

    try testing.expectEqual(@as(usize, 1), State.await_count);
    try testing.expectEqual(@as(usize, 42), result_val);
    try testing.expect(!res.fiber.isAlive());
}

test "IoScheduler: two fibers round-robin" {
    if (!has_std_io) return;

    const State = struct {
        var order: [8]u8 = .{0} ** 8;
        var idx: usize = 0;
    };
    State.order = .{0} ** 8;
    State.idx = 0;

    var mock_vt = makeNoopVtable();
    mock_vt.await = &struct {
        fn mock_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {}
    }.mock_await;

    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    const Trace = types.Emit(u8);

    var res1 = try initIoFiber(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Trace, 'A');
            // Trigger an IO await to yield
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(Trace, 'B');
        }
    }.body, mock_io, 0);
    defer res1.fiber.deinit();

    var res2 = try initIoFiber(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Trace, '1');
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(Trace, '2');
        }
    }.body, mock_io, 0);
    defer res2.fiber.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onEmit(Trace, &struct {
        fn handle(val: *const Trace.Value, _: ?*anyopaque) void {
            State.order[State.idx] = val.*;
            State.idx += 1;
        }
    }.handle, null);

    var sched = IoSchedulerImpl.init(testing.allocator, mock_io);
    defer sched.deinit();
    try sched.spawn(&res1.fiber, res1.fio, &handlers);
    try sched.spawn(&res2.fiber, res2.fio, &handlers);
    sched.run();

    // Both fibers should complete
    try testing.expect(!res1.fiber.isAlive());
    try testing.expect(!res2.fiber.isAlive());

    // Verify interleaving: A, 1, then (after both yield on io_wait) B, 2
    const trace = State.order[0..State.idx];
    try testing.expectEqual(@as(usize, 4), trace.len);
    try testing.expectEqual(@as(u8, 'A'), trace[0]);
    try testing.expectEqual(@as(u8, '1'), trace[1]);
    try testing.expectEqual(@as(u8, 'B'), trace[2]);
    try testing.expectEqual(@as(u8, '2'), trace[3]);
}

test "IoScheduler: IO combined with algebraic effects" {
    if (!has_std_io) return;

    var mock_vt = makeNoopVtable();
    mock_vt.await = &struct {
        fn mock_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {}
    }.mock_await;
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    const GetVal = types.Perform(void, i32);
    const Result = types.Emit(i32);

    const State = struct {
        var result: i32 = 0;
    };
    State.result = 0;

    var res = try initIoFiber(&struct {
        fn body(ctx: *EffectContext) void {
            // Perform an algebraic effect
            const val = ctx.perform(GetVal, {});
            // Do some IO
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            // Emit the result
            ctx.emit(Result, val * 2);
        }
    }.body, mock_io, 0);
    defer res.fiber.deinit();

    const cont_mod = @import("effect/cont.zig");

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onPerform(GetVal, &struct {
        fn handle(_: *GetVal.Value, cont: *cont_mod.Cont(GetVal), _: ?*anyopaque) void {
            cont.@"resume"(21);
        }
    }.handle, null);
    handlers.onEmit(Result, &struct {
        fn handle(val: *const Result.Value, _: ?*anyopaque) void {
            State.result = val.*;
        }
    }.handle, null);

    var sched = IoSchedulerImpl.init(testing.allocator, mock_io);
    defer sched.deinit();
    try sched.spawn(&res.fiber, res.fio, &handlers);
    sched.run();

    try testing.expectEqual(@as(i32, 42), State.result);
    try testing.expect(!res.fiber.isAlive());
}

// ============================================================
// Test helpers
// ============================================================

/// A stand-in for std.Io.AnyFuture (which is opaque).
/// We only need a pointer to pass through the vtable — it's never dereferenced
/// by our mock.
pub const DummyFuture = struct { _padding: u8 = 0 };

/// Build a VTable where every field is a no-op/panic stub.
/// We only need `await` and `cancel` to be real; the rest are never called in tests.
pub fn makeNoopVtable() std.Io.VTable {
    if (!has_std_io) unreachable;

    var vt: std.Io.VTable = undefined;
    // Zero-fill: all function pointers become well-defined (will segfault if
    // called, but our tests only call await/cancel).
    const vt_bytes: *[(@sizeOf(std.Io.VTable))]u8 = @ptrCast(&vt);
    @memset(vt_bytes, 0);

    // Set the two we actually need
    vt.await = &struct {
        fn noop_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {}
    }.noop_await;
    vt.cancel = &struct {
        fn noop_cancel(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {}
    }.noop_cancel;

    return vt;
}
