// Cooperative IO scheduling for effect fibers.
//
// The Scheduler owns a single IO vtable copy (IoState). When a fiber
// awaits IO, the interceptor yields control to the Scheduler, which
// can run other fibers while the IO completes.

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

const AwaitFn = fn (?*anyopaque, *std.Io.AnyFuture, []u8, std.mem.Alignment) void;
const CancelFn = fn (?*anyopaque, *std.Io.AnyFuture, []u8, std.mem.Alignment) void;

/// Threadlocal holding the active fiber handle for the currently-executing fiber.
/// Set by Scheduler before each resume, read by interceptAwait.
pub threadlocal var active_fiber_handle: ?*EffectFiber.Handle = null;

/// Original await/cancel function pointers, set once per run() call from IoState.
threadlocal var original_await_fn: *const AwaitFn = undefined;
threadlocal var original_cancel_fn: *const CancelFn = undefined;

fn interceptAwait(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result_buf: []u8, alignment: std.mem.Alignment) void {
    const handle = active_fiber_handle orelse @panic("interceptAwait called without active fiber handle");
    _ = handle.yield(.{ .kind = .io_wait });
    original_await_fn(userdata, future, result_buf, alignment);
}

fn interceptCancel(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result_buf: []u8, alignment: std.mem.Alignment) void {
    _ = active_fiber_handle orelse @panic("interceptCancel called without active fiber handle");
    original_cancel_fn(userdata, future, result_buf, alignment);
}

/// Result of createFiber — bundles the fiber with its handle pointer.
pub const FiberResult = struct {
    fiber: EffectFiber,
    handle: *EffectFiber.Handle,
};

const HandlerFiberCtx = handler_mod.HandlerFiberCtx;
const initFiberPooled = types.initFiberPooled;

pub const Scheduler = struct {
    allocator: std.mem.Allocator,
    // Intrusive doubly-linked list of fiber entries
    head: ?*FiberEntry = null,
    tail: ?*FiberEntry = null,
    count: usize = 0,
    io_state: ?IoState = null,
    pool: @import("pool.zig").StackPool = .{},

    const IoState = struct {
        vtable: std.Io.VTable,
        userdata: ?*anyopaque,
        original_await: *const AwaitFn,
        original_cancel: *const CancelFn,

        fn wrappedIo(self: *IoState) std.Io {
            return .{ .userdata = self.userdata, .vtable = &self.vtable };
        }
    };

    const FiberEntry = struct {
        fiber: *EffectFiber,
        handlers: *const HandlerSet,
        handle: ?*EffectFiber.Handle,
        pending_effect: ?RawEffect,
        next: ?*FiberEntry = null,
        prev: ?*FiberEntry = null,
    };

    pub fn init(allocator: std.mem.Allocator) Scheduler {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Scheduler) void {
        self.pool.deinit();
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

    pub fn spawn(self: *Scheduler, fiber: *EffectFiber, handle: ?*EffectFiber.Handle, handlers: *const HandlerSet) !void {
        const entry = try self.allocator.create(FiberEntry);
        entry.* = .{
            .fiber = fiber,
            .handlers = handlers,
            .handle = handle,
            .pending_effect = null,
        };
        self.pushBack(entry);
    }

    fn setupIo(self: *Scheduler, inner: std.Io) void {
        var vt = inner.vtable.*;
        const orig_await = vt.await;
        const orig_cancel = vt.cancel;
        vt.await = &interceptAwait;
        vt.cancel = &interceptCancel;
        self.io_state = .{
            .vtable = vt,
            .userdata = inner.userdata,
            .original_await = orig_await,
            .original_cancel = orig_cancel,
        };
    }

    pub fn createFiber(self: *Scheduler, body: EffectBodyFn, inner_io: std.Io, stack_size: usize) !FiberResult {
        if (self.io_state == null) {
            self.setupIo(inner_io);
        }

        const Static = struct {
            threadlocal var current_body: EffectBodyFn = undefined;
            threadlocal var current_io: std.Io = undefined;
        };
        Static.current_body = body;
        Static.current_io = self.io_state.?.wrappedIo();

        var fib = try EffectFiber.initPooled(&struct {
            fn wrapper(h: *EffectFiber.Handle) void {
                const b = Static.current_body;
                const io = Static.current_io;
                active_fiber_handle = h;
                var ctx = EffectContext.initWithIo(h, io);
                // Yield immediately to consume threadlocals before caller creates next fiber.
                // The scheduler's run() will resume us past this point.
                _ = h.yield(.{ .kind = .io_wait });
                b(&ctx);
            }
        }.wrapper, stack_size, &self.pool);

        // Eagerly start the fiber so the wrapper runs up to the initial yield,
        // consuming the threadlocals. The fiber is now suspended.
        _ = fib.start();

        // Capture the handle set by the wrapper before it's overwritten
        // by the next createFiber call.
        const handle = active_fiber_handle orelse @panic("createFiber: wrapper did not set active_fiber_handle");

        return .{ .fiber = fib, .handle = handle };
    }

    /// Restore the threadlocal before resuming a fiber.
    fn activateFiber(entry: *FiberEntry) void {
        active_fiber_handle = entry.handle;
    }

    /// Capture the threadlocal after a fiber yields/completes.
    fn captureFiber(entry: *FiberEntry) void {
        entry.handle = active_fiber_handle;
    }

    pub fn run(self: *Scheduler) void {
        // Set original function pointers from io_state (once, before the loop).
        if (self.io_state) |*ios| {
            original_await_fn = ios.original_await;
            original_cancel_fn = ios.original_cancel;
        }

        // Start all fibers, capture initial effects.
        // IO fibers created with createFiber are already suspended (eagerly
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
                        entry.pending_effect = self.dispatchPerformScheduled(&eff, entry.fiber, entry.handlers);
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

    /// Walk the handler chain (child -> parent). At each level, try simple
    /// bindings first, then effectful bindings. Effectful handlers run in
    /// their own fibers; their effects are dispatched to the parent scope.
    fn dispatchPerformScheduled(
        self: *Scheduler,
        eff: *const RawEffect,
        origin_fiber: *EffectFiber,
        handlers: ?*const HandlerSet,
    ) ?RawEffect {
        var level: ?*const HandlerSet = handlers;
        while (level) |hs| : (level = hs.parent) {
            // Simple bindings (same logic as dispatchPerform)
            for (hs.perform_bindings.items) |binding| {
                if (binding.id == eff.id) {
                    switch (binding.handler(eff, origin_fiber, binding.ctx)) {
                        .handled => |next| return next,
                        .skipped => {},
                    }
                }
            }

            // Effectful bindings
            for (hs.effectful_bindings.items) |binding| {
                if (binding.id == eff.id) {
                    var hctx = HandlerFiberCtx{
                        .raw = eff,
                        .user_ctx = binding.ctx,
                    };
                    handler_mod.handler_fiber_ctx_tls = &hctx;

                    var hfib = initFiberPooled(&self.pool, binding.fiber_body) catch @panic("OOM");
                    defer hfib.deinit();

                    // Run handler fiber — dispatch ITS effects to parent scope
                    var heff = hfib.start();
                    while (heff) |h| {
                        switch (h.kind) {
                            .emit => {
                                if (hs.parent) |p| {
                                    dispatch_mod.dispatchEmit(&h, &hfib, p);
                                }
                                heff = hfib.resumeVoid();
                            },
                            .perform => {
                                heff = self.dispatchPerformScheduled(&h, &hfib, hs.parent);
                            },
                            .io_wait => unreachable,
                        }
                    }

                    // Handler fiber completed — check outcome
                    if (hctx.delegated) continue;
                    if (hctx.resumed) return origin_fiber.resumeVoid();
                    if (hctx.dropped) {
                        origin_fiber.deinit();
                        return null;
                    }
                    // auto-drop (shouldn't happen — wrapper sets dropped)
                    origin_fiber.deinit();
                    return null;
                }
            }
        }

        // Chain exhausted — no handler accepted. Resume with zeroed value.
        return origin_fiber.resumeVoid();
    }

    // -- Intrusive linked list operations --

    fn pushBack(self: *Scheduler, entry: *FiberEntry) void {
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

    fn popFront(self: *Scheduler) void {
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

test "Scheduler: single fiber yields on await then completes" {

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

    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();

    var res = try sched.createFiber(&struct {
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

    try sched.spawn(&res.fiber, res.handle, &handlers);
    sched.run();

    try testing.expectEqual(@as(usize, 1), State.await_count);
    try testing.expectEqual(@as(usize, 42), result_val);
    try testing.expect(!res.fiber.isAlive());
}

test "Scheduler: two fibers round-robin" {

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

    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();

    var res1 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Trace, 'A');
            // Trigger an IO await to yield
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(Trace, 'B');
        }
    }.body, mock_io, 0);
    defer res1.fiber.deinit();

    var res2 = try sched.createFiber(&struct {
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

    try sched.spawn(&res1.fiber, res1.handle, &handlers);
    try sched.spawn(&res2.fiber, res2.handle, &handlers);
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

test "Scheduler: IO combined with algebraic effects" {

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

    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();

    var res = try sched.createFiber(&struct {
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

    try sched.spawn(&res.fiber, res.handle, &handlers);
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
