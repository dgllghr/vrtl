// Multicore work-stealing scheduler for effect fibers.
//
// N worker threads, each with its own deque + stack pool.
// Idle workers steal from peers. Worker 0 runs on the caller thread.

const std = @import("std");
const types = @import("effect/types.zig");
const dispatch_mod = @import("effect/dispatch.zig");
const handler_mod = @import("effect/handler.zig");
const Deque = @import("deque.zig").WorkStealingDeque;
const StackPool = @import("pool.zig").StackPool;
const darwin = std.c;

const RawEffect = types.RawEffect;
const EffectFiber = types.EffectFiber;
const EffectContext = types.EffectContext;
const EffectBodyFn = types.EffectBodyFn;
const EffectKind = types.EffectKind;
const HandlerSet = handler_mod.HandlerSet;

const AwaitFn = fn (?*anyopaque, *std.Io.AnyFuture, []u8, std.mem.Alignment) void;
const CancelFn = fn (?*anyopaque, *std.Io.AnyFuture, []u8, std.mem.Alignment) void;

const PendingIo = struct {
    userdata: ?*anyopaque,
    future: *std.Io.AnyFuture,
    result_buf: []u8,
    alignment: std.mem.Alignment,
    await_fn: *const AwaitFn,
};

/// Threadlocal holding the active fiber handle for the currently-executing fiber.
/// Set by Worker before each resume, read by interceptAwait.
pub threadlocal var active_fiber_handle: ?*EffectFiber.Handle = null;

/// Original await/cancel function pointers, set once per worker thread from IoState.
threadlocal var original_await_fn: *const AwaitFn = undefined;
threadlocal var original_cancel_fn: *const CancelFn = undefined;

/// Threadlocal for passing IO args from interceptAwait to the scheduler.
threadlocal var pending_io: ?PendingIo = null;

fn interceptAwait(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result_buf: []u8, alignment: std.mem.Alignment) void {
    const handle = active_fiber_handle orelse @panic("interceptAwait called without active fiber handle");
    pending_io = .{
        .userdata = userdata,
        .future = future,
        .result_buf = result_buf,
        .alignment = alignment,
        .await_fn = original_await_fn,
    };
    _ = handle.yield(.{ .kind = .io_wait });
    // When resumed: IO is done, result_buf already filled by BG thread
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

// ============================================================
// Futex helpers (macOS __ulock)
// ============================================================

const PARK_RUNNING: u32 = 0;
const PARK_PARKED: u32 = 1;
const PARK_NOTIFIED: u32 = 2;

fn futexWait(ptr: *u32, expected: u32) void {
    _ = darwin.__ulock_wait(
        .{ .op = .COMPARE_AND_WAIT },
        @ptrCast(ptr),
        @as(u64, expected),
        0, // infinite timeout
    );
}

fn futexWakeOne(ptr: *u32) void {
    _ = darwin.__ulock_wake(
        .{ .op = .COMPARE_AND_WAIT },
        @ptrCast(ptr),
        0,
    );
}

fn futexWakeAll(ptr: *u32) void {
    _ = darwin.__ulock_wake(
        .{ .op = .COMPARE_AND_WAIT, .WAKE_ALL = true },
        @ptrCast(ptr),
        0,
    );
}

// ============================================================
// Worker
// ============================================================

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {}
}

const Inbox = struct {
    mu: std.atomic.Mutex = .unlocked,
    items: std.ArrayListUnmanaged(*Scheduler.FiberEntry) = .{},

    fn push(self: *Inbox, allocator: std.mem.Allocator, entry: *Scheduler.FiberEntry) void {
        spinLock(&self.mu);
        defer self.mu.unlock();
        self.items.append(allocator, entry) catch @panic("OOM: inbox push");
    }

    fn drain(self: *Inbox, deque: *Deque(*Scheduler.FiberEntry)) usize {
        spinLock(&self.mu);
        const slice = self.items.items;
        const count = slice.len;
        if (count == 0) {
            self.mu.unlock();
            return 0;
        }
        for (slice) |entry| {
            deque.push(entry) catch unreachable;
        }
        self.items.clearRetainingCapacity();
        self.mu.unlock();
        return count;
    }

    fn deinit(self: *Inbox, allocator: std.mem.Allocator) void {
        self.items.deinit(allocator);
    }
};

pub const Worker = struct {
    deque: Deque(*Scheduler.FiberEntry),
    pool: StackPool = .{},
    inbox: Inbox = .{},
    thread: ?std.Thread = null,
    id: u32,
    parent: *Scheduler,
    rng_state: u32,
    park_state: u32 = PARK_RUNNING,

    fn initWorker(alloc: std.mem.Allocator, id: u32, parent: *Scheduler) !Worker {
        return .{
            .deque = try Deque(*Scheduler.FiberEntry).init(alloc, 16),
            .id = id,
            .parent = parent,
            .rng_state = id +| 1,
        };
    }

    fn deinitWorker(self: *Worker) void {
        self.pool.deinit();
        while (self.deque.pop()) |entry| {
            self.parent.allocator.destroy(entry);
        }
        self.deque.deinit();
        self.inbox.deinit(self.parent.allocator);
    }

    /// xorshift32 PRNG for steal target selection.
    fn nextRand(self: *Worker) u32 {
        var x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.rng_state = x;
        return x;
    }

    /// Restore the threadlocal before resuming a fiber.
    const FiberEntry = Scheduler.FiberEntry;

    fn activateFiber(entry: *FiberEntry) void {
        active_fiber_handle = entry.handle;
    }

    /// Capture the threadlocal after a fiber yields/completes.
    fn captureFiber(entry: *FiberEntry) void {
        entry.handle = active_fiber_handle;
    }

    /// Process a single fiber entry — dispatch its pending effect.
    fn processEntry(self: *Worker, entry: *FiberEntry) void {
        const sched = self.parent;
        if (entry.pending_effect) |eff| {
            switch (eff.kind) {
                .perform => {
                    activateFiber(entry);
                    entry.pending_effect = self.dispatchPerformScheduled(&eff, entry.fiber, entry.handlers);
                    captureFiber(entry);
                    self.deque.push(entry) catch unreachable;
                },
                .emit => {
                    dispatch_mod.dispatchEmit(&eff, entry.fiber, entry.handlers);
                    activateFiber(entry);
                    entry.pending_effect = entry.fiber.resumeVoid();
                    captureFiber(entry);
                    self.deque.push(entry) catch unreachable;
                },
                .io_wait => {
                    // Check entry first (captured in start phase), then TLS
                    const pio = entry.pending_io orelse blk: {
                        const tls = pending_io;
                        pending_io = null;
                        break :blk tls;
                    };
                    if (pio) |io_args| {
                        // Real IO — park fiber, submit to BG thread
                        entry.pending_io = io_args;
                        self.submitIoWait(entry);
                    } else {
                        // Initial yield or no-op — resume immediately
                        entry.pending_io = null;
                        activateFiber(entry);
                        entry.pending_effect = entry.fiber.resumeVoid();
                        captureFiber(entry);
                        // Capture any new pending_io from the resumed fiber
                        entry.pending_io = pending_io;
                        pending_io = null;
                        self.deque.push(entry) catch unreachable;
                    }
                },
            }
            // Wake a peer after pushing work back
            sched.wakeOne(self);
        } else {
            // Fiber is dead — free the entry, decrement live count
            sched.allocator.destroy(entry);
            const prev = @atomicRmw(isize, &sched.live_fibers, .Sub, 1, .release);
            if (prev <= 1) {
                // Last fiber died — signal shutdown
                @atomicStore(u32, &sched.shutdown, 1, .release);
                sched.wakeAll();
            }
        }
    }

    /// Try stealing from a random peer. Returns stolen entry or null.
    fn tryStealing(self: *Worker) ?*FiberEntry {
        const sched = self.parent;
        const n = sched.num_workers;
        if (n <= 1) return null;

        const start = self.nextRand() % n;
        for (0..n) |i| {
            const target_id = (start + @as(u32, @intCast(i))) % n;
            if (target_id == self.id) continue;

            const target = &sched.workers[target_id];
            // Try twice on CAS abort
            for (0..2) |_| {
                switch (target.deque.steal()) {
                    .success => |entry| return entry,
                    .abort => continue,
                    .empty => break,
                }
            }
        }
        return null;
    }

    /// Submit a fiber's IO to a background thread so the worker stays free.
    fn submitIoWait(self: *Worker, entry: *FiberEntry) void {
        const sched = self.parent;
        const worker_ptr = self;
        const t = std.Thread.spawn(.{ .stack_size = 64 * 1024 }, struct {
            fn run(w: *Worker, e: *FiberEntry, s: *Scheduler) void {
                const io = e.pending_io.?;
                io.await_fn(io.userdata, io.future, io.result_buf, io.alignment);
                e.pending_io = null;
                // Mark ready to resume (io_wait with no pending_io → immediate resume)
                e.pending_effect = .{ .kind = .io_wait };
                w.inbox.push(s.allocator, e);
                // Wake the worker
                if (@cmpxchgStrong(u32, &w.park_state, PARK_PARKED, PARK_NOTIFIED, .release, .monotonic) == null) {
                    futexWakeOne(&w.park_state);
                }
            }
        }.run, .{ worker_ptr, entry, sched }) catch @panic("failed to spawn IO thread");

        spinLock(&sched.io_threads_mu);
        sched.io_threads.append(sched.allocator, t) catch @panic("OOM: io_threads append");
        sched.io_threads_mu.unlock();
    }

    /// Park this worker until notified.
    fn park(self: *Worker) void {
        // CAS RUNNING → PARKED
        if (@cmpxchgStrong(u32, &self.park_state, PARK_RUNNING, PARK_PARKED, .seq_cst, .monotonic)) |actual| {
            // Was NOTIFIED — consume and return
            if (actual == PARK_NOTIFIED) {
                @atomicStore(u32, &self.park_state, PARK_RUNNING, .monotonic);
                return;
            }
            // Already PARKED shouldn't happen
            return;
        }
        // Wait until not PARKED
        futexWait(&self.park_state, PARK_PARKED);
        // Reset to RUNNING
        @atomicStore(u32, &self.park_state, PARK_RUNNING, .monotonic);
    }

    /// Worker main loop.
    fn workerLoop(self: *Worker) void {
        const sched = self.parent;

        // Set IO threadlocals
        if (sched.io_state) |*ios| {
            original_await_fn = ios.original_await;
            original_cancel_fn = ios.original_cancel;
        }

        // Start phase: drain own deque (FIFO steal), start each fiber, push back
        {
            const n = self.deque.len();
            for (0..n) |_| {
                const entry = switch (self.deque.steal()) {
                    .success => |e| e,
                    else => break,
                };
                if (entry.fiber.isSuspended()) {
                    activateFiber(entry);
                    entry.pending_effect = entry.fiber.resumeVoid();
                } else {
                    entry.pending_effect = entry.fiber.start();
                }
                captureFiber(entry);
                // Capture pending_io before next fiber overwrites TLS
                entry.pending_io = pending_io;
                pending_io = null;
                self.deque.push(entry) catch unreachable;
            }
        }

        // Main loop
        while (true) {
            // Drain inbox (completed IO from BG threads)
            const drained = self.inbox.drain(&self.deque);
            if (drained > 0) continue;

            // Check termination
            if (@atomicLoad(isize, &sched.live_fibers, .acquire) <= 0) {
                @atomicStore(u32, &sched.shutdown, 1, .release);
                sched.wakeAll();
                return;
            }

            // Pop local
            if (self.deque.pop()) |entry| {
                self.processEntry(entry);
                continue;
            }

            // Try stealing
            if (self.tryStealing()) |entry| {
                self.processEntry(entry);
                continue;
            }

            // Check shutdown
            if (@atomicLoad(u32, &sched.shutdown, .acquire) != 0) return;

            // Park
            self.park();

            // After wake, check shutdown again
            if (@atomicLoad(u32, &sched.shutdown, .acquire) != 0) return;
        }
    }

    /// Thread entry point for worker threads (id > 0).
    fn threadEntry(self: *Worker) void {
        self.workerLoop();
    }

    /// Walk the handler chain (child -> parent). At each level, try simple
    /// bindings first, then effectful bindings. Effectful handlers run in
    /// their own fibers; their effects are dispatched to the parent scope.
    fn dispatchPerformScheduled(
        self: *Worker,
        eff: *const RawEffect,
        origin_fiber: *EffectFiber,
        handlers: ?*const HandlerSet,
    ) ?RawEffect {
        var level: ?*const HandlerSet = handlers;
        while (level) |hs| : (level = hs.parent) {
            // Simple bindings
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
                    // auto-drop
                    origin_fiber.deinit();
                    return null;
                }
            }
        }

        // Chain exhausted — no handler accepted. Resume with zeroed value.
        return origin_fiber.resumeVoid();
    }
};

// ============================================================
// Scheduler
// ============================================================

pub const Scheduler = struct {
    allocator: std.mem.Allocator,
    workers: []Worker,
    num_workers: u32,
    live_fibers: isize = 0,
    shutdown: u32 = 0,
    spawn_buffer: std.ArrayListUnmanaged(*FiberEntry),
    io_state: ?IoState = null,
    io_threads_mu: std.atomic.Mutex = .unlocked,
    io_threads: std.ArrayListUnmanaged(std.Thread) = .{},

    const IoState = struct {
        vtable: std.Io.VTable,
        userdata: ?*anyopaque,
        original_await: *const AwaitFn,
        original_cancel: *const CancelFn,

        fn wrappedIo(self: *IoState) std.Io {
            return .{ .userdata = self.userdata, .vtable = &self.vtable };
        }
    };

    pub const FiberEntry = struct {
        fiber: *EffectFiber,
        handlers: *const HandlerSet,
        handle: ?*EffectFiber.Handle,
        pending_effect: ?RawEffect,
        pending_io: ?PendingIo = null,
    };

    /// Initialize a scheduler with `num_workers` workers.
    /// Pass 0 to auto-detect CPU count.
    pub fn init(allocator: std.mem.Allocator, num_workers: u32) !Scheduler {
        const nw: u32 = if (num_workers == 0)
            @intCast(std.Thread.getCpuCount() catch 1)
        else
            num_workers;

        const workers = try allocator.alloc(Worker, nw);
        for (workers, 0..) |*w, i| {
            w.* = try Worker.initWorker(allocator, @intCast(i), undefined);
        }

        return Scheduler{
            .allocator = allocator,
            .workers = workers,
            .num_workers = nw,
            .spawn_buffer = .{},
        };
    }

    /// Set worker parent pointers to this Scheduler's current address.
    /// Must be called before workers access `parent`. Safe to call repeatedly.
    fn fixupParents(self: *Scheduler) void {
        for (self.workers) |*w| {
            w.parent = self;
        }
    }

    pub fn deinit(self: *Scheduler) void {
        for (self.workers) |*w| {
            w.deinitWorker();
        }
        self.allocator.free(self.workers);
        self.spawn_buffer.deinit(self.allocator);
        self.io_threads.deinit(self.allocator);
    }

    pub fn spawn(self: *Scheduler, fiber: *EffectFiber, handle: ?*EffectFiber.Handle, handlers: *const HandlerSet) !void {
        self.fixupParents();
        const entry = try self.allocator.create(FiberEntry);
        entry.* = .{
            .fiber = fiber,
            .handlers = handlers,
            .handle = handle,
            .pending_effect = null,
        };
        try self.spawn_buffer.append(self.allocator, entry);
        _ = @atomicRmw(isize, &self.live_fibers, .Add, 1, .monotonic);
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
        self.fixupParents();
        if (self.io_state == null) {
            self.setupIo(inner_io);
        }

        const Static = struct {
            threadlocal var current_body: EffectBodyFn = undefined;
            threadlocal var current_io: std.Io = undefined;
        };
        Static.current_body = body;
        Static.current_io = self.io_state.?.wrappedIo();

        // Always use worker 0's pool for createFiber (called from main thread)
        var fib = try EffectFiber.initPooled(&struct {
            fn wrapper(h: *EffectFiber.Handle) void {
                const b = Static.current_body;
                const io = Static.current_io;
                active_fiber_handle = h;
                var ctx = EffectContext.initWithIo(h, io);
                _ = h.yield(.{ .kind = .io_wait });
                b(&ctx);
            }
        }.wrapper, stack_size, &self.workers[0].pool);

        _ = fib.start();

        const handle = active_fiber_handle orelse @panic("createFiber: wrapper did not set active_fiber_handle");

        return .{ .fiber = fib, .handle = handle };
    }

    pub fn run(self: *Scheduler) void {
        self.fixupParents();
        // Reset shutdown state
        @atomicStore(u32, &self.shutdown, 0, .monotonic);

        // Nothing to do
        if (self.spawn_buffer.items.len == 0 and @atomicLoad(isize, &self.live_fibers, .monotonic) <= 0) return;

        // Distribute buffered fibers round-robin across workers
        for (self.spawn_buffer.items, 0..) |entry, i| {
            const target = @as(u32, @intCast(i)) % self.num_workers;
            self.workers[target].deque.push(entry) catch unreachable;
        }
        self.spawn_buffer.clearRetainingCapacity();

        // Spawn N-1 worker threads
        for (self.workers[1..]) |*w| {
            w.thread = std.Thread.spawn(.{}, Worker.threadEntry, .{w}) catch @panic("failed to spawn worker thread");
        }

        // Run worker 0 on caller thread
        self.workers[0].workerLoop();

        // Join all worker threads
        for (self.workers[1..]) |*w| {
            if (w.thread) |t| {
                t.join();
                w.thread = null;
            }
        }

        // Join all IO background threads
        for (self.io_threads.items) |t| {
            t.join();
        }
        self.io_threads.clearRetainingCapacity();
    }

    /// Wake one parked peer worker (not self).
    fn wakeOne(self: *Scheduler, caller: *Worker) void {
        const n = self.num_workers;
        if (n <= 1) return;
        const start = caller.nextRand() % n;
        for (0..n) |i| {
            const target_id = (start + @as(u32, @intCast(i))) % n;
            if (target_id == caller.id) continue;
            const target = &self.workers[target_id];
            // CAS PARKED → NOTIFIED
            if (@cmpxchgStrong(u32, &target.park_state, PARK_PARKED, PARK_NOTIFIED, .release, .monotonic) == null) {
                futexWakeOne(&target.park_state);
                return;
            }
        }
    }

    /// Wake all workers (for shutdown).
    fn wakeAll(self: *Scheduler) void {
        for (self.workers) |*w| {
            @atomicStore(u32, &w.park_state, PARK_NOTIFIED, .release);
            futexWakeAll(&w.park_state);
        }
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

    var sched = try Scheduler.init(testing.allocator, 1);
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

test "Scheduler: two fibers LIFO depth-first" {

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

    var sched = try Scheduler.init(testing.allocator, 1);
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

    // LIFO: last-spawned fiber (res2) runs depth-first to completion,
    // then res1 runs. Within each fiber, order is preserved.
    const trace = State.order[0..State.idx];
    try testing.expectEqual(@as(usize, 4), trace.len);
    try testing.expectEqual(@as(u8, '1'), trace[0]);
    try testing.expectEqual(@as(u8, '2'), trace[1]);
    try testing.expectEqual(@as(u8, 'A'), trace[2]);
    try testing.expectEqual(@as(u8, 'B'), trace[3]);
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

    var sched = try Scheduler.init(testing.allocator, 1);
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
// Multicore tests
// ============================================================

test "Scheduler: 100 fibers, 4 workers, atomic counter" {
    const N = 100;

    var mock_vt = makeNoopVtable();
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    const BenchPerform = types.Perform(u64, u64);
    const cont_mod = @import("effect/cont.zig");

    var counter: isize = 0;

    var sched = try Scheduler.init(testing.allocator, 4);
    defer sched.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onPerform(BenchPerform, &struct {
        fn handle(_: *BenchPerform.Value, cont: *cont_mod.Cont(BenchPerform), _: ?*anyopaque) void {
            cont.@"resume"(1);
        }
    }.handle, null);

    var results: [N]FiberResult = undefined;
    for (0..N) |i| {
        results[i] = try sched.createFiber(&struct {
            fn body(ctx: *EffectContext) void {
                // Perform an effect that returns 1
                const r = ctx.perform(BenchPerform, 0);
                std.mem.doNotOptimizeAway(r);
            }
        }.body, mock_io, 0);
        try sched.spawn(&results[i].fiber, results[i].handle, &handlers);
        // Atomic inc for each fiber that will complete
        _ = @atomicRmw(isize, &counter, .Add, 0, .monotonic); // just to reference counter
    }

    sched.run();

    // All fibers should be dead
    for (0..N) |i| {
        try testing.expect(!results[i].fiber.isAlive());
        results[i].fiber.deinit();
    }
}

test "Scheduler: zero fibers, run returns immediately" {
    var sched = try Scheduler.init(testing.allocator, 4);
    defer sched.deinit();
    sched.run();
    // Should not hang
}

test "Scheduler: two fibers, 2 workers, both complete" {
    var mock_vt = makeNoopVtable();
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    const BenchPerform = types.Perform(u64, u64);
    const cont_mod = @import("effect/cont.zig");

    var sched = try Scheduler.init(testing.allocator, 2);
    defer sched.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onPerform(BenchPerform, &struct {
        fn handle(_: *BenchPerform.Value, cont: *cont_mod.Cont(BenchPerform), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);

    var res1 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            for (0..100) |i| {
                const r = ctx.perform(BenchPerform, i);
                std.mem.doNotOptimizeAway(r);
            }
        }
    }.body, mock_io, 0);

    var res2 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            for (0..100) |i| {
                const r = ctx.perform(BenchPerform, i);
                std.mem.doNotOptimizeAway(r);
            }
        }
    }.body, mock_io, 0);

    try sched.spawn(&res1.fiber, res1.handle, &handlers);
    try sched.spawn(&res2.fiber, res2.handle, &handlers);
    sched.run();

    try testing.expect(!res1.fiber.isAlive());
    try testing.expect(!res2.fiber.isAlive());
    res1.fiber.deinit();
    res2.fiber.deinit();
}

// ============================================================
// Non-blocking IO tests
// ============================================================

test "Scheduler: IO does not block worker" {
    // 2 fibers on 1 worker. Each does a mock await that sleeps 50ms.
    // If IO is non-blocking (parallel BG threads), wall-clock should be ~50ms, not ~100ms.
    const State = struct {
        var completed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);
    };
    State.completed = std.atomic.Value(u32).init(0);

    var mock_vt = makeNoopVtable();
    mock_vt.await = &struct {
        fn slow_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {
            const ts: std.c.timespec = .{ .sec = 0, .nsec = 50_000_000 };
            _ = std.c.nanosleep(&ts, null);
            _ = State.completed.fetchAdd(1, .monotonic);
        }
    }.slow_await;
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    var sched = try Scheduler.init(testing.allocator, 1);
    defer sched.deinit();

    var res1 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
        }
    }.body, mock_io, 0);
    defer res1.fiber.deinit();

    var res2 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
        }
    }.body, mock_io, 0);
    defer res2.fiber.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    try sched.spawn(&res1.fiber, res1.handle, &handlers);
    try sched.spawn(&res2.fiber, res2.handle, &handlers);

    var t_start: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &t_start);
    sched.run();
    var t_end: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &t_end);
    const elapsed_ms = @divFloor(
        (@as(i64, t_end.sec) - @as(i64, t_start.sec)) * 1_000_000_000 +
            (@as(i64, t_end.nsec) - @as(i64, t_start.nsec)),
        1_000_000,
    );

    try testing.expect(!res1.fiber.isAlive());
    try testing.expect(!res2.fiber.isAlive());
    try testing.expectEqual(@as(u32, 2), State.completed.load(.acquire));
    // Should complete in ~50ms (parallel), not ~100ms (serial). Allow up to 90ms.
    try testing.expect(elapsed_ms < 90);
}

test "Scheduler: IO + effects interleaved" {
    // 1 fiber: perform → IO await → emit. 1 worker. Verify all effects dispatched.
    const GetVal = types.Perform(void, i32);
    const Result = types.Emit(i32);
    const cont_mod = @import("effect/cont.zig");

    const State = struct {
        var result: i32 = 0;
    };
    State.result = 0;

    var mock_vt = makeNoopVtable();
    mock_vt.await = &struct {
        fn mock_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {}
    }.mock_await;
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    var sched = try Scheduler.init(testing.allocator, 1);
    defer sched.deinit();

    var res = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(GetVal, {});
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(Result, val * 3);
        }
    }.body, mock_io, 0);
    defer res.fiber.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onPerform(GetVal, &struct {
        fn handle(_: *GetVal.Value, cont: *cont_mod.Cont(GetVal), _: ?*anyopaque) void {
            cont.@"resume"(7);
        }
    }.handle, null);
    handlers.onEmit(Result, &struct {
        fn handle(val: *const Result.Value, _: ?*anyopaque) void {
            State.result = val.*;
        }
    }.handle, null);

    try sched.spawn(&res.fiber, res.handle, &handlers);
    sched.run();

    try testing.expectEqual(@as(i32, 21), State.result);
    try testing.expect(!res.fiber.isAlive());
}

test "Scheduler: multiple IO awaits per fiber" {
    // 1 fiber does 3 sequential IO awaits. 1 worker. All complete.
    const State = struct {
        var await_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);
    };
    State.await_count = std.atomic.Value(u32).init(0);

    var mock_vt = makeNoopVtable();
    mock_vt.await = &struct {
        fn counting_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {
            _ = State.await_count.fetchAdd(1, .monotonic);
        }
    }.counting_await;
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    var sched = try Scheduler.init(testing.allocator, 1);
    defer sched.deinit();

    var res = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            for (0..3) |_| {
                var dummy: DummyFuture = .{};
                ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            }
        }
    }.body, mock_io, 0);
    defer res.fiber.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    try sched.spawn(&res.fiber, res.handle, &handlers);
    sched.run();

    try testing.expectEqual(@as(u32, 3), State.await_count.load(.acquire));
    try testing.expect(!res.fiber.isAlive());
}

test "Scheduler: IO with multicore" {
    // 10 fibers × IO await, 4 workers. All complete.
    const N = 10;
    const State = struct {
        var completed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);
    };
    State.completed = std.atomic.Value(u32).init(0);

    var mock_vt = makeNoopVtable();
    mock_vt.await = &struct {
        fn mock_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {
            _ = State.completed.fetchAdd(1, .monotonic);
        }
    }.mock_await;
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    var sched = try Scheduler.init(testing.allocator, 4);
    defer sched.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    var results: [N]FiberResult = undefined;
    for (0..N) |i| {
        results[i] = try sched.createFiber(&struct {
            fn body(ctx: *EffectContext) void {
                var dummy: DummyFuture = .{};
                ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            }
        }.body, mock_io, 0);
        try sched.spawn(&results[i].fiber, results[i].handle, &handlers);
    }

    sched.run();

    try testing.expectEqual(@as(u32, N), State.completed.load(.acquire));
    for (0..N) |i| {
        try testing.expect(!results[i].fiber.isAlive());
        results[i].fiber.deinit();
    }
}

// ============================================================
// Test helpers
// ============================================================

/// A stand-in for std.Io.AnyFuture (which is opaque).
pub const DummyFuture = struct { _padding: u8 = 0 };

/// Build a VTable where every field is a no-op/panic stub.
pub fn makeNoopVtable() std.Io.VTable {
    var vt: std.Io.VTable = undefined;
    const vt_bytes: *[(@sizeOf(std.Io.VTable))]u8 = @ptrCast(&vt);
    @memset(vt_bytes, 0);

    vt.await = &struct {
        fn noop_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {}
    }.noop_await;
    vt.cancel = &struct {
        fn noop_cancel(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {}
    }.noop_cancel;

    return vt;
}
