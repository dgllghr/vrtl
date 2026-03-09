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
const WakeHandle = types.WakeHandle;
const HandlerSet = handler_mod.HandlerSet;

const net = std.Io.net;

const AwaitFn = fn (?*anyopaque, *std.Io.AnyFuture, []u8, std.mem.Alignment) void;
const CancelFn = fn (?*anyopaque, *std.Io.AnyFuture, []u8, std.mem.Alignment) void;
const NetAcceptFn = fn (?*anyopaque, net.Socket.Handle) net.Server.AcceptError!net.Stream;
const NetReadFn = fn (?*anyopaque, net.Socket.Handle, [][]u8) net.Stream.Reader.Error!usize;
const NetWriteFn = fn (?*anyopaque, net.Socket.Handle, []const u8, []const []const u8, usize) net.Stream.Writer.Error!usize;

/// Type-erased pending IO operation. Each interceptor creates a stack-local
/// Ctx that captures the original fn + args + a result slot. The BG thread
/// calls run_fn(ctx), which writes the result into Ctx. The fiber reads
/// the result after being resumed.
const PendingIo = struct {
    run_fn: *const fn (*anyopaque) void,
    ctx: *anyopaque,
};

/// Threadlocal holding the active fiber handle for the currently-executing fiber.
/// Set by Worker before each resume, read by interceptAwait.
pub threadlocal var active_fiber_handle: ?*EffectFiber.Handle = null;

/// Original function pointers, set once per worker thread from IoState.
threadlocal var original_await_fn: *const AwaitFn = undefined;
threadlocal var original_cancel_fn: *const CancelFn = undefined;
threadlocal var original_net_accept_fn: *const NetAcceptFn = undefined;
threadlocal var original_net_read_fn: *const NetReadFn = undefined;
threadlocal var original_net_write_fn: *const NetWriteFn = undefined;

/// Threadlocal for passing IO args from interceptAwait to the scheduler.
threadlocal var pending_io: ?PendingIo = null;

const EmitFiberCtx = handler_mod.EmitFiberCtx;


fn interceptAwait(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result_buf: []u8, alignment: std.mem.Alignment) void {
    const handle = active_fiber_handle orelse @panic("interceptAwait called without active fiber handle");
    const Ctx = struct {
        ud: ?*anyopaque,
        fut: *std.Io.AnyFuture,
        buf: []u8,
        align_: std.mem.Alignment,
        orig: *const AwaitFn,
        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.orig(self.ud, self.fut, self.buf, self.align_);
        }
    };
    var ctx = Ctx{ .ud = userdata, .fut = future, .buf = result_buf, .align_ = alignment, .orig = original_await_fn };
    pending_io = .{ .run_fn = &Ctx.run, .ctx = @ptrCast(&ctx) };
    _ = handle.yield(.{ .kind = .@"suspend" });
}

fn interceptCancel(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result_buf: []u8, alignment: std.mem.Alignment) void {
    _ = active_fiber_handle orelse @panic("interceptCancel called without active fiber handle");
    original_cancel_fn(userdata, future, result_buf, alignment);
}

fn interceptNetAccept(userdata: ?*anyopaque, server_handle: net.Socket.Handle) net.Server.AcceptError!net.Stream {
    const h = active_fiber_handle orelse @panic("interceptNetAccept called without active fiber handle");
    const Ctx = struct {
        ud: ?*anyopaque,
        hnd: net.Socket.Handle,
        orig: *const NetAcceptFn,
        result: net.Server.AcceptError!net.Stream = error.NetworkDown,
        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.result = self.orig(self.ud, self.hnd);
        }
    };
    var ctx = Ctx{ .ud = userdata, .hnd = server_handle, .orig = original_net_accept_fn };
    pending_io = .{ .run_fn = &Ctx.run, .ctx = @ptrCast(&ctx) };
    _ = h.yield(.{ .kind = .@"suspend" });
    return ctx.result;
}

fn interceptNetRead(userdata: ?*anyopaque, src: net.Socket.Handle, data: [][]u8) net.Stream.Reader.Error!usize {
    const h = active_fiber_handle orelse @panic("interceptNetRead called without active fiber handle");
    const Ctx = struct {
        ud: ?*anyopaque,
        hnd: net.Socket.Handle,
        data: [][]u8,
        orig: *const NetReadFn,
        result: net.Stream.Reader.Error!usize = error.NetworkDown,
        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.result = self.orig(self.ud, self.hnd, self.data);
        }
    };
    var ctx = Ctx{ .ud = userdata, .hnd = src, .data = data, .orig = original_net_read_fn };
    pending_io = .{ .run_fn = &Ctx.run, .ctx = @ptrCast(&ctx) };
    _ = h.yield(.{ .kind = .@"suspend" });
    return ctx.result;
}

fn interceptNetWrite(userdata: ?*anyopaque, dest: net.Socket.Handle, header: []const u8, data: []const []const u8, splat: usize) net.Stream.Writer.Error!usize {
    const h = active_fiber_handle orelse @panic("interceptNetWrite called without active fiber handle");
    const Ctx = struct {
        ud: ?*anyopaque,
        hnd: net.Socket.Handle,
        header: []const u8,
        data: []const []const u8,
        splat: usize,
        orig: *const NetWriteFn,
        result: net.Stream.Writer.Error!usize = error.NetworkDown,
        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.result = self.orig(self.ud, self.hnd, self.header, self.data, self.splat);
        }
    };
    var ctx = Ctx{ .ud = userdata, .hnd = dest, .header = header, .data = data, .splat = splat, .orig = original_net_write_fn };
    pending_io = .{ .run_fn = &Ctx.run, .ctx = @ptrCast(&ctx) };
    _ = h.yield(.{ .kind = .@"suspend" });
    return ctx.result;
}

const HandlerFiberCtx = handler_mod.HandlerFiberCtx;
const initFiberPooled = types.initFiberPooled;
const initFiberFromBlock = types.initFiberFromBlock;
const resetFiberPooled = types.resetFiberPooled;

/// Stack size for handler/observer fibers — one page of usable stack.
/// Keeps handler blocks small (2 pages total: guard + stack).
const HANDLER_STACK_SIZE: usize = std.heap.page_size_min;

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
// Suspend/wake support
// ============================================================

fn doWakeSuspend(wh: *WakeHandle) void {
    const entry: *Scheduler.FiberEntry = @ptrFromInt(wh._data[0]);
    const worker: *Worker = @ptrFromInt(wh._data[1]);
    const sched: *Scheduler = @ptrFromInt(wh._data[2]);
    entry.pending_effect = .{ .kind = .@"suspend" };
    entry.pending_io = null;
    worker.inbox.push(sched.allocator, entry);
    // Unconditional store avoids missed wakeup when worker is still RUNNING:
    // worker's park() CAS RUNNING→PARKED will fail (sees NOTIFIED), skip sleep.
    @atomicStore(u32, &worker.park_state, PARK_NOTIFIED, .release);
    futexWakeOne(&worker.park_state);
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

// ============================================================
// IO Thread Pool
// ============================================================

/// Elastic thread pool for offloading blocking IO calls.
/// Starts with a base number of threads and grows on demand when all
/// threads are busy (prevents deadlock when long-running blocking calls
/// like accept() exhaust the pool while read/write calls wait).
const IoPool = struct {
    const IoWork = struct {
        run_fn: *const fn (*anyopaque) void,
        ctx: *anyopaque,
        entry: *Scheduler.FiberEntry,
        worker: *Worker,
        sched: *Scheduler,
    };

    mu: std.atomic.Mutex = .unlocked,
    queue: std.ArrayListUnmanaged(IoWork) = .{},
    /// Pending work count — pool threads futex-wait on this.
    pending: u32 = 0,
    /// Number of threads currently idle (waiting for work).
    idle: u32 = 0,
    /// Set to 1 to signal pool threads to exit.
    stop_flag: u32 = 0,
    threads: std.ArrayListUnmanaged(std.Thread) = .{},
    allocator: std.mem.Allocator = undefined,

    fn start(self: *IoPool, allocator: std.mem.Allocator, num_threads: u32) void {
        self.allocator = allocator;
        @atomicStore(u32, &self.stop_flag, 0, .release);
        for (0..num_threads) |_| {
            self.spawnOne();
        }
    }

    fn spawnOne(self: *IoPool) void {
        const t = std.Thread.spawn(.{ .stack_size = 64 * 1024 }, IoPool.poolThread, .{self}) catch @panic("failed to spawn IO pool thread");
        spinLock(&self.mu);
        self.threads.append(self.allocator, t) catch @panic("OOM: IoPool threads");
        self.mu.unlock();
    }

    fn stop(self: *IoPool) void {
        spinLock(&self.mu);
        const n = self.threads.items.len;
        self.mu.unlock();
        if (n == 0) return;

        @atomicStore(u32, &self.stop_flag, 1, .release);
        // Set pending to non-zero so any pool thread that hasn't called
        // futexWait yet will see *pending != 0 in the atomic compare,
        // causing futexWait to return immediately instead of blocking.
        // Without this, a thread between its stop_flag check and futexWait
        // would miss the wakeAll and block forever.
        @atomicStore(u32, &self.pending, 1, .release);
        futexWakeAll(&self.pending);

        // Snapshot thread list — no new threads can be spawned after stop_flag is set
        spinLock(&self.mu);
        const threads = self.threads.items;
        for (threads) |t| {
            t.join();
        }
        self.threads.deinit(self.allocator);
        self.threads = .{};
        self.mu.unlock();
        self.queue.deinit(self.allocator);
        self.queue = .{};
    }

    fn submit(self: *IoPool, work: IoWork) void {
        spinLock(&self.mu);
        self.queue.append(self.allocator, work) catch @panic("OOM: IoPool queue");
        self.mu.unlock();

        _ = @atomicRmw(u32, &self.pending, .Add, 1, .release);

        // If no threads are idle, grow the pool to prevent deadlock
        if (@atomicLoad(u32, &self.idle, .acquire) == 0) {
            self.spawnOne();
        } else {
            futexWakeOne(&self.pending);
        }
    }

    fn poolThread(self: *IoPool) void {
        while (true) {
            // Mark idle, wait for work
            _ = @atomicRmw(u32, &self.idle, .Add, 1, .release);
            while (true) {
                if (@atomicLoad(u32, &self.stop_flag, .acquire) != 0) {
                    _ = @atomicRmw(u32, &self.idle, .Sub, 1, .release);
                    return;
                }
                const p = @atomicLoad(u32, &self.pending, .acquire);
                if (p > 0) {
                    if (@cmpxchgStrong(u32, &self.pending, p, p - 1, .acq_rel, .acquire) == null) {
                        break; // claimed
                    }
                    continue;
                }
                futexWait(&self.pending, 0);
            }
            _ = @atomicRmw(u32, &self.idle, .Sub, 1, .release);

            // Pop and execute work
            spinLock(&self.mu);
            const work = if (self.queue.items.len > 0) self.queue.pop() else null;
            self.mu.unlock();

            if (work) |w| {
                w.run_fn(w.ctx);
                w.entry.pending_io = null;
                w.entry.pending_effect = .{ .kind = .@"suspend" };
                w.worker.inbox.push(w.sched.allocator, w.entry);
                @atomicStore(u32, &w.worker.park_state, PARK_NOTIFIED, .release);
                futexWakeOne(&w.worker.park_state);
            }
        }
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
    /// Per-worker cache of recycled FiberEntries with stacks still attached.
    /// Avoids both GPA alloc/free and StackPool churn for handler/observer fibers.
    warm_cache: ?*Scheduler.FiberEntry = null,
    /// Per-worker cache of persistent handler fibers (alive, parked at sentinel yield).
    /// Keyed by binding's fiber_body pointer. Saves one context switch per invocation.
    persistent: [MAX_PERSISTENT]?PersistentSlot = .{null} ** MAX_PERSISTENT,

    const MAX_PERSISTENT = 8;
    const PersistentSlot = struct {
        key: EffectBodyFn,
        entry: *Scheduler.FiberEntry,
    };

    fn initWorker(alloc: std.mem.Allocator, id: u32, parent: *Scheduler) !Worker {
        return .{
            .deque = try Deque(*Scheduler.FiberEntry).init(alloc, 16),
            .id = id,
            .parent = parent,
            .rng_state = id +| 1,
        };
    }

    fn deinitWorker(self: *Worker) void {
        // Drain deque first — fiber.deinit frees the stack block
        // (which contains the entry in the unified layout).
        while (self.deque.pop()) |entry| {
            entry.fiber.deinit();
        }
        // Drain warm cache — all cached entries use unified allocation.
        // fiber.deinit munmaps the block containing the entry.
        while (self.warm_cache) |entry| {
            self.warm_cache = switch (entry.role) {
                .cached => |next| next,
                else => null,
            };
            entry.fiber.deinit();
        }
        // Drain persistent cache — all persistent entries are unified.
        for (&self.persistent) |*slot| {
            if (slot.*) |s| {
                s.entry.fiber.deinit();
                slot.* = null;
            }
        }
        // Now free all stacks from pool (after deque/cache are drained)
        self.pool.deinit();
        self.deque.deinit();
        self.inbox.deinit(self.parent.allocator);
    }

    const WarmAllocResult = struct {
        entry: *Scheduler.FiberEntry,
        warm: bool,
        /// Block base and size for cold-path init (undefined for warm hits).
        block: [*]u8 = undefined,
        block_size: usize = 0,
    };

    /// Get a cached FiberEntry (with stack attached), or allocate a unified
    /// block from the pool (FiberEntry embedded at top of stack block).
    fn warmAlloc(self: *Worker, stack_size: usize) WarmAllocResult {
        if (self.warm_cache) |entry| {
            self.warm_cache = switch (entry.role) {
                .cached => |next| next,
                else => null,
            };
            return .{ .entry = entry, .warm = true };
        }
        // Cold path: allocate from pool with FiberEntry at top of block
        const ps = Scheduler.PAGE_SIZE;
        const usable = std.mem.alignForward(usize, @max(stack_size, ps), ps);
        const block_size = ps + usable + Scheduler.ENTRY_SIZE;
        const block = self.pool.allocBlock(block_size) orelse @panic("OOM");
        const entry = Scheduler.entryFromBlock(block, block_size);
        return .{ .entry = entry, .warm = false, .block = block, .block_size = block_size };
    }

    /// Return a FiberEntry (with its stack still attached) to the warm cache.
    /// Do NOT call fiber.deinit before this — the stack stays allocated.
    fn warmFree(self: *Worker, entry: *Scheduler.FiberEntry) void {
        entry.role = .{ .cached = self.warm_cache };
        self.warm_cache = entry;
    }

    /// Get a persistent handler fiber from the cache, keyed by binding body.
    fn persistentGet(self: *Worker, key: EffectBodyFn) ?*FiberEntry {
        for (&self.persistent) |*slot| {
            if (slot.*) |s| {
                if (s.key == key) {
                    const entry = s.entry;
                    slot.* = null;
                    return entry;
                }
            }
        }
        return null;
    }

    /// Return a persistent handler fiber to the cache. The fiber is alive,
    /// parked at the sentinel yield, ready for resumeVoid on next invocation.
    fn persistentPut(self: *Worker, key: EffectBodyFn, entry: *FiberEntry) void {
        for (&self.persistent) |*slot| {
            if (slot.* == null) {
                slot.* = .{ .key = key, .entry = entry };
                return;
            }
        }
        // Cache full — destroy excess fiber (unified block, no allocator.destroy)
        entry.fiber.deinit();
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
    /// Performs and emits are dispatched inline in a tight loop.
    /// Only returns to the main loop on suspend or death.
    fn processEntry(self: *Worker, entry: *FiberEntry) void {
        const sched = self.parent;

        // Fast path: if no pending effect, handle unstarted/dead fiber
        if (entry.pending_effect == null) {
            if (entry.fiber.isAlive()) {
                // Stolen entry that hasn't been started yet
                if (entry.fiber.isSuspended()) {
                    activateFiber(entry);
                    entry.pending_effect = entry.fiber.schedulerResumeVoid();
                } else {
                    entry.pending_effect = entry.fiber.schedulerResumeVoid();
                }
                captureFiber(entry);
                entry.pending_io = pending_io;
                pending_io = null;
                // Fall through to the dispatch loop below
            } else {
                // Dead fiber — clean up
                self.completeFiber(entry);
                return;
            }
        }

        // Tight dispatch loop — process performs/emits inline without
        // bouncing through the deque between each effect.
        while (entry.pending_effect) |eff| {
            switch (eff.kind) {
                .perform => {
                    activateFiber(entry);
                    const result = self.dispatchPerformScheduled(&eff, entry, entry.handlers);
                    captureFiber(entry);
                    entry.pending_io = pending_io;
                    pending_io = null;
                    switch (result) {
                        .parked => return, // Origin parked by async handler
                        .effect => |next| entry.pending_effect = next,
                    }
                },
                .emit => {
                    // Spawn real handler fibers for each matching emit binding.
                    // Each handler copies the value to its coroutine stack before
                    // yielding, so the emitter can safely resume after all copies.
                    self.spawnEmitHandlers(&eff, entry.handlers);
                    activateFiber(entry);
                    entry.pending_effect = entry.fiber.schedulerResumeVoid();
                    captureFiber(entry);
                    entry.pending_io = pending_io;
                    pending_io = null;
                },
                .@"suspend" => {
                    if (eff.id == types.SUSPEND_WAKE) {
                        // WakeHandle suspend
                        const wh: *WakeHandle = @ptrCast(@alignCast(eff.value_ptr));
                        wh._data[0] = @intFromPtr(entry);
                        wh._data[1] = @intFromPtr(self);
                        wh._data[2] = @intFromPtr(sched);
                        wh._wake_fn = &doWakeSuspend;
                        if (@cmpxchgStrong(u32, &wh.state, WakeHandle.INIT, WakeHandle.PARKED, .acq_rel, .acquire)) |actual| {
                            if (actual == WakeHandle.WOKEN) {
                                // Early wake — re-enqueue
                                entry.pending_effect = .{ .kind = .@"suspend" };
                                entry.pending_io = null;
                                self.deque.push(entry) catch unreachable;
                            }
                        }
                        return;
                    }
                    if (eff.id == types.SUSPEND_HANDLER_DONE) {
                        // Persistent handler fiber completed (slow path).
                        // Same as completeFiber for handlers, but return to persistent cache.
                        switch (entry.role) {
                            .handler => |hs| {
                                const handler_ctx = hs.ctx;
                                const origin = hs.origin_entry;
                                const original_effect = hs.original_effect;
                                const next_level = hs.next_level;
                                const next_binding_idx = hs.next_binding_idx;
                                self.persistentPut(hs.binding_body, entry);
                                _ = @atomicRmw(isize, &sched.live_fibers, .Sub, 1, .release);
                                self.handlePerformHandlerCompletion(origin, handler_ctx, original_effect, next_level, next_binding_idx);
                                if (@atomicLoad(isize, &sched.live_fibers, .acquire) <= 0) {
                                    @atomicStore(u32, &sched.shutdown, 1, .release);
                                    sched.wakeAll();
                                }
                                return;
                            },
                            else => {}, // fall through to normal suspend handling
                        }
                    }
                    const pio = entry.pending_io orelse blk: {
                        const tls = pending_io;
                        pending_io = null;
                        break :blk tls;
                    };
                    if (pio) |io_args| {
                        // Real IO — hand off to BG thread
                        entry.pending_io = io_args;
                        self.submitIoWait(entry);
                        return;
                    }
                    // No-op suspend — resume immediately
                    entry.pending_io = null;
                    activateFiber(entry);
                    entry.pending_effect = entry.fiber.schedulerResumeVoid();
                    captureFiber(entry);
                    entry.pending_io = pending_io;
                    pending_io = null;
                },
            }
        }

        // pending_effect is null — fiber completed
        self.completeFiber(entry);
    }

    /// Clean up a completed fiber. If it's a handler fiber, handle the
    /// perform outcome (resume/drop/delegate origin).
    fn completeFiber(self: *Worker, entry: *FiberEntry) void {
        const sched = self.parent;

        // Check if this was a handler fiber for an effectful perform
        switch (entry.role) {
            .handler => |hs| {
                // Capture state before caching the entry
                const handler_ctx = hs.ctx;
                const origin = hs.origin_entry;
                const original_effect = hs.original_effect;
                const next_level = hs.next_level;
                const next_binding_idx = hs.next_binding_idx;
                self.warmFree(entry);
                _ = @atomicRmw(isize, &sched.live_fibers, .Sub, 1, .release);
                // Handle the origin fiber's fate
                self.handlePerformHandlerCompletion(origin, handler_ctx, original_effect, next_level, next_binding_idx);
                // Check shutdown after handler completion (origin may have been re-enqueued)
                if (@atomicLoad(isize, &sched.live_fibers, .acquire) <= 0) {
                    @atomicStore(u32, &sched.shutdown, 1, .release);
                    sched.wakeAll();
                }
                return;
            },
            else => {},
        }

        switch (entry.role) {
            .observer => {
                // Scheduler-internal fiber — warm cache for reuse
                self.warmFree(entry);
            },
            .user => {
                // User fiber — leave block allocated. User calls destroyFiber.
            },
            .handler => unreachable, // handled above
            .cached => unreachable, // only set inside warm cache
        }
        const prev = @atomicRmw(isize, &sched.live_fibers, .Sub, 1, .release);
        if (prev <= 1) {
            @atomicStore(u32, &sched.shutdown, 1, .release);
            sched.wakeAll();
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

    /// Submit a fiber's IO to the IO thread pool so the worker stays free.
    fn submitIoWait(self: *Worker, entry: *FiberEntry) void {
        const sched = self.parent;
        const io = entry.pending_io.?;
        sched.io_pool.submit(.{
            .run_fn = io.run_fn,
            .ctx = io.ctx,
            .entry = entry,
            .worker = self,
            .sched = sched,
        });
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
            original_net_accept_fn = ios.original_net_accept;
            original_net_read_fn = ios.original_net_read;
            original_net_write_fn = ios.original_net_write;
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
                    entry.pending_effect = entry.fiber.schedulerResumeVoid();
                } else {
                    entry.pending_effect = entry.fiber.schedulerResumeVoid();
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

    /// Spawn real emit handler fibers for each matching binding in the chain.
    /// Each fiber copies the emit value to its coroutine stack, yields once
    /// (so the emitter can resume), then runs the handler with full effect support.
    /// Handlers that complete synchronously are run inline without deque overhead.
    fn spawnEmitHandlers(self: *Worker, eff: *const RawEffect, handlers: *const HandlerSet) void {
        const sched = self.parent;
        var level: ?*const HandlerSet = handlers;
        while (level) |hs| : (level = hs.parent) {
            for (hs.emit_bindings.items) |binding| {
                if (binding.id == eff.id) {
                    // Set TLS context for the fiber body
                    handler_mod.emit_fiber_ctx_tls = .{
                        .value_ptr = eff.value_ptr,
                        .user_ctx = binding.ctx,
                    };

                    // Create handler fiber (warm cache reuses stack)
                    const alloc = self.warmAlloc(HANDLER_STACK_SIZE);
                    const h_entry = alloc.entry;
                    if (alloc.warm) {
                        resetFiberPooled(&h_entry.fiber, binding.fiber_body);
                    } else {
                        initFiberFromBlock(&h_entry.fiber, &self.pool, binding.fiber_body, alloc.block, alloc.block_size, Scheduler.usableStackInBlock(alloc.block_size));
                    }

                    // Start fiber — it copies value and yields suspend
                    var next_eff = h_entry.fiber.schedulerResumeVoid();

                    // Skip the value-copy suspend
                    if (next_eff) |fe| {
                        if (fe.kind == .@"suspend") {
                            next_eff = h_entry.fiber.schedulerResumeVoid();
                        }
                    }

                    if (next_eff == null) {
                        // === FAST PATH: handler completed synchronously ===
                        self.warmFree(h_entry);
                        continue;
                    }

                    // === SLOW PATH: handler yielded, needs async processing ===
                    h_entry.role = .observer;
                    h_entry.handlers = hs.parent orelse hs;
                    h_entry.pending_effect = next_eff;
                    h_entry.handle = active_fiber_handle;
                    h_entry.pending_io = null;

                    self.deque.push(h_entry) catch unreachable;
                    _ = @atomicRmw(isize, &sched.live_fibers, .Add, 1, .monotonic);
                }
            }
        }
    }

    /// Walk the handler chain (child -> parent). At each level, try simple
    /// bindings first, then effectful bindings.
    ///
    /// Sync handlers run inline. Effectful handlers are spawned as real
    /// scheduled fibers — the origin fiber is parked and resumed when the
    /// handler completes.
    ///
    /// Returns `.effect` with the next effect (or null for completed),
    /// or `.parked` if the origin was parked by an async handler.
    const PerformDispatchResult = union(enum) {
        effect: ?RawEffect,
        parked,
    };

    fn dispatchPerformScheduled(
        self: *Worker,
        eff: *const RawEffect,
        origin_entry: *FiberEntry,
        handlers: ?*const HandlerSet,
    ) PerformDispatchResult {
        return self.dispatchPerformFrom(eff, origin_entry, handlers, null, 0);
    }

    /// Inner dispatch that supports resuming from a specific position (for delegation).
    fn dispatchPerformFrom(
        self: *Worker,
        eff: *const RawEffect,
        origin_entry: *FiberEntry,
        handlers: ?*const HandlerSet,
        skip_level: ?*const HandlerSet,
        skip_effectful_idx: usize,
    ) PerformDispatchResult {
        const sched = self.parent;
        var level: ?*const HandlerSet = handlers;
        while (level) |hs| : (level = hs.parent) {
            // Simple bindings (always inline)
            for (hs.perform_bindings.items) |binding| {
                if (binding.id == eff.id) {
                    switch (binding.handler(eff, &origin_entry.fiber, binding.ctx)) {
                        .handled => |next| return .{ .effect = next },
                        .skipped => {},
                    }
                }
            }

            // Effectful bindings — persistent fibers eliminate one context switch
            for (hs.effectful_bindings.items, 0..) |binding, idx| {
                if (binding.id != eff.id) continue;

                // Skip bindings we've already tried (delegation resume)
                if (skip_level) |sl| {
                    if (sl == hs and idx < skip_effectful_idx) continue;
                }

                // Try persistent cache first (one context switch via resumeVoid)
                var h_entry: *FiberEntry = undefined;
                var from_persistent = false;

                if (self.persistentGet(binding.fiber_body)) |cached| {
                    h_entry = cached;
                    h_entry.role.handler.ctx = .{ .raw = eff, .user_ctx = binding.ctx };
                    from_persistent = true;
                } else {
                    // Allocate new entry (warm cache or fresh)
                    const alloc = self.warmAlloc(HANDLER_STACK_SIZE);
                    h_entry = alloc.entry;
                    h_entry.role = .{ .handler = .{
                        .ctx = .{ .raw = eff, .user_ctx = binding.ctx },
                        .origin_entry = undefined, // set in slow path only
                        .original_effect = undefined,
                        .next_level = undefined,
                        .next_binding_idx = undefined,
                        .binding_body = binding.fiber_body,
                    } };
                    if (alloc.warm) {
                        resetFiberPooled(&h_entry.fiber, binding.fiber_body);
                    } else {
                        initFiberFromBlock(&h_entry.fiber, &self.pool, binding.fiber_body, alloc.block, alloc.block_size, Scheduler.usableStackInBlock(alloc.block_size));
                    }
                }

                handler_mod.handler_fiber_ctx_tls = &h_entry.role.handler.ctx;

                // Resume (persistent) or start (new) the handler fiber
                const first_eff_result = if (from_persistent)
                    h_entry.fiber.schedulerResumeVoid()
                else
                    h_entry.fiber.schedulerResumeVoid();

                // Check for fast-path completion (sentinel suspend from persistent body)
                const is_done = if (first_eff_result) |fe|
                    (fe.kind == .@"suspend" and fe.id == types.SUSPEND_HANDLER_DONE)
                else
                    false;

                if (first_eff_result == null or is_done) {
                    // === FAST PATH: handler completed synchronously ===
                    const ctx = h_entry.role.handler.ctx;
                    if (is_done) {
                        self.persistentPut(binding.fiber_body, h_entry);
                    } else {
                        // Fiber died unexpectedly — warm cache it
                        self.warmFree(h_entry);
                    }

                    if (ctx.delegated) continue; // try next binding
                    if (ctx.resumed) return .{ .effect = origin_entry.fiber.schedulerResumeVoid() };
                    // Dropped or auto-drop — deinit origin (completeFiber handles cleanup)
                    origin_entry.fiber.deinit();
                    return .{ .effect = null };
                }

                // === SLOW PATH: handler yielded, needs async scheduling ===
                h_entry.role.handler.origin_entry = origin_entry;
                h_entry.role.handler.original_effect = eff.*;
                h_entry.role.handler.next_level = hs;
                h_entry.role.handler.next_binding_idx = idx + 1;
                h_entry.role.handler.binding_body = binding.fiber_body;
                // Point ctx.raw at the entry's stable copy (original eff may be stack-local)
                h_entry.role.handler.ctx.raw = &h_entry.role.handler.original_effect;

                h_entry.handlers = hs.parent orelse hs;
                h_entry.pending_effect = first_eff_result;
                h_entry.handle = active_fiber_handle;
                h_entry.pending_io = pending_io;
                pending_io = null;

                // Push handler to deque, increment live_fibers
                self.deque.push(h_entry) catch unreachable;
                _ = @atomicRmw(isize, &sched.live_fibers, .Add, 1, .monotonic);

                // Origin is parked — don't re-enqueue it
                return .parked;
            }
        }

        // Chain exhausted — no handler accepted. Resume with zeroed value.
        return .{ .effect = origin_entry.fiber.schedulerResumeVoid() };
    }

    /// Called when a handler fiber completes. Handles the Cont outcome
    /// (resume/drop/delegate) for the origin fiber.
    fn handlePerformHandlerCompletion(
        self: *Worker,
        origin: *FiberEntry,
        ctx: HandlerFiberCtx,
        original_effect: RawEffect,
        next_level: ?*const HandlerSet,
        next_binding_idx: usize,
    ) void {
        const sched = self.parent;

        if (ctx.delegated) {
            // Continue searching from the next binding.
            // original_effect is a stack-local copy — stable for the call.
            var eff_copy = original_effect;
            const result = self.dispatchPerformFrom(
                &eff_copy,
                origin,
                next_level,
                next_level,
                next_binding_idx,
            );
            switch (result) {
                .parked => {}, // Another async handler took over
                .effect => |next_eff| {
                    origin.pending_effect = next_eff;
                    origin.pending_io = null;
                    self.deque.push(origin) catch unreachable;
                },
            }
            return;
        }

        if (ctx.resumed) {
            // Cont wrote the resume value. Re-enqueue origin.
            origin.pending_effect = null;
            origin.pending_io = null;
            self.deque.push(origin) catch unreachable;
            return;
        }

        // Dropped or auto-drop — destroy origin fiber.
        // For user fibers, leave the block for destroyFiber.
        // For observer fibers, deinit immediately.
        switch (origin.role) {
            .user => {},
            .observer => origin.fiber.deinit(),
            .handler => origin.fiber.deinit(),
            .cached => unreachable,
        }
        const prev = @atomicRmw(isize, &sched.live_fibers, .Sub, 1, .release);
        if (prev <= 1) {
            @atomicStore(u32, &sched.shutdown, 1, .release);
            sched.wakeAll();
        }
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
    io_pool: IoPool = .{},

    const IoState = struct {
        vtable: std.Io.VTable,
        userdata: ?*anyopaque,
        original_await: *const AwaitFn,
        original_cancel: *const CancelFn,
        original_net_accept: *const NetAcceptFn,
        original_net_read: *const NetReadFn,
        original_net_write: *const NetWriteFn,

        fn wrappedIo(self: *IoState) std.Io {
            return .{ .userdata = self.userdata, .vtable = &self.vtable };
        }
    };

    const PAGE_SIZE: usize = std.heap.page_size_min;

    /// Size of FiberEntry, aligned to 16 bytes for stack alignment.
    const ENTRY_SIZE: usize = std.mem.alignForward(usize, @sizeOf(FiberEntry), 16);

    /// Derive a FiberEntry pointer from a stack block. The entry lives at
    /// the top of the block (above the stack growth direction).
    fn entryFromBlock(block: [*]u8, block_size: usize) *FiberEntry {
        return @ptrCast(@alignCast(block + block_size - ENTRY_SIZE));
    }

    /// Usable stack size within a block after reserving space for the guard
    /// page and the embedded FiberEntry.
    fn usableStackInBlock(block_size: usize) usize {
        return block_size - PAGE_SIZE - ENTRY_SIZE;
    }

    pub const FiberEntry = struct {
        // --- Cache line 0: dispatch loop hot path ---
        // pending_effect, handle, pending_io are checked/updated every
        // iteration of the dispatch loop in processEntry.
        pending_effect: ?RawEffect,
        handle: ?*EffectFiber.Handle,
        pending_io: ?PendingIo = null,
        handlers: *const HandlerSet,
        // --- Cache line 1+: cold / setup ---
        // fiber is large (~168 bytes) but only accessed on start/resume
        // (once per processEntry call), not in the inner dispatch loop.
        fiber: EffectFiber,
        role: Role = .user,

        const Role = union(enum) {
            /// User-created fiber (via createFiber). User calls destroyFiber.
            user,
            /// Scheduler-internal fiber (emit observer, handler slow path).
            /// Warm-cached on completion.
            observer,
            handler: HandlerState,
            /// Next pointer in the per-worker warm cache.
            cached: ?*FiberEntry,
        };

        const HandlerState = struct {
            /// Handler outcome flags — lives on the entry so the fiber's
            /// pointer to it (captured via TLS) remains valid for the lifetime.
            ctx: HandlerFiberCtx,
            /// The origin fiber's entry (the one that called perform).
            origin_entry: *FiberEntry,
            /// Copy of the effect — needed for re-dispatch on delegation.
            original_effect: RawEffect,
            /// Where to continue searching if handler delegates.
            next_level: ?*const HandlerSet,
            next_binding_idx: usize,
            /// The binding's fiber body — used as key for persistent cache return.
            binding_body: EffectBodyFn,
        };
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
        self.io_pool.stop();
        for (self.workers) |*w| {
            w.deinitWorker();
        }
        self.allocator.free(self.workers);
        self.spawn_buffer.deinit(self.allocator);
    }

    pub fn spawn(self: *Scheduler, entry: *FiberEntry, handlers: *const HandlerSet) !void {
        self.fixupParents();
        entry.handlers = handlers;
        try self.spawn_buffer.append(self.allocator, entry);
        _ = @atomicRmw(isize, &self.live_fibers, .Add, 1, .monotonic);
    }

    fn setupIo(self: *Scheduler, inner: std.Io) void {
        var vt = inner.vtable.*;
        const orig_await = vt.await;
        const orig_cancel = vt.cancel;
        const orig_net_accept = vt.netAccept;
        const orig_net_read = vt.netRead;
        const orig_net_write = vt.netWrite;
        vt.await = &interceptAwait;
        vt.cancel = &interceptCancel;
        vt.netAccept = &interceptNetAccept;
        vt.netRead = &interceptNetRead;
        vt.netWrite = &interceptNetWrite;
        self.io_state = .{
            .vtable = vt,
            .userdata = inner.userdata,
            .original_await = orig_await,
            .original_cancel = orig_cancel,
            .original_net_accept = orig_net_accept,
            .original_net_read = orig_net_read,
            .original_net_write = orig_net_write,
        };
    }

    /// Set the IO backend for all fibers. Call before createFiber.
    /// If not called, fibers get a noop IO.
    pub fn setIo(self: *Scheduler, inner_io: std.Io) void {
        self.setupIo(inner_io);
    }

    pub fn createFiber(self: *Scheduler, body: EffectBodyFn, stack_size: usize) !*FiberEntry {
        self.fixupParents();
        if (self.io_state == null) {
            var noop_vt = makeNoopVtable();
            self.setupIo(.{ .userdata = null, .vtable = &noop_vt });
        }

        const Static = struct {
            threadlocal var current_body: EffectBodyFn = undefined;
            threadlocal var current_io: std.Io = undefined;
        };
        Static.current_body = body;
        Static.current_io = self.io_state.?.wrappedIo();

        // Unified block: [guard page | usable stack | FiberEntry]
        const ps = PAGE_SIZE;
        const raw_stack = if (stack_size == 0) 64 * 1024 else stack_size;
        const usable = std.mem.alignForward(usize, @max(raw_stack, ps), ps);
        const block_size = ps + usable + ENTRY_SIZE;
        const block = self.workers[0].pool.allocBlock(block_size) orelse return error.OutOfMemory;
        const entry = entryFromBlock(block, block_size);

        initFiberFromBlock(&entry.fiber, &self.workers[0].pool, &struct {
            fn wrapper(ctx: *EffectContext) void {
                const b = Static.current_body;
                const io = Static.current_io;
                active_fiber_handle = ctx.handle;
                ctx.io = io;
                _ = ctx.handle.yield(.{ .kind = .@"suspend" });
                b(ctx);
            }
        }.wrapper, block, block_size, usableStackInBlock(block_size));

        _ = entry.fiber.schedulerResumeVoid();

        const handle = active_fiber_handle orelse @panic("createFiber: wrapper did not set active_fiber_handle");

        entry.handle = handle;
        entry.pending_effect = null;
        entry.pending_io = null;
        entry.role = .user;

        return entry;
    }

    /// Destroy a fiber entry created by createFiber.
    /// Safe to call after run() — deinit is idempotent for dead fibers.
    /// The entry pointer is invalid after this call (it lives in the
    /// stack block that gets freed).
    pub fn destroyFiber(self: *Scheduler, entry: *FiberEntry) void {
        _ = self;
        entry.fiber.deinit();
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

        // Start IO thread pool if IO is configured
        if (self.io_state != null) {
            self.io_pool.start(self.allocator, self.num_workers * 2);
        }

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

        // Stop IO thread pool
        self.io_pool.stop();
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
    sched.setIo(mock_io);

    const res = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            // Simulate an IO await by calling await through the io interface
            var dummy_future: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy_future), &.{}, .@"1");
            ctx.emit(Result, 42);
        }
    }.body, 0);
    defer sched.destroyFiber(res);

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onEmit(Result, &struct {
        fn handle(val: *const Result.Value, _: *EffectContext, raw_ctx: ?*anyopaque) void {
            const r: *usize = @ptrCast(@alignCast(raw_ctx.?));
            r.* = val.*;
        }
    }.handle, @ptrCast(&result_val));

    try sched.spawn(res, &handlers);
    sched.run();

    try testing.expectEqual(@as(usize, 1), State.await_count);
    try testing.expectEqual(@as(usize, 42), result_val);
    try testing.expect(!res.fiber.isAlive());
}

test "Scheduler: two fibers LIFO depth-first" {
    // With async emit, observer fibers run independently so cross-fiber ordering
    // is non-deterministic. Verify all 4 emit values appear (count-based).

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
    sched.setIo(mock_io);

    const res1 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Trace, 'A');
            // Trigger an IO await to yield
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(Trace, 'B');
        }
    }.body, 0);
    defer sched.destroyFiber(res1);

    const res2 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Trace, '1');
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(Trace, '2');
        }
    }.body, 0);
    defer sched.destroyFiber(res2);

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onEmit(Trace, &struct {
        fn handle(val: *const Trace.Value, _: *EffectContext, _: ?*anyopaque) void {
            State.order[State.idx] = val.*;
            State.idx += 1;
        }
    }.handle, null);

    try sched.spawn(res1, &handlers);
    try sched.spawn(res2, &handlers);
    sched.run();

    // Both fibers should complete
    try testing.expect(!res1.fiber.isAlive());
    try testing.expect(!res2.fiber.isAlive());

    // All 4 emit values should appear (order is non-deterministic with async emit)
    const trace = State.order[0..State.idx];
    try testing.expectEqual(@as(usize, 4), trace.len);
    var counts = [_]u8{0} ** 256;
    for (trace) |c| counts[c] += 1;
    try testing.expectEqual(@as(u8, 1), counts['A']);
    try testing.expectEqual(@as(u8, 1), counts['B']);
    try testing.expectEqual(@as(u8, 1), counts['1']);
    try testing.expectEqual(@as(u8, 1), counts['2']);
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
    sched.setIo(mock_io);

    const res = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            // Perform an algebraic effect
            const val = ctx.perform(GetVal, {});
            // Do some IO
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            // Emit the result
            ctx.emit(Result, val * 2);
        }
    }.body, 0);
    defer sched.destroyFiber(res);

    const cont_mod = @import("effect/cont.zig");

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onPerformSync(GetVal, &struct {
        fn handle(_: *GetVal.Value, cont: *cont_mod.Cont(GetVal), _: ?*anyopaque) void {
            cont.@"resume"(21);
        }
    }.handle, null);
    handlers.onEmit(Result, &struct {
        fn handle(val: *const Result.Value, _: *EffectContext, _: ?*anyopaque) void {
            State.result = val.*;
        }
    }.handle, null);

    try sched.spawn(res, &handlers);
    sched.run();

    try testing.expectEqual(@as(i32, 42), State.result);
    try testing.expect(!res.fiber.isAlive());
}

// ============================================================
// Multicore tests
// ============================================================

test "Scheduler: 100 fibers, 4 workers, atomic counter" {
    const N = 100;

    const BenchPerform = types.Perform(u64, u64);
    const cont_mod = @import("effect/cont.zig");

    var counter: isize = 0;

    var sched = try Scheduler.init(testing.allocator, 4);
    defer sched.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onPerformSync(BenchPerform, &struct {
        fn handle(_: *BenchPerform.Value, cont: *cont_mod.Cont(BenchPerform), _: ?*anyopaque) void {
            cont.@"resume"(1);
        }
    }.handle, null);

    var results: [N]*Scheduler.FiberEntry = undefined;
    for (0..N) |i| {
        results[i] = try sched.createFiber(&struct {
            fn body(ctx: *EffectContext) void {
                // Perform an effect that returns 1
                const r = ctx.perform(BenchPerform, 0);
                std.mem.doNotOptimizeAway(r);
            }
        }.body, 0);
        try sched.spawn(results[i], &handlers);
        // Atomic inc for each fiber that will complete
        _ = @atomicRmw(isize, &counter, .Add, 0, .monotonic); // just to reference counter
    }

    sched.run();

    // All fibers should be dead
    for (0..N) |i| {
        try testing.expect(!results[i].fiber.isAlive());
        sched.destroyFiber(results[i]);
    }
}

test "Scheduler: zero fibers, run returns immediately" {
    var sched = try Scheduler.init(testing.allocator, 4);
    defer sched.deinit();
    sched.run();
    // Should not hang
}

test "Scheduler: two fibers, 2 workers, both complete" {
    const BenchPerform = types.Perform(u64, u64);
    const cont_mod = @import("effect/cont.zig");

    var sched = try Scheduler.init(testing.allocator, 2);
    defer sched.deinit();

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onPerformSync(BenchPerform, &struct {
        fn handle(_: *BenchPerform.Value, cont: *cont_mod.Cont(BenchPerform), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);

    const res1 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            for (0..100) |i| {
                const r = ctx.perform(BenchPerform, i);
                std.mem.doNotOptimizeAway(r);
            }
        }
    }.body, 0);

    const res2 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            for (0..100) |i| {
                const r = ctx.perform(BenchPerform, i);
                std.mem.doNotOptimizeAway(r);
            }
        }
    }.body, 0);

    try sched.spawn(res1, &handlers);
    try sched.spawn(res2, &handlers);
    sched.run();

    try testing.expect(!res1.fiber.isAlive());
    try testing.expect(!res2.fiber.isAlive());
    sched.destroyFiber(res1);
    sched.destroyFiber(res2);
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
    sched.setIo(mock_io);

    const res1 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
        }
    }.body, 0);
    defer sched.destroyFiber(res1);

    const res2 = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
        }
    }.body, 0);
    defer sched.destroyFiber(res2);

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    try sched.spawn(res1, &handlers);
    try sched.spawn(res2, &handlers);

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
    sched.setIo(mock_io);

    const res = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(GetVal, {});
            var dummy: DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(Result, val * 3);
        }
    }.body, 0);
    defer sched.destroyFiber(res);

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onPerformSync(GetVal, &struct {
        fn handle(_: *GetVal.Value, cont: *cont_mod.Cont(GetVal), _: ?*anyopaque) void {
            cont.@"resume"(7);
        }
    }.handle, null);
    handlers.onEmit(Result, &struct {
        fn handle(val: *const Result.Value, _: *EffectContext, _: ?*anyopaque) void {
            State.result = val.*;
        }
    }.handle, null);

    try sched.spawn(res, &handlers);
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
    sched.setIo(mock_io);

    const res = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            for (0..3) |_| {
                var dummy: DummyFuture = .{};
                ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            }
        }
    }.body, 0);
    defer sched.destroyFiber(res);

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    try sched.spawn(res, &handlers);
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
    sched.setIo(mock_io);

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();

    var results: [N]*Scheduler.FiberEntry = undefined;
    for (0..N) |i| {
        results[i] = try sched.createFiber(&struct {
            fn body(ctx: *EffectContext) void {
                var dummy: DummyFuture = .{};
                ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            }
        }.body, 0);
        try sched.spawn(results[i], &handlers);
    }

    sched.run();

    try testing.expectEqual(@as(u32, N), State.completed.load(.acquire));
    for (0..N) |i| {
        try testing.expect(!results[i].fiber.isAlive());
        sched.destroyFiber(results[i]);
    }
}

// ============================================================
// Suspend/wake tests
// ============================================================

test "Scheduler: early wake resumes immediately" {
    // Fiber calls wake() before suspend() — scheduler sees WOKEN, resumes immediately.
    const Done = types.Emit(u32);

    const State = struct {
        var value: u32 = 0;
    };
    State.value = 0;

    var sched = try Scheduler.init(testing.allocator, 1);
    defer sched.deinit();

    const res = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            var wh: WakeHandle = .{};
            wh.wake(); // early wake: INIT → WOKEN
            ctx.@"suspend"(&wh); // scheduler sees WOKEN → resumes immediately
            ctx.emit(Done, 42);
        }
    }.body, 0);
    defer sched.destroyFiber(res);

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onEmit(Done, &struct {
        fn handle(val: *const Done.Value, _: *EffectContext, _: ?*anyopaque) void {
            State.value = val.*;
        }
    }.handle, null);

    try sched.spawn(res, &handlers);
    sched.run();

    try testing.expectEqual(@as(u32, 42), State.value);
    try testing.expect(!res.fiber.isAlive());
}

test "Scheduler: suspend + wake from another fiber" {
    // Fiber A suspends. Fiber B wakes it (possibly early wake). Fiber A resumes and emits.
    const Done = types.Emit(u32);

    const Shared = struct {
        var wh: WakeHandle = .{};
        var value: u32 = 0;
    };
    Shared.wh = .{};
    Shared.value = 0;

    var sched = try Scheduler.init(testing.allocator, 2);
    defer sched.deinit();

    // Fiber A: suspend, emit after resume
    const res_a = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.@"suspend"(&Shared.wh);
            ctx.emit(Done, 99);
        }
    }.body, 0);
    defer sched.destroyFiber(res_a);

    // Fiber B: sleep briefly then wake A
    const res_b = try sched.createFiber(&struct {
        fn body(_: *EffectContext) void {
            // Brief sleep to let scheduler process A's suspend
            var ts: std.c.timespec = .{ .sec = 0, .nsec = 5_000_000 }; // 5ms
            _ = std.c.nanosleep(&ts, null);
            Shared.wh.wake();
        }
    }.body, 0);
    defer sched.destroyFiber(res_b);

    var handlers = HandlerSet.init(testing.allocator);
    defer handlers.deinit();
    handlers.onEmit(Done, &struct {
        fn handle(val: *const Done.Value, _: *EffectContext, _: ?*anyopaque) void {
            Shared.value = val.*;
        }
    }.handle, null);

    try sched.spawn(res_a, &handlers);
    try sched.spawn(res_b, &handlers);
    sched.run();

    try testing.expectEqual(@as(u32, 99), Shared.value);
    try testing.expect(!res_a.fiber.isAlive());
    try testing.expect(!res_b.fiber.isAlive());
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
