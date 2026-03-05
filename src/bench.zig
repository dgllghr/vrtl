const std = @import("std");
const fiber_mod = @import("fiber.zig");
const types = @import("effect/types.zig");
const handler_mod = @import("effect/handler.zig");
const dispatch_mod = @import("effect/dispatch.zig");
const cont_mod = @import("effect/cont.zig");
const sched_mod = @import("scheduler.zig");
const pool_mod = @import("pool.zig");
const deque_mod = @import("deque.zig");

const EffectFiber = types.EffectFiber;
const EffectContext = types.EffectContext;
const HandlerSet = handler_mod.HandlerSet;
const Cont = cont_mod.Cont;

const BenchEmit = types.Emit(u64);
const BenchPerform = types.Perform(u64, u64);

const allocator = std.heap.page_allocator;

fn clockNs() u64 {
    // Read ARM64 hardware counter directly — no libc needed
    const cnt: u64 = asm volatile ("mrs %[cnt], cntvct_el0"
        : [cnt] "=r" (-> u64),
    );
    const freq: u64 = asm volatile ("mrs %[freq], cntfrq_el0"
        : [freq] "=r" (-> u64),
    );
    return @intCast(@as(u128, cnt) * 1_000_000_000 / freq);
}


pub fn main() void {
    const p = std.debug.print;

    p("\n{s:<40} {s:>12} {s:>14} {s:>10}\n", .{
        "Benchmark", "Iterations", "Total (ms)", "ns/op",
    });
    p("{s}\n", .{"-" ** 78});

    bench(p, "fiber create/destroy", 100_000, benchFiberCreateDestroy);
    bench(p, "fiber create/destroy (pooled)", 100_000, benchFiberCreateDestroyPooled);
    bench(p, "yield/resume round-trips", 1_000_000, benchYieldResume);
    bench(p, "emit dispatch", 1_000_000, benchEmitDispatch);
    bench(p, "perform dispatch", 1_000_000, benchPerformDispatch);
    bench(p, "scheduler 1W 10x1000 performs", 100, benchScheduler);
    bench(p, "scheduler 4W 40x1000 performs", 10, benchSchedulerMulticore);
    bench(p, "deque push/pop", 1_000_000, benchDequePushPop);
    bench(p, "deque 1-owner 4-thieves", 100, benchDequeSteal);

    p("\n", .{});
}

fn bench(
    p: *const fn (comptime []const u8, anytype) void,
    name: []const u8,
    iters: usize,
    bench_fn: *const fn (usize) u64,
) void {
    const elapsed = bench_fn(iters);
    const ns_op = elapsed / iters;
    const ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
    p("{s:<40} {d:>12} {d:>14.3} {d:>10}\n", .{ name, iters, ms, ns_op });
}

// ============================================================
// 1. Fiber create/destroy
// ============================================================

fn benchFiberCreateDestroy(iters: usize) u64 {
    const start = clockNs();
    for (0..iters) |_| {
        var f = types.initFiberDefault(&struct {
            fn body(_: *EffectContext) void {}
        }.body) catch @panic("init");
        f.deinit();
    }
    return clockNs() - start;
}

// ============================================================
// 1b. Fiber create/destroy (pooled)
// ============================================================

fn benchFiberCreateDestroyPooled(iters: usize) u64 {
    var pool = pool_mod.StackPool{};
    defer pool.deinit();
    const start = clockNs();
    for (0..iters) |_| {
        var f = types.initFiberPooled(&pool, &struct {
            fn body(_: *EffectContext) void {}
        }.body) catch @panic("init");
        f.deinit();
    }
    return clockNs() - start;
}

// ============================================================
// 2. Yield/resume round-trips
// ============================================================

fn benchYieldResume(iters: usize) u64 {
    const F = fiber_mod.Fiber(void, void);
    const S = struct {
        threadlocal var n: usize = 0;
    };
    S.n = iters;

    var f = F.initDefault(&struct {
        fn body(h: *F.Handle) void {
            for (0..S.n) |_| {
                h.yieldVoid();
            }
        }
    }.body) catch @panic("init");
    defer f.deinit();

    const start = clockNs();
    _ = f.start();
    for (0..iters) |_| {
        _ = f.resumeVoid();
    }
    return clockNs() - start;
}

// ============================================================
// 3. Emit dispatch
// ============================================================

fn benchEmitDispatch(iters: usize) u64 {
    const S = struct {
        threadlocal var n: usize = 0;
    };
    S.n = iters;

    var counter: u64 = 0;

    var fib = types.initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            for (0..S.n) |i| {
                ctx.emit(BenchEmit, i);
            }
        }
    }.body) catch @panic("init");
    defer fib.deinit();

    var handlers = HandlerSet.init(allocator);
    defer handlers.deinit();
    handlers.onEmit(BenchEmit, &struct {
        fn handle(_: *const BenchEmit.Value, ctx: ?*anyopaque) void {
            const c: *u64 = @ptrCast(@alignCast(ctx.?));
            c.* += 1;
        }
    }.handle, @ptrCast(&counter));

    const start = clockNs();
    dispatch_mod.run(&fib, &handlers);
    const elapsed = clockNs() - start;
    std.mem.doNotOptimizeAway(counter);
    return elapsed;
}

// ============================================================
// 4. Perform dispatch
// ============================================================

fn benchPerformDispatch(iters: usize) u64 {
    const S = struct {
        threadlocal var n: usize = 0;
    };
    S.n = iters;

    var fib = types.initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            for (0..S.n) |i| {
                const r = ctx.perform(BenchPerform, i);
                std.mem.doNotOptimizeAway(r);
            }
        }
    }.body) catch @panic("init");
    defer fib.deinit();

    var handlers = HandlerSet.init(allocator);
    defer handlers.deinit();
    handlers.onPerform(BenchPerform, &struct {
        fn handle(_: *BenchPerform.Value, cont: *Cont(BenchPerform), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);

    const start = clockNs();
    dispatch_mod.run(&fib, &handlers);
    return clockNs() - start;
}

// ============================================================
// 5. Scheduler: N fibers x M performs
// ============================================================

fn benchScheduler(iters: usize) u64 {
    const N = 10;
    const M = 1000;

    var mock_vt = sched_mod.makeNoopVtable();
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    const start = clockNs();
    for (0..iters) |_| {
        var sched = sched_mod.Scheduler.init(allocator, 1) catch @panic("OOM");

        var handlers = HandlerSet.init(allocator);
        handlers.onPerform(BenchPerform, &struct {
            fn handle(_: *BenchPerform.Value, cont: *Cont(BenchPerform), _: ?*anyopaque) void {
                cont.@"resume"(42);
            }
        }.handle, null);

        var results: [N]sched_mod.FiberResult = undefined;
        for (0..N) |i| {
            results[i] = sched.createFiber(&struct {
                fn body(ctx: *EffectContext) void {
                    for (0..M) |j| {
                        const r = ctx.perform(BenchPerform, j);
                        std.mem.doNotOptimizeAway(r);
                    }
                }
            }.body, mock_io, 0) catch @panic("init");
            sched.spawn(&results[i].fiber, results[i].handle, &handlers) catch @panic("spawn");
        }

        sched.run();

        for (0..N) |i| {
            results[i].fiber.deinit();
        }
        handlers.deinit();
        sched.deinit();
    }
    return clockNs() - start;
}

// ============================================================
// 5b. Scheduler multicore: N fibers x M performs, K workers
// ============================================================

fn benchSchedulerMulticore(iters: usize) u64 {
    const N = 40;
    const M = 1000;
    const W = 4;

    var mock_vt = sched_mod.makeNoopVtable();
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    const start = clockNs();
    for (0..iters) |_| {
        var sched = sched_mod.Scheduler.init(allocator, W) catch @panic("OOM");

        var handlers = HandlerSet.init(allocator);
        handlers.onPerform(BenchPerform, &struct {
            fn handle(_: *BenchPerform.Value, cont: *Cont(BenchPerform), _: ?*anyopaque) void {
                cont.@"resume"(42);
            }
        }.handle, null);

        var results: [N]sched_mod.FiberResult = undefined;
        for (0..N) |i| {
            results[i] = sched.createFiber(&struct {
                fn body(ctx: *EffectContext) void {
                    for (0..M) |j| {
                        const r = ctx.perform(BenchPerform, j);
                        std.mem.doNotOptimizeAway(r);
                    }
                }
            }.body, mock_io, 0) catch @panic("init");
            sched.spawn(&results[i].fiber, results[i].handle, &handlers) catch @panic("spawn");
        }

        sched.run();

        for (0..N) |i| {
            results[i].fiber.deinit();
        }
        handlers.deinit();
        sched.deinit();
    }
    return clockNs() - start;
}

// ============================================================
// 6. Deque push/pop (single-threaded owner throughput)
// ============================================================

fn benchDequePushPop(iters: usize) u64 {
    const Deque = deque_mod.WorkStealingDeque(u64);
    var d = Deque.init(allocator, 1024) catch @panic("OOM");
    defer d.deinit();

    const start = clockNs();
    for (0..iters) |i| {
        d.push(i) catch unreachable;
        std.mem.doNotOptimizeAway(d.pop());
    }
    return clockNs() - start;
}

// ============================================================
// 7. Deque 1-owner + 4-thieves (multi-threaded work-stealing)
// ============================================================

fn benchDequeSteal(iters: usize) u64 {
    // Each iteration: owner pushes 100k items in waves, pops between waves.
    // 4 thief threads steal concurrently. Measures total throughput.
    const num_thieves = 4;
    const items_per_iter: u64 = 100_000;
    const wave_size: u64 = 500;

    const Deque = deque_mod.WorkStealingDeque(u64);

    const ThiefCtx = struct {
        deque: *Deque,
        done: *u32,
        stolen: u64,
    };

    const start = clockNs();

    for (0..iters) |_| {
        var deque = Deque.init(allocator, 1024) catch @panic("OOM");
        defer deque.deinit();

        var done: u32 = 0;
        var ctxs: [num_thieves]ThiefCtx = undefined;
        var threads: [num_thieves]std.Thread = undefined;

        for (0..num_thieves) |i| {
            ctxs[i] = .{ .deque = &deque, .done = &done, .stolen = 0 };
            threads[i] = std.Thread.spawn(.{}, struct {
                fn run(ctx: *ThiefCtx) void {
                    var count: u64 = 0;
                    while (true) {
                        switch (ctx.deque.steal()) {
                            .success => count += 1,
                            .abort => continue,
                            .empty => {
                                if (@atomicLoad(u32, ctx.done, .acquire) == 1) {
                                    while (true) {
                                        switch (ctx.deque.steal()) {
                                            .success => count += 1,
                                            .abort => continue,
                                            .empty => {
                                                ctx.stolen = count;
                                                return;
                                            },
                                        }
                                    }
                                }
                            },
                        }
                    }
                }
            }.run, .{&ctxs[i]}) catch @panic("spawn");
        }

        // Owner: push in waves, pop half between waves.
        var pushed: u64 = 0;
        var owner_popped: u64 = 0;
        while (pushed < items_per_iter) {
            const wave_end = @min(pushed + wave_size, items_per_iter);
            while (pushed < wave_end) : (pushed += 1) {
                deque.push(pushed) catch unreachable;
            }
            for (0..wave_size / 2) |_| {
                if (deque.pop()) |_| {
                    owner_popped += 1;
                }
            }
        }
        while (deque.pop()) |_| {
            owner_popped += 1;
        }
        @atomicStore(u32, &done, 1, .release);

        var total_stolen: u64 = 0;
        for (0..num_thieves) |i| {
            threads[i].join();
            total_stolen += ctxs[i].stolen;
        }
        std.mem.doNotOptimizeAway(owner_popped);
        std.mem.doNotOptimizeAway(total_stolen);
    }

    return clockNs() - start;
}
