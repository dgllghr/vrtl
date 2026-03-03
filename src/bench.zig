const std = @import("std");
const fiber_mod = @import("fiber.zig");
const types = @import("effect/types.zig");
const handler_mod = @import("effect/handler.zig");
const dispatch_mod = @import("effect/dispatch.zig");
const cont_mod = @import("effect/cont.zig");
const io_mod = @import("io.zig");

const EffectFiber = types.EffectFiber;
const EffectContext = types.EffectContext;
const HandlerSet = handler_mod.HandlerSet;
const Cont = cont_mod.Cont;

const BenchEmit = types.Emit(u64);
const BenchPerform = types.Perform(u64, u64);

const allocator = std.heap.page_allocator;

fn clockNs() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(std.c.CLOCK.MONOTONIC_RAW, &ts);
    return @intCast(@as(i128, ts.sec) * 1_000_000_000 + ts.nsec);
}

pub fn main() void {
    const p = std.debug.print;

    p("\n{s:<40} {s:>12} {s:>14} {s:>10}\n", .{
        "Benchmark", "Iterations", "Total (ms)", "ns/op",
    });
    p("{s}\n", .{"-" ** 78});

    bench(p, "fiber create/destroy", 100_000, benchFiberCreateDestroy);
    bench(p, "yield/resume round-trips", 1_000_000, benchYieldResume);
    bench(p, "emit dispatch", 1_000_000, benchEmitDispatch);
    bench(p, "perform dispatch", 1_000_000, benchPerformDispatch);
    bench(p, "scheduler 10x1000 performs", 100, benchScheduler);

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

    const start = clockNs();
    for (0..iters) |_| {
        var sched = io_mod.Scheduler.init(allocator);

        var handlers = HandlerSet.init(allocator);
        handlers.onPerform(BenchPerform, &struct {
            fn handle(_: *BenchPerform.Value, cont: *Cont(BenchPerform), _: ?*anyopaque) void {
                cont.@"resume"(42);
            }
        }.handle, null);

        var fibers: [N]EffectFiber = undefined;
        for (0..N) |i| {
            fibers[i] = types.initFiberDefault(&struct {
                fn body(ctx: *EffectContext) void {
                    for (0..M) |j| {
                        const r = ctx.perform(BenchPerform, j);
                        std.mem.doNotOptimizeAway(r);
                    }
                }
            }.body) catch @panic("init");
            sched.spawn(&fibers[i], null, &handlers) catch @panic("spawn");
        }

        sched.run();

        for (0..N) |i| {
            fibers[i].deinit();
        }
        handlers.deinit();
        sched.deinit();
    }
    return clockNs() - start;
}
