//! vrtl — algebraic effects for Zig.
//!
//! Declare effects as types:
//!   const Write = vt.Perform(Request, WriteResult);
//!   const Log = vt.Emit(LogEntry);
//!
//! Bind handlers and run:
//!   var handlers = vt.HandlerSet.init(allocator);
//!   handlers.onPerform(Write, &writeHandler, null);
//!   handlers.onEmit(Log, &logHandler, null);
//!   vt.run(&fiber, &handlers);

const effect = @import("effect.zig");

// Effect declaration
pub const Perform = effect.Perform;
pub const Emit = effect.Emit;

// Context for effectful fiber bodies
pub const EffectContext = effect.EffectContext;

// Fiber creation
pub const initFiber = effect.initFiber;
pub const initFiberDefault = effect.initFiberDefault;

// Handler binding
pub const HandlerSet = effect.HandlerSet;

// Running
pub const run = effect.run;
pub const Scheduler = effect.Scheduler;

// IO scheduling
pub const FiberResult = effect.FiberResult;

// Continuations (used in handler signatures)
pub const Cont = effect.Cont;
pub const SchedulerCont = effect.SchedulerCont;

// Handler function types (useful for explicit type annotations)
pub const PerformHandlerFn = effect.PerformHandlerFn;
pub const EmitHandlerFn = effect.EmitHandlerFn;
pub const EffectfulHandlerFn = effect.EffectfulHandlerFn;

// Low-level / advanced
pub const EffectFiber = effect.EffectFiber;
pub const EffectBodyFn = effect.EffectBodyFn;
pub const RawEffect = effect.RawEffect;
pub const EffectKind = effect.EffectKind;
pub const effectId = effect.effectId;

// Sub-modules (for direct access if needed)
pub const fiber = @import("fiber.zig");

test {
    _ = @import("deque.zig");
    _ = @import("effect.zig");
    _ = @import("fiber.zig");
    _ = @import("scheduler.zig");
    _ = @import("pool.zig");
}

// ============================================================
// Integration: nested fibers with multi-file IO
// ============================================================

test "Scheduler: nested handler chain with multi-file IO across fibers" {
    const std = @import("std");
    const testing = std.testing;

    // -- Effects --
    // ReadFile: fiber requests file contents by name (algebraic perform).
    // FileWritten: fiber announces it finished writing (emit).
    const ReadFile = Perform([]const u8, []const u8);
    const FileWritten = Emit([]const u8);

    // -- Shared test state (module-level statics for comptime callbacks) --
    const S = struct {
        // Log of every event, in order, across all fibers.
        var log: [32][]const u8 = .{""} ** 32;
        var log_len: usize = 0;
        // Number of IO awaits that resolved.
        var await_count: usize = 0;

        fn record(msg: []const u8) void {
            log[log_len] = msg;
            log_len += 1;
        }

        fn reset() void {
            log = .{""} ** 32;
            log_len = 0;
            await_count = 0;
        }
    };
    S.reset();

    // -- Mock IO backend --
    // The mock await callback bumps a counter (simulating completed IO).
    const sched_mod = @import("scheduler.zig");
    var mock_vt = sched_mod.makeNoopVtable();
    mock_vt.await = &struct {
        fn mock_await(_: ?*anyopaque, _: *std.Io.AnyFuture, _: []u8, _: std.mem.Alignment) void {
            S.await_count += 1;
        }
    }.mock_await;
    const mock_io = std.Io{ .userdata = null, .vtable = &mock_vt };

    // -- Scheduler (must exist before creating IO fibers) --
    var sched = try Scheduler.init(testing.allocator, 1);
    defer sched.deinit();

    // -- Fibers --
    // Fiber A: reads "config.json", writes it, reads "schema.json", writes it.
    var res_a = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            const cfg = ctx.perform(ReadFile, "config.json");
            _ = cfg; // contents received from handler
            S.record("A:write(config.json)");
            var dummy: sched_mod.DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(FileWritten, "config.json");

            const schema = ctx.perform(ReadFile, "schema.json");
            _ = schema;
            S.record("A:write(schema.json)");
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(FileWritten, "schema.json");
        }
    }.body, mock_io, 0);
    defer res_a.fiber.deinit();

    // Fiber B: reads "data.csv", writes it, reads "report.txt", writes it.
    var res_b = try sched.createFiber(&struct {
        fn body(ctx: *EffectContext) void {
            const data = ctx.perform(ReadFile, "data.csv");
            _ = data;
            S.record("B:write(data.csv)");
            var dummy: sched_mod.DummyFuture = .{};
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(FileWritten, "data.csv");

            const report = ctx.perform(ReadFile, "report.txt");
            _ = report;
            S.record("B:write(report.txt)");
            ctx.io.vtable.await(ctx.io.userdata, @ptrCast(&dummy), &.{}, .@"1");
            ctx.emit(FileWritten, "report.txt");
        }
    }.body, mock_io, 0);
    defer res_b.fiber.deinit();

    // -- Nested handler chain --
    //
    //   inner (child):  observes FileWritten -> records to log
    //                   delegates ReadFile to parent
    //   outer (parent): handles ReadFile -> returns mock contents

    var inner = HandlerSet.init(testing.allocator);
    defer inner.deinit();

    // Inner: observe FileWritten
    inner.onEmit(FileWritten, &struct {
        fn handle(val: *const FileWritten.Value, _: ?*anyopaque) void {
            S.record(val.*);
        }
    }.handle, null);

    // Inner: delegate ReadFile
    inner.onPerform(ReadFile, &struct {
        fn handle(_: *ReadFile.Value, cont: *Cont(ReadFile), _: ?*anyopaque) void {
            cont.delegate();
        }
    }.handle, null);

    var outer = HandlerSet.init(testing.allocator);
    defer outer.deinit();

    // Outer: handle ReadFile with mock content
    outer.onPerform(ReadFile, &struct {
        fn handle(filename: *ReadFile.Value, cont: *Cont(ReadFile), _: ?*anyopaque) void {
            // Return the filename itself as "contents" (proves delegation worked).
            cont.@"resume"(filename.*);
        }
    }.handle, null);

    // Outer: also observe FileWritten (bubbles up from inner)
    outer.onEmit(FileWritten, &struct {
        fn handle(val: *const FileWritten.Value, _: ?*anyopaque) void {
            // Prefix to distinguish from inner observer
            _ = val;
            S.record("parent-saw-write");
        }
    }.handle, null);

    inner.setParent(&outer);

    // -- Run --
    try sched.spawn(&res_a.fiber, res_a.handle, &inner);
    try sched.spawn(&res_b.fiber, res_b.handle, &inner);
    sched.run();

    // -- Assertions --
    // Both fibers completed.
    try testing.expect(!res_a.fiber.isAlive());
    try testing.expect(!res_b.fiber.isAlive());

    // 4 IO awaits total (2 per fiber).
    try testing.expectEqual(@as(usize, 4), S.await_count);

    // Collect the log into something we can search.
    const log = S.log[0..S.log_len];

    // Verify key events appeared (order may vary due to round-robin,
    // but within each fiber the sequence is deterministic).
    var a_writes: usize = 0;
    var b_writes: usize = 0;
    var file_written_events: usize = 0;
    var parent_observations: usize = 0;
    for (log) |entry| {
        if (std.mem.startsWith(u8, entry, "A:write")) a_writes += 1;
        if (std.mem.startsWith(u8, entry, "B:write")) b_writes += 1;
        if (std.mem.eql(u8, entry, "config.json") or
            std.mem.eql(u8, entry, "schema.json") or
            std.mem.eql(u8, entry, "data.csv") or
            std.mem.eql(u8, entry, "report.txt")) file_written_events += 1;
        if (std.mem.eql(u8, entry, "parent-saw-write")) parent_observations += 1;
    }

    // Each fiber wrote 2 files.
    try testing.expectEqual(@as(usize, 2), a_writes);
    try testing.expectEqual(@as(usize, 2), b_writes);

    // 4 FileWritten emits, observed by inner handler.
    try testing.expectEqual(@as(usize, 4), file_written_events);

    // 4 FileWritten emits also bubbled to parent handler.
    try testing.expectEqual(@as(usize, 4), parent_observations);

    // Verify per-fiber ordering: A's first write came before A's second.
    var a_first: ?usize = null;
    var a_second: ?usize = null;
    var b_first: ?usize = null;
    var b_second: ?usize = null;
    for (log, 0..) |entry, i| {
        if (std.mem.eql(u8, entry, "A:write(config.json)")) a_first = i;
        if (std.mem.eql(u8, entry, "A:write(schema.json)")) a_second = i;
        if (std.mem.eql(u8, entry, "B:write(data.csv)")) b_first = i;
        if (std.mem.eql(u8, entry, "B:write(report.txt)")) b_second = i;
    }
    try testing.expect(a_first.? < a_second.?);
    try testing.expect(b_first.? < b_second.?);
}

