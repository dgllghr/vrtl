// Algebraic effects with compile-time type safety.
//
// Declare effects as types:
//   const Write = vt.Perform(Request, WriteResult);
//   const Log = vt.Emit(LogEntry);
//
// The effect type encodes kind, value type, and resume type.
// Handler binding is type-checked at compile time.

const std = @import("std");

const types = @import("effect/types.zig");
const cont_mod = @import("effect/cont.zig");
const handler_mod = @import("effect/handler.zig");
const dispatch_mod = @import("effect/dispatch.zig");

// Re-export all public symbols

pub const Perform = types.Perform;
pub const Emit = types.Emit;
pub const EffectKind = types.EffectKind;
pub const effectId = types.effectId;
pub const RawEffect = types.RawEffect;
pub const EffectFiber = types.EffectFiber;
pub const EffectBodyFn = types.EffectBodyFn;
pub const initFiber = types.initFiber;
pub const initFiberDefault = types.initFiberDefault;
pub const EffectContext = types.EffectContext;

pub const Cont = cont_mod.Cont;
pub const SchedulerCont = cont_mod.SchedulerCont;

pub const PerformHandlerFn = handler_mod.PerformHandlerFn;
pub const EmitHandlerFn = handler_mod.EmitHandlerFn;
pub const EffectfulHandlerFn = handler_mod.EffectfulHandlerFn;
pub const HandlerSet = handler_mod.HandlerSet;

pub const run = dispatch_mod.run;

// Scheduling
const sched_mod = @import("scheduler.zig");
pub const Scheduler = sched_mod.Scheduler;
pub const FiberResult = sched_mod.FiberResult;

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
    const B = Perform(u16, u16);
    const C = Emit(u8);
    const D = Emit(u16);

    // Different type parameters → different effect types → different IDs.
    try testing.expect(effectId(A) != effectId(B));
    try testing.expect(effectId(A) != effectId(C));
    try testing.expect(effectId(B) != effectId(C));
    try testing.expect(effectId(C) != effectId(D));
    // Same type should produce the same ID.
    try testing.expectEqual(effectId(A), effectId(A));
    // Perform(u8, u8) is memoized — two bindings yield the same type.
    const A2 = Perform(u8, u8);
    try testing.expectEqual(effectId(A), effectId(A2));
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

// ============================================================
// §9. Delegation tests
// ============================================================

test "child delegates perform to parent" {
    const Fetch = Perform([]const u8, i32);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(Fetch, "key");
            std.debug.assert(val == 99);
        }
    }.body);
    defer fib.deinit();

    // Child: delegates everything
    var child = HandlerSet.init(testing.allocator);
    defer child.deinit();
    child.onPerform(Fetch, &struct {
        fn handle(_: *Fetch.Value, cont: *Cont(Fetch), _: ?*anyopaque) void {
            cont.delegate();
        }
    }.handle, null);

    // Parent: handles with 99
    var parent = HandlerSet.init(testing.allocator);
    defer parent.deinit();
    parent.onPerform(Fetch, &struct {
        fn handle(_: *Fetch.Value, cont: *Cont(Fetch), _: ?*anyopaque) void {
            cont.@"resume"(99);
        }
    }.handle, null);

    child.setParent(&parent);
    run(&fib, &child);

    try testing.expect(!fib.isAlive());
}

test "conditional delegation (cache pattern)" {
    const Lookup = Perform([]const u8, i32);
    const Result = Emit(i32);

    const State = struct { results: [2]i32 = .{ 0, 0 }, idx: usize = 0 };
    var state = State{};

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const a = ctx.perform(Lookup, "cached");
            ctx.emit(Result, a);
            const b = ctx.perform(Lookup, "miss");
            ctx.emit(Result, b);
        }
    }.body);
    defer fib.deinit();

    // Child: cache layer — handles "cached", delegates others
    var cache = HandlerSet.init(testing.allocator);
    defer cache.deinit();
    cache.onPerform(Lookup, &struct {
        fn handle(key: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            if (std.mem.eql(u8, key.*, "cached")) {
                cont.@"resume"(10); // cache hit
            } else {
                cont.delegate(); // cache miss → parent
            }
        }
    }.handle, null);

    cache.onEmit(Result, &struct {
        fn handle(val: *const Result.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.results[s.idx] = val.*;
            s.idx += 1;
        }
    }.handle, @ptrCast(&state));

    // Parent: DB layer — always returns 42
    var db = HandlerSet.init(testing.allocator);
    defer db.deinit();
    db.onPerform(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            cont.@"resume"(42); // DB result
        }
    }.handle, null);

    cache.setParent(&db);
    run(&fib, &cache);

    try testing.expectEqual(@as(i32, 10), state.results[0]); // cache hit
    try testing.expectEqual(@as(i32, 42), state.results[1]); // DB fallback
}

test "emit propagates to all layers" {
    const Ping = Emit(void);

    const State = struct { child_count: usize = 0, parent_count: usize = 0 };
    var state = State{};

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            ctx.emit(Ping, {});
            ctx.emit(Ping, {});
        }
    }.body);
    defer fib.deinit();

    var child = HandlerSet.init(testing.allocator);
    defer child.deinit();
    child.onEmit(Ping, &struct {
        fn handle(_: *const Ping.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.child_count += 1;
        }
    }.handle, @ptrCast(&state));

    var parent = HandlerSet.init(testing.allocator);
    defer parent.deinit();
    parent.onEmit(Ping, &struct {
        fn handle(_: *const Ping.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.parent_count += 1;
        }
    }.handle, @ptrCast(&state));

    child.setParent(&parent);
    run(&fib, &child);

    try testing.expectEqual(@as(usize, 2), state.child_count);
    try testing.expectEqual(@as(usize, 2), state.parent_count);
}

test "three-level delegation chain" {
    const Ask = Perform(u8, u8);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(Ask, 1);
            std.debug.assert(val == 77);
        }
    }.body);
    defer fib.deinit();

    // Level 1 (innermost): delegates
    var l1 = HandlerSet.init(testing.allocator);
    defer l1.deinit();
    l1.onPerform(Ask, &struct {
        fn handle(_: *Ask.Value, cont: *Cont(Ask), _: ?*anyopaque) void {
            cont.delegate();
        }
    }.handle, null);

    // Level 2 (middle): also delegates
    var l2 = HandlerSet.init(testing.allocator);
    defer l2.deinit();
    l2.onPerform(Ask, &struct {
        fn handle(_: *Ask.Value, cont: *Cont(Ask), _: ?*anyopaque) void {
            cont.delegate();
        }
    }.handle, null);

    // Level 3 (outermost): handles
    var l3 = HandlerSet.init(testing.allocator);
    defer l3.deinit();
    l3.onPerform(Ask, &struct {
        fn handle(_: *Ask.Value, cont: *Cont(Ask), _: ?*anyopaque) void {
            cont.@"resume"(77);
        }
    }.handle, null);

    l1.setParent(&l2);
    l2.setParent(&l3);
    run(&fib, &l1);

    try testing.expect(!fib.isAlive());
}

test "auto-drop preserved with delegation in chain" {
    const Oops = Perform(u8, u8);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            _ = ctx.perform(Oops, 1);
            @panic("unreachable after auto-drop");
        }
    }.body);
    defer fib.deinit();

    // Child: delegates
    var child = HandlerSet.init(testing.allocator);
    defer child.deinit();
    child.onPerform(Oops, &struct {
        fn handle(_: *Oops.Value, cont: *Cont(Oops), _: ?*anyopaque) void {
            cont.delegate();
        }
    }.handle, null);

    // Parent: forgets to resume/drop — should auto-drop
    var parent = HandlerSet.init(testing.allocator);
    defer parent.deinit();
    parent.onPerform(Oops, &struct {
        fn handle(_: *Oops.Value, _: *Cont(Oops), _: ?*anyopaque) void {
            // Intentionally do nothing — should auto-drop.
        }
    }.handle, null);

    child.setParent(&parent);
    run(&fib, &child);

    try testing.expect(!fib.isAlive());
}

// ============================================================
// §10. Scheduler tests
// ============================================================

test "scheduler: effectful handler re-performs effect" {
    const Lookup = Perform([]const u8, i32);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(Lookup, "key");
            std.debug.assert(val == 42);
        }
    }.body);
    defer fib.deinit();

    // Child: effectful handler that re-performs
    var child_hs = HandlerSet.init(testing.allocator);
    defer child_hs.deinit();
    child_hs.onPerformEffect(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *SchedulerCont(Lookup), ectx: *EffectContext, _: ?*anyopaque) void {
            const result = ectx.perform(Lookup, "key");
            cont.@"resume"(result);
        }
    }.handle, null);

    // Parent: simple handler returns 42
    var parent_hs = HandlerSet.init(testing.allocator);
    defer parent_hs.deinit();
    parent_hs.onPerform(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);

    child_hs.setParent(&parent_hs);
    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &child_hs);
    sched.run();

    try testing.expect(!fib.isAlive());
}

test "scheduler: pre/post work around re-perform" {
    const Lookup = Perform([]const u8, i32);
    const LogEvent = Emit([]const u8);

    const State = struct {
        log: [4][]const u8 = .{ "", "", "", "" },
        idx: usize = 0,
    };
    var state = State{};

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(Lookup, "key");
            std.debug.assert(val == 42);
        }
    }.body);
    defer fib.deinit();

    // Effectful handler: emits before and after re-perform
    var child_hs = HandlerSet.init(testing.allocator);
    defer child_hs.deinit();
    child_hs.onPerformEffect(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *SchedulerCont(Lookup), ectx: *EffectContext, _: ?*anyopaque) void {
            ectx.emit(LogEvent, "before");
            const result = ectx.perform(Lookup, "key");
            ectx.emit(LogEvent, "after");
            cont.@"resume"(result);
        }
    }.handle, null);

    // Parent: handles Lookup and observes LogEvent
    var parent_hs = HandlerSet.init(testing.allocator);
    defer parent_hs.deinit();
    parent_hs.onPerform(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);
    parent_hs.onEmit(LogEvent, &struct {
        fn handle(val: *const LogEvent.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.log[s.idx] = val.*;
            s.idx += 1;
        }
    }.handle, @ptrCast(&state));

    child_hs.setParent(&parent_hs);
    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &child_hs);
    sched.run();

    try testing.expectEqual(@as(usize, 2), state.idx);
    try testing.expectEqualStrings("before", state.log[0]);
    try testing.expectEqualStrings("after", state.log[1]);
}

test "scheduler: effectful handler transforms result" {
    const Lookup = Perform([]const u8, i32);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(Lookup, "key");
            std.debug.assert(val == 84); // parent's 42 doubled
        }
    }.body);
    defer fib.deinit();

    var child_hs = HandlerSet.init(testing.allocator);
    defer child_hs.deinit();
    child_hs.onPerformEffect(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *SchedulerCont(Lookup), ectx: *EffectContext, _: ?*anyopaque) void {
            const result = ectx.perform(Lookup, "key");
            cont.@"resume"(result * 2);
        }
    }.handle, null);

    var parent_hs = HandlerSet.init(testing.allocator);
    defer parent_hs.deinit();
    parent_hs.onPerform(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);

    child_hs.setParent(&parent_hs);
    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &child_hs);
    sched.run();

    try testing.expect(!fib.isAlive());
}

test "scheduler: effectful handler drops origin" {
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
    handlers.onPerformEffect(Abort, &struct {
        fn handle(_: *Abort.Value, cont: *SchedulerCont(Abort), _: *EffectContext, _: ?*anyopaque) void {
            cont.drop();
        }
    }.handle, null);

    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &handlers);
    sched.run();

    try testing.expect(!fib.isAlive());
}

test "scheduler: effectful handler delegates" {
    const Lookup = Perform([]const u8, i32);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(Lookup, "key");
            std.debug.assert(val == 99);
        }
    }.body);
    defer fib.deinit();

    var child_hs = HandlerSet.init(testing.allocator);
    defer child_hs.deinit();
    child_hs.onPerformEffect(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *SchedulerCont(Lookup), _: *EffectContext, _: ?*anyopaque) void {
            cont.delegate();
        }
    }.handle, null);

    var parent_hs = HandlerSet.init(testing.allocator);
    defer parent_hs.deinit();
    parent_hs.onPerform(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            cont.@"resume"(99);
        }
    }.handle, null);

    child_hs.setParent(&parent_hs);
    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &child_hs);
    sched.run();

    try testing.expect(!fib.isAlive());
}

test "scheduler: effectful handler auto-drop" {
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
    handlers.onPerformEffect(Oops, &struct {
        fn handle(_: *Oops.Value, _: *SchedulerCont(Oops), _: *EffectContext, _: ?*anyopaque) void {
            // Intentionally do nothing — should auto-drop
        }
    }.handle, null);

    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &handlers);
    sched.run();

    try testing.expect(!fib.isAlive());
}

test "scheduler: mixed simple and effectful handlers" {
    const Lookup = Perform([]const u8, i32);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const a = ctx.perform(Lookup, "cached");
            const b = ctx.perform(Lookup, "miss");
            std.debug.assert(a == 10);
            std.debug.assert(b == 84); // 42 * 2
        }
    }.body);
    defer fib.deinit();

    // Child: simple handler — cache hit for "cached", delegate for miss
    var child_hs = HandlerSet.init(testing.allocator);
    defer child_hs.deinit();
    child_hs.onPerform(Lookup, &struct {
        fn handle(key: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            if (std.mem.eql(u8, key.*, "cached")) {
                cont.@"resume"(10);
            } else {
                cont.delegate();
            }
        }
    }.handle, null);

    // Middle: effectful handler — intercepts, re-performs to parent, doubles result
    var middle_hs = HandlerSet.init(testing.allocator);
    defer middle_hs.deinit();
    middle_hs.onPerformEffect(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *SchedulerCont(Lookup), ectx: *EffectContext, _: ?*anyopaque) void {
            const result = ectx.perform(Lookup, "key");
            cont.@"resume"(result * 2);
        }
    }.handle, null);

    // Parent: simple handler — always returns 42
    var parent_hs = HandlerSet.init(testing.allocator);
    defer parent_hs.deinit();
    parent_hs.onPerform(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);

    child_hs.setParent(&middle_hs);
    middle_hs.setParent(&parent_hs);
    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &child_hs);
    sched.run();

    try testing.expect(!fib.isAlive());
}

test "scheduler: nested effectful handlers" {
    const Lookup = Perform([]const u8, i32);

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(Lookup, "key");
            std.debug.assert(val == 52); // inner adds 10 to outer's passthrough of 42
        }
    }.body);
    defer fib.deinit();

    // Inner effectful: re-performs, adds 10
    var inner_hs = HandlerSet.init(testing.allocator);
    defer inner_hs.deinit();
    inner_hs.onPerformEffect(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *SchedulerCont(Lookup), ectx: *EffectContext, _: ?*anyopaque) void {
            const result = ectx.perform(Lookup, "key");
            cont.@"resume"(result + 10);
        }
    }.handle, null);

    // Outer effectful: re-performs, passes through
    var outer_hs = HandlerSet.init(testing.allocator);
    defer outer_hs.deinit();
    outer_hs.onPerformEffect(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *SchedulerCont(Lookup), ectx: *EffectContext, _: ?*anyopaque) void {
            const result = ectx.perform(Lookup, "key");
            cont.@"resume"(result);
        }
    }.handle, null);

    // Base: simple handler returns 42
    var base_hs = HandlerSet.init(testing.allocator);
    defer base_hs.deinit();
    base_hs.onPerform(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);

    inner_hs.setParent(&outer_hs);
    outer_hs.setParent(&base_hs);
    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &inner_hs);
    sched.run();

    try testing.expect(!fib.isAlive());
}

test "scheduler: simple-only handlers match run() behavior" {
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

    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &handlers);
    sched.run();

    try testing.expectEqual(@as(i32, 84), state.sum);
}

test "scheduler: effectful handler emits to parent observers" {
    const Lookup = Perform([]const u8, i32);
    const Trace = Emit([]const u8);

    const State = struct {
        log: [4][]const u8 = .{ "", "", "", "" },
        idx: usize = 0,
    };
    var state = State{};

    var fib = try initFiberDefault(&struct {
        fn body(ctx: *EffectContext) void {
            const val = ctx.perform(Lookup, "key");
            std.debug.assert(val == 42);
        }
    }.body);
    defer fib.deinit();

    // Child: effectful handler that emits trace events
    var child_hs = HandlerSet.init(testing.allocator);
    defer child_hs.deinit();
    child_hs.onPerformEffect(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *SchedulerCont(Lookup), ectx: *EffectContext, _: ?*anyopaque) void {
            ectx.emit(Trace, "handling lookup");
            const result = ectx.perform(Lookup, "key");
            ectx.emit(Trace, "got result");
            cont.@"resume"(result);
        }
    }.handle, null);

    // Parent: handles Lookup and observes Trace
    var parent_hs = HandlerSet.init(testing.allocator);
    defer parent_hs.deinit();
    parent_hs.onPerform(Lookup, &struct {
        fn handle(_: *Lookup.Value, cont: *Cont(Lookup), _: ?*anyopaque) void {
            cont.@"resume"(42);
        }
    }.handle, null);
    parent_hs.onEmit(Trace, &struct {
        fn handle(val: *const Trace.Value, raw_ctx: ?*anyopaque) void {
            const s: *State = @ptrCast(@alignCast(raw_ctx.?));
            s.log[s.idx] = val.*;
            s.idx += 1;
        }
    }.handle, @ptrCast(&state));

    child_hs.setParent(&parent_hs);
    var sched = Scheduler.init(testing.allocator);
    defer sched.deinit();
    try sched.spawn(&fib, null, &child_hs);
    sched.run();

    try testing.expectEqual(@as(usize, 2), state.idx);
    try testing.expectEqualStrings("handling lookup", state.log[0]);
    try testing.expectEqualStrings("got result", state.log[1]);
}
