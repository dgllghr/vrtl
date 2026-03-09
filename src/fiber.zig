// Typed fibers built on the pure Zig coroutine runtime.
//
// A Fiber(Y, R) wraps a coroutine with typed yield/resume:
//   - The fiber yields values of type Y
//   - The fiber receives values of type R when resumed
//
// Non-zero-size yield/resume values are passed via threadlocal
// pointers — no memcpy overhead.

const std = @import("std");
const coro = @import("coro.zig");

// ============================================================
// §1. Fiber
// ============================================================

pub fn Fiber(comptime YieldT: type, comptime ResumeT: type) type {
    return struct {
        co: coro.Coro = undefined,
        state: FiberState = .ready,

        const Self = @This();

        pub const Yield = YieldT;
        pub const Resume = ResumeT;

        pub const FiberState = enum {
            ready,
            running,
            suspended,
            dead,
        };

        /// The function type that fiber bodies must match.
        /// Receives a handle to yield values and receive resume values.
        pub const BodyFn = *const fn (*Handle) void;

        // Threadlocal slots for passing yield/resume values as pointers.
        // The source value lives on the yielding/resuming stack frame,
        // which is frozen during the context switch, so the pointer is
        // valid until the next resume/yield.
        const Slots = struct {
            threadlocal var yield_ptr: *const YieldT = undefined;
            threadlocal var resume_ptr: *const ResumeT = undefined;
        };

        /// Handle passed to the fiber body. Provides yield().
        pub const Handle = struct {
            co: *coro.Coro,

            /// Yield a value and suspend. Returns the value passed to resume().
            pub fn yield(self: *Handle, value: YieldT) ResumeT {
                if (comptime @sizeOf(YieldT) > 0) {
                    Slots.yield_ptr = &value;
                }
                coro.yield(self.co) catch @panic("yield failed");
                if (comptime @sizeOf(ResumeT) > 0) {
                    return Slots.resume_ptr.*;
                }
                return undefined;
            }

            /// Yield without a value (for Fiber(void, R)).
            pub fn yieldVoid(self: *Handle) ResumeT {
                coro.yield(self.co) catch @panic("yield failed");
                if (comptime @sizeOf(ResumeT) > 0) {
                    return Slots.resume_ptr.*;
                }
                return undefined;
            }
        };

        const Wrapper = struct {
            fn entry(co_ptr: *coro.Coro) void {
                const body_ptr: *const BodyFn = @ptrCast(@alignCast(
                    coro.getUserData(co_ptr) orelse @panic("no user_data")));
                var handle = Handle{ .co = co_ptr };
                body_ptr.*(&handle);
            }
        };

        // Threadlocal slot so the body fn pointer outlives the
        // init stack frame. Works for comptime-known function
        // pointers; runtime closures would need a heap allocation.
        const Static = struct {
            threadlocal var current_body: BodyFn = undefined;
        };

        /// Initialize a fiber in-place that will run `body`.
        pub fn init(self: *Self, body: BodyFn, stack_size: usize) !void {
            var desc = coro.descInit(Wrapper.entry, stack_size);
            Static.current_body = body;
            coro.setUserData(&desc, @ptrCast(&Static.current_body));
            try coro.create(&self.co, &desc);
            self.state = .ready;
        }

        /// Initialize a fiber in-place using `pool` for stack allocation.
        pub fn initPooled(self: *Self, body: BodyFn, stack_size: usize, pool: *@import("pool.zig").StackPool) !void {
            var desc = coro.descInit(Wrapper.entry, stack_size);
            Static.current_body = body;
            coro.setUserData(&desc, @ptrCast(&Static.current_body));
            pool.patchDesc(&desc);
            try coro.create(&self.co, &desc);
            self.state = .ready;
        }

        /// Initialize a fiber in-place using a pre-allocated stack block.
        /// The caller is responsible for guard page setup.
        pub fn initFromBlock(self: *Self, body: BodyFn, block: [*]u8, block_size: usize, stack_size: usize, pool: *@import("pool.zig").StackPool) void {
            Static.current_body = body;
            var desc = coro.descInit(Wrapper.entry, 0);
            coro.setUserData(&desc, @ptrCast(&Static.current_body));
            pool.patchDesc(&desc);
            coro.createFromBlock(&self.co, &desc, block, block_size, stack_size);
            self.state = .ready;
        }

        /// Initialize with default stack size (64KB).
        pub fn initDefault(self: *Self, body: BodyFn) !void {
            try self.init(body, 0);
        }

        /// Reset the fiber for reuse with a new body. The underlying
        /// coroutine's stack must still be allocated (not destroyed).
        /// After reset, the fiber can be started again with start().
        pub fn reset(self: *Self, body: BodyFn) void {
            Static.current_body = body;
            coro.resetContext(&self.co);
            self.state = .ready;
        }

        /// Destroy the fiber and free its stack. Idempotent —
        /// safe to call more than once (coro.destroy checks block_size == 0).
        pub fn deinit(self: *Self) void {
            self.state = .dead;
            coro.destroy(&self.co) catch {};
        }

        /// Start the fiber for the first time (no resume value).
        /// Returns the first yielded value, or null if the fiber completed.
        pub fn start(self: *Self) ?YieldT {
            self.state = .running;
            coro.@"resume"(&self.co) catch {
                self.state = .dead;
                return null;
            };
            return self.afterResume();
        }

        /// Start with an initial value (for Fiber(Y, R) where R != void).
        pub fn startWith(self: *Self, value: ResumeT) ?YieldT {
            if (comptime @sizeOf(ResumeT) > 0) {
                Slots.resume_ptr = &value;
            }
            self.state = .running;
            coro.@"resume"(&self.co) catch {
                self.state = .dead;
                return null;
            };
            return self.afterResume();
        }

        /// Resume a suspended fiber with a value.
        /// Returns the next yielded value, or null if the fiber completed.
        pub fn @"resume"(self: *Self, value: ResumeT) ?YieldT {
            if (self.state != .suspended) return null;
            if (comptime @sizeOf(ResumeT) > 0) {
                Slots.resume_ptr = &value;
            }
            self.state = .running;
            coro.@"resume"(&self.co) catch {
                self.state = .dead;
                return null;
            };
            return self.afterResume();
        }

        /// Resume without a value (for Fiber(Y, void)).
        pub fn resumeVoid(self: *Self) ?YieldT {
            if (self.state != .suspended) return null;
            self.state = .running;
            coro.@"resume"(&self.co) catch {
                self.state = .dead;
                return null;
            };
            return self.afterResume();
        }

        /// Scheduler-optimized resume. No state checks, no TLS bookkeeping.
        /// Works for both initial start and subsequent resumes. The caller
        /// must ensure the fiber is in ready or suspended state.
        pub fn schedulerResumeVoid(self: *Self) ?YieldT {
            self.state = .running;
            coro.schedulerResume(&self.co);
            return self.afterResume();
        }

        pub fn isAlive(self: *const Self) bool {
            return self.state != .dead;
        }

        pub fn isSuspended(self: *const Self) bool {
            return self.state == .suspended;
        }

        // -- Internal --

        fn afterResume(self: *Self) ?YieldT {
            const s = coro.status(&self.co);
            if (s == .suspended) {
                self.state = .suspended;
                if (comptime @sizeOf(YieldT) > 0) {
                    return Slots.yield_ptr.*;
                }
                return @as(YieldT, undefined);
            } else {
                self.state = .dead;
                return null;
            }
        }
    };
}

// ============================================================
// §2. Convenience aliases
// ============================================================

/// A fiber that yields values of type T and doesn't receive resume values.
pub fn Generator(comptime T: type) type {
    return Fiber(T, void);
}

/// A fiber that yields void and receives values of type T (a sink/consumer).
pub fn Consumer(comptime T: type) type {
    return Fiber(void, T);
}

/// A fiber that transforms: receives T, yields U.
pub fn Transformer(comptime In: type, comptime Out: type) type {
    return Fiber(Out, In);
}

// ============================================================
// §4. Tests
// ============================================================

test "generator yields sequence then dies" {
    const Gen = Generator(i32);
    var g: Gen = undefined;
    try g.initDefault(&struct {
        fn body(h: *Gen.Handle) void {
            _ = h.yield(10);
            _ = h.yield(20);
            _ = h.yield(30);
        }
    }.body);
    defer g.deinit();

    try std.testing.expectEqual(10, g.start().?);
    try std.testing.expectEqual(20, g.resumeVoid().?);
    try std.testing.expectEqual(30, g.resumeVoid().?);
    try std.testing.expectEqual(null, g.resumeVoid());
    try std.testing.expect(!g.isAlive());
}

test "fiber that immediately returns is dead after start" {
    const Gen = Generator(i32);
    var g: Gen = undefined;
    try g.initDefault(&struct {
        fn body(_: *Gen.Handle) void {}
    }.body);
    defer g.deinit();

    try std.testing.expectEqual(null, g.start());
    try std.testing.expect(!g.isAlive());
}

test "bidirectional fiber passes values both ways" {
    const F = Fiber(i32, i32);
    var f: F = undefined;
    try f.initDefault(&struct {
        fn body(h: *F.Handle) void {
            const a = h.yield(1); // yield 1, receive first resume value
            const b = h.yield(a * 10); // echo it back scaled
            _ = h.yield(b * 10);
        }
    }.body);
    defer f.deinit();

    try std.testing.expectEqual(1, f.start().?);
    try std.testing.expectEqual(50, f.@"resume"(5).?); // sends 5, gets 5*10
    try std.testing.expectEqual(70, f.@"resume"(7).?); // sends 7, gets 7*10
    try std.testing.expectEqual(null, f.@"resume"(0));
}

test "consumer receives values via yieldVoid" {
    const C = Consumer(i32);
    const State = struct {
        threadlocal var sum: i32 = 0;
    };
    State.sum = 0;

    var c: C = undefined;
    try c.initDefault(&struct {
        fn body(h: *C.Handle) void {
            State.sum += h.yieldVoid(); // suspend, wait for value
            State.sum += h.yieldVoid();
            State.sum += h.yieldVoid();
        }
    }.body);
    defer c.deinit();

    _ = c.start();
    _ = c.@"resume"(10);
    _ = c.@"resume"(20);
    _ = c.@"resume"(30);
    try std.testing.expectEqual(@as(i32, 60), State.sum);
}

test "resuming a non-suspended fiber returns null" {
    const Gen = Generator(i32);
    var g: Gen = undefined;
    try g.initDefault(&struct {
        fn body(h: *Gen.Handle) void {
            _ = h.yield(1);
        }
    }.body);
    defer g.deinit();

    // Before start, resume should be a no-op.
    try std.testing.expectEqual(null, g.resumeVoid());
    // Start and exhaust.
    _ = g.start();
    _ = g.resumeVoid();
    // After dead, resume should be a no-op.
    try std.testing.expectEqual(null, g.resumeVoid());
}

test "state transitions" {
    const Gen = Generator(u8);
    var g: Gen = undefined;
    try g.initDefault(&struct {
        fn body(h: *Gen.Handle) void {
            _ = h.yield(1);
        }
    }.body);
    defer g.deinit();

    try std.testing.expectEqual(Gen.FiberState.ready, g.state);
    _ = g.start();
    try std.testing.expectEqual(Gen.FiberState.suspended, g.state);
    try std.testing.expect(g.isSuspended());
    _ = g.resumeVoid();
    try std.testing.expectEqual(Gen.FiberState.dead, g.state);
}
