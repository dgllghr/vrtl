// Typed fibers built on minicoro.
//
// A Fiber(Y, R) wraps a minicoro coroutine with typed yield/resume:
//   - The fiber yields values of type Y
//   - The fiber receives values of type R when resumed
//
// Data passes through minicoro's storage (mco_push/mco_pop),
// which lives on the coroutine's stack — no heap allocation
// for the transfer itself.

const std = @import("std");
const coro = @import("coro.zig");

// ============================================================
// §1. Fiber
// ============================================================

pub fn Fiber(comptime YieldT: type, comptime ResumeT: type) type {
    return struct {
        co: *coro.Coro,
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

        /// Handle passed to the fiber body. Provides yield().
        pub const Handle = struct {
            co: *coro.Coro,

            /// Yield a value and suspend. Returns the value passed to resume().
            pub fn yield(self: *Handle, value: YieldT) ResumeT {
                pushTyped(self.co, YieldT, value);
                coro.yield(self.co) catch @panic("yield failed");
                return popTyped(self.co, ResumeT);
            }

            /// Yield without a value (for Fiber(void, R)).
            pub fn yieldVoid(self: *Handle) ResumeT {
                coro.yield(self.co) catch @panic("yield failed");
                return popTyped(self.co, ResumeT);
            }
        };

        /// Create a fiber that will run `body`.
        pub fn init(body: BodyFn, stack_size: usize) !Self {
            const Wrapper = struct {
                fn entry(co: ?*coro.Coro) callconv(.C) void {
                    const c = co orelse @panic("null coro");
                    const body_ptr: *const BodyFn = @ptrCast(@alignCast(
                        coro.getUserData(c) orelse @panic("no user_data")));
                    var handle = Handle{ .co = c };
                    body_ptr.*(&handle);
                }
            };

            var desc = coro.descInit(Wrapper.entry, stack_size);

            // Store the body fn pointer in a threadlocal so it outlives
            // this stack frame. This works for comptime-known function
            // pointers; runtime closures would need a heap allocation.
            const Static = struct {
                threadlocal var current_body: BodyFn = undefined;
            };
            Static.current_body = body;
            coro.setUserData(&desc, @ptrCast(&Static.current_body));

            const co = try coro.create(&desc);
            return Self{ .co = co };
        }

        /// Create with default stack size (64KB).
        pub fn initDefault(body: BodyFn) !Self {
            return init(body, 0); // 0 = use minicoro default
        }

        /// Destroy the fiber and free its stack.
        pub fn deinit(self: *Self) void {
            coro.destroy(self.co) catch {};
        }

        /// Start the fiber for the first time (no resume value).
        /// Returns the first yielded value, or null if the fiber completed.
        pub fn start(self: *Self) ?YieldT {
            self.state = .running;
            coro.@"resume"(self.co) catch {
                self.state = .dead;
                return null;
            };
            return self.afterResume();
        }

        /// Start with an initial value (for Fiber(Y, R) where R != void).
        pub fn startWith(self: *Self, value: ResumeT) ?YieldT {
            pushTyped(self.co, ResumeT, value);
            self.state = .running;
            coro.@"resume"(self.co) catch {
                self.state = .dead;
                return null;
            };
            return self.afterResume();
        }

        /// Resume a suspended fiber with a value.
        /// Returns the next yielded value, or null if the fiber completed.
        pub fn @"resume"(self: *Self, value: ResumeT) ?YieldT {
            if (self.state != .suspended) return null;
            pushTyped(self.co, ResumeT, value);
            self.state = .running;
            coro.@"resume"(self.co) catch {
                self.state = .dead;
                return null;
            };
            return self.afterResume();
        }

        /// Resume without a value (for Fiber(Y, void)).
        pub fn resumeVoid(self: *Self) ?YieldT {
            if (self.state != .suspended) return null;
            self.state = .running;
            coro.@"resume"(self.co) catch {
                self.state = .dead;
                return null;
            };
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
            const s = coro.status(self.co);
            if (s == coro.c.MCO_SUSPENDED) {
                self.state = .suspended;
                return popTyped(self.co, YieldT);
            } else {
                self.state = .dead;
                return null;
            }
        }
    };
}

// ============================================================
// §2. Typed push/pop via minicoro storage
// ============================================================

fn pushTyped(co: *coro.Coro, comptime T: type, value: T) void {
    if (@sizeOf(T) == 0) return; // void, zero-size types
    const bytes = std.mem.asBytes(&value);
    coro.push(co, bytes) catch @panic("push failed: not enough space");
}

fn popTyped(co: *coro.Coro, comptime T: type) T {
    if (@sizeOf(T) == 0) return undefined;
    var value: T = undefined;
    const buf = std.mem.asBytes(&value);
    coro.pop(co, buf) catch @panic("pop failed: not enough data");
    return value;
}

// ============================================================
// §3. Convenience aliases
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
    var g = try Gen.initDefault(&struct {
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
    var g = try Gen.initDefault(&struct {
        fn body(_: *Gen.Handle) void {}
    }.body);
    defer g.deinit();

    try std.testing.expectEqual(null, g.start());
    try std.testing.expect(!g.isAlive());
}

test "bidirectional fiber passes values both ways" {
    const F = Fiber(i32, i32);
    var f = try F.initDefault(&struct {
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
    var total: i32 = 0;
    var c = try C.initDefault(&struct {
        fn body(h: *C.Handle) void {
            var sum: i32 = 0;
            sum += h.yieldVoid(); // suspend, wait for value
            sum += h.yieldVoid();
            sum += h.yieldVoid();
            // Store result where the test can read it. We use the
            // coroutine's own storage for this via a final yield.
            const co = h.co;
            coro.push(co, std.mem.asBytes(&sum)) catch @panic("push");
        }
    }.body);
    defer c.deinit();

    _ = c.start();
    _ = c.@"resume"(10);
    _ = c.@"resume"(20);
    _ = c.@"resume"(30);
    // Fiber is dead now; read the sum it pushed before exiting.
    var buf: [@sizeOf(i32)]u8 = undefined;
    coro.pop(c.co, &buf) catch @panic("pop");
    total = std.mem.bytesToValue(i32, &buf);
    try std.testing.expectEqual(@as(i32, 60), total);
}

test "resuming a non-suspended fiber returns null" {
    const Gen = Generator(i32);
    var g = try Gen.initDefault(&struct {
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
    var g = try Gen.initDefault(&struct {
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
