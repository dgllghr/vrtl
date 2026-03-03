const std = @import("std");
const types = @import("types.zig");

const RawEffect = types.RawEffect;
const EffectFiber = types.EffectFiber;

// ============================================================
// §3. Continuation — typed, given to perform handlers
// ============================================================

pub fn Cont(comptime E: type) type {
    const R = E.Resume;

    return struct {
        fiber: *EffectFiber,
        resume_ptr: ?*anyopaque,
        alive: bool = true,
        delegated: bool = false,
        next_effect: ?RawEffect = null,

        const Self = @This();

        // The continuation is only valid for the duration of the handler
        // call. It is stack-allocated inside the erased wrapper and
        // cannot be stored, returned, or accessed after the handler
        // returns. If the handler returns without calling resume(),
        // drop(), or delegate(), the wrapper auto-drops (destroying the
        // fiber).
        //
        // Handlers may perform blocking or async IO (e.g. via std.Io)
        // before resuming. The fiber's stack is untouched during this
        // time — std.Io operates on the handler's own stack. When the
        // handler's IO completes and it calls resume(), the fiber
        // continues normally.

        /// Resume the performer, delivering a value.
        pub fn @"resume"(self: *Self, val: R) void {
            if (!self.alive) return;
            self.alive = false;
            if (@sizeOf(R) > 0) {
                if (self.resume_ptr) |ptr| {
                    const slot: *R = @ptrCast(@alignCast(ptr));
                    slot.* = val;
                }
            }
            self.next_effect = self.fiber.resumeVoid();
        }

        /// Drop the continuation without resuming. The fiber is
        /// destroyed and its stack freed. The performer never returns
        /// from perform().
        ///
        /// Warning: defers and destructors in the fiber will NOT run.
        /// Resources held across a perform boundary will leak. Use
        /// arena allocators for memory, and avoid holding file handles
        /// or locks across perform calls.
        pub fn drop(self: *Self) void {
            if (!self.alive) return;
            self.alive = false;
            self.fiber.deinit();
        }

        /// Delegate the effect to the parent handler scope. The
        /// handler is saying "I can't handle this, pass it up."
        /// The dispatch loop will continue searching parent layers.
        pub fn delegate(self: *Self) void {
            std.debug.assert(self.alive); // can't delegate after resume/drop
            self.delegated = true;
        }

        pub fn isAlive(self: *const Self) bool {
            return self.alive;
        }
    };
}

// ============================================================
// §3b. SchedulerCont — deferred continuation for effectful handlers
// ============================================================

pub fn SchedulerCont(comptime E: type) type {
    const R = E.Resume;

    return struct {
        resume_ptr: ?*anyopaque,
        alive: bool = true,
        delegated: bool = false,
        resumed: bool = false,
        dropped: bool = false,

        const Self = @This();

        /// Write the resume value to the origin fiber's stack slot.
        /// The scheduler resumes the origin fiber after this handler
        /// fiber completes.
        pub fn @"resume"(self: *Self, val: R) void {
            if (!self.alive) return;
            self.alive = false;
            self.resumed = true;
            if (@sizeOf(R) > 0) {
                if (self.resume_ptr) |ptr| {
                    const slot: *R = @ptrCast(@alignCast(ptr));
                    slot.* = val;
                }
            }
        }

        /// Drop the continuation without resuming. The scheduler
        /// will destroy the origin fiber.
        pub fn drop(self: *Self) void {
            if (!self.alive) return;
            self.alive = false;
            self.dropped = true;
        }

        /// Signal that this handler can't handle the effect.
        /// The scheduler continues searching parent scopes.
        /// Sets alive = false (unlike Cont.delegate()) to prevent
        /// contradictory resume/drop after delegate.
        pub fn delegate(self: *Self) void {
            std.debug.assert(self.alive);
            self.alive = false;
            self.delegated = true;
        }
    };
}
