const std = @import("std");
const fiber_mod = @import("../fiber.zig");

// ============================================================
// §1. Effect type declarations
// ============================================================

pub fn Perform(comptime T: type, comptime R: type) type {
    return struct {
        pub const Value = T;
        pub const Resume = R;
        pub const kind: EffectKind = .perform;
    };
}

pub fn Emit(comptime T: type) type {
    return struct {
        pub const Value = T;
        pub const kind: EffectKind = .emit;
    };
}

pub const EffectKind = enum(u8) { perform, emit, @"suspend", park };

pub fn effectId(comptime E: type) usize {
    return @intFromPtr(@typeName(E).ptr);
}

// ============================================================
// §2. RawEffect — type-erased, yielded through the fiber
// ============================================================

pub const RawEffect = struct {
    id: usize = 0,
    kind: EffectKind,
    value_ptr: *anyopaque = undefined,
    /// For perform: pointer to the resume slot on the performer's
    /// stack frame. The continuation writes the resume value here.
    resume_ptr: ?*anyopaque = null,
    /// For emit: size of the value (needed for heap-copying in async dispatch).
    value_size: usize = 0,
};

pub const EffectFiber = fiber_mod.Fiber(RawEffect, void);
pub const EffectBodyFn = *const fn (*EffectContext) void;

/// Create an effect fiber from a body that receives an EffectContext
/// directly. Equivalent to EffectFiber.init with a wrapper that
/// constructs the context.
pub fn initFiber(body: EffectBodyFn, stack_size: usize) !EffectFiber {
    const Static = struct {
        threadlocal var current_body: EffectBodyFn = undefined;
    };
    Static.current_body = body;

    return EffectFiber.init(&struct {
        fn wrapper(h: *EffectFiber.Handle) void {
            const b = Static.current_body;
            var ctx = EffectContext.init(h);
            b(&ctx);
        }
    }.wrapper, stack_size);
}

/// Create an effect fiber with the default stack size.
pub fn initFiberDefault(body: EffectBodyFn) !EffectFiber {
    return initFiber(body, 0);
}

/// Create an effect fiber using a StackPool for allocation.
pub fn initFiberPooled(pool: *@import("../pool.zig").StackPool, body: EffectBodyFn) !EffectFiber {
    const Static = struct {
        threadlocal var current_body: EffectBodyFn = undefined;
    };
    Static.current_body = body;

    return EffectFiber.initPooled(&struct {
        fn wrapper(h: *EffectFiber.Handle) void {
            const b = Static.current_body;
            var ctx = EffectContext.init(h);
            b(&ctx);
        }
    }.wrapper, 0, pool);
}

// ============================================================
// §3. WakeHandle — park/wake primitive for fibers
// ============================================================

pub const WakeHandle = struct {
    _data: [3]usize = .{ 0, 0, 0 },
    _wake_fn: ?*const fn (*WakeHandle) void = null,
    state: u32 = INIT,

    pub const INIT: u32 = 0;
    pub const PARKED: u32 = 1;
    pub const WOKEN: u32 = 2;

    /// Re-enqueue the parked fiber. Safe to call from any fiber or thread.
    /// No-op if already woken. Handles early wake (called before park is processed).
    pub fn wake(self: *WakeHandle) void {
        const prev = @atomicRmw(u32, &self.state, .Xchg, WOKEN, .acq_rel);
        if (prev == PARKED) {
            self._wake_fn.?(self);
        }
    }
};

// ============================================================
// §4. EffectContext — used inside effectful fiber bodies
// ============================================================

pub const EffectContext = struct {
    handle: *EffectFiber.Handle,
    io: std.Io = .{ .userdata = null, .vtable = undefined },

    pub fn init(handle: *EffectFiber.Handle) EffectContext {
        return .{ .handle = handle };
    }

    pub fn initWithIo(handle: *EffectFiber.Handle, io_instance: std.Io) EffectContext {
        return .{ .handle = handle, .io = io_instance };
    }

    /// Perform: transfer ownership, suspend, receive a resume value.
    pub fn perform(self: *EffectContext, comptime E: type, val: E.Value) E.Resume {
        comptime std.debug.assert(E.kind == .perform);
        var storage: E.Value = val;
        var result: E.Resume = undefined;
        _ = self.handle.yield(.{
            .id = effectId(E),
            .kind = .perform,
            .value_ptr = @ptrCast(&storage),
            .resume_ptr = if (@sizeOf(E.Resume) > 0) @ptrCast(&result) else null,
        });
        return result;
    }

    /// Emit: yield to the scheduler which dispatches observers asynchronously.
    /// The emitting fiber resumes immediately; observers run in separate fibers.
    pub fn emit(self: *EffectContext, comptime E: type, val: E.Value) void {
        comptime std.debug.assert(E.kind == .emit);
        var storage: E.Value = val;
        _ = self.handle.yield(.{
            .id = effectId(E),
            .kind = .emit,
            .value_ptr = @ptrCast(&storage),
            .value_size = @sizeOf(E.Value),
        });
    }

    /// Park: suspend the fiber until wh.wake() is called.
    /// The WakeHandle must live on the fiber's stack (frozen while parked).
    pub fn park(self: *EffectContext, wh: *WakeHandle) void {
        _ = self.handle.yield(.{ .kind = .park, .value_ptr = @ptrCast(wh) });
    }
};
