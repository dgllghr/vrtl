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

pub const EffectKind = enum(u8) { perform, emit, @"suspend" };

/// Sentinel: when RawEffect.id == SUSPEND_WAKE, the suspend is a WakeHandle
/// park (not an IO suspend). The scheduler checks this to distinguish the two.
pub const SUSPEND_WAKE: usize = 1;

/// Sentinel: persistent handler fiber completed one invocation and is
/// ready to be returned to the persistent cache.
pub const SUSPEND_HANDLER_DONE: usize = 2;

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
};

pub const EffectFiber = fiber_mod.Fiber(RawEffect, void);
pub const EffectBodyFn = *const fn (*EffectContext) void;

/// Initialize an effect fiber in-place from a body that receives an
/// EffectContext directly.
pub fn initFiber(fiber: *EffectFiber, body: EffectBodyFn, stack_size: usize) !void {
    const Static = struct {
        threadlocal var current_body: EffectBodyFn = undefined;
    };
    Static.current_body = body;

    try fiber.init(&struct {
        fn wrapper(h: *EffectFiber.Handle) void {
            const b = Static.current_body;
            var ctx = EffectContext.init(h);
            b(&ctx);
        }
    }.wrapper, stack_size);
}

/// Initialize an effect fiber in-place with the default stack size.
pub fn initFiberDefault(fiber: *EffectFiber, body: EffectBodyFn) !void {
    try initFiber(fiber, body, 0);
}

/// Threadlocal for passing the effect body to the pooled fiber wrapper.
/// Module-level so both initFiberPooled and resetFiberPooled can share it.
threadlocal var pooled_fiber_body: EffectBodyFn = undefined;

fn pooledFiberWrapper(h: *EffectFiber.Handle) void {
    const b = pooled_fiber_body;
    var ctx = EffectContext.init(h);
    b(&ctx);
}

/// Initialize an effect fiber in-place using a StackPool for allocation.
pub fn initFiberPooled(fiber: *EffectFiber, pool: *@import("../pool.zig").StackPool, body: EffectBodyFn, stack_size: usize) !void {
    pooled_fiber_body = body;
    try fiber.initPooled(&pooledFiberWrapper, stack_size, pool);
}

/// Initialize an effect fiber in-place using a pre-allocated stack block.
pub fn initFiberFromBlock(fiber: *EffectFiber, pool: *@import("../pool.zig").StackPool, body: EffectBodyFn, block: [*]u8, block_size: usize, stack_size: usize) void {
    pooled_fiber_body = body;
    fiber.initFromBlock(&pooledFiberWrapper, block, block_size, stack_size, pool);
}

/// Reset a pooled effect fiber for reuse with a new body. The fiber's
/// stack must still be allocated (not destroyed). After reset, the fiber
/// can be started again with start().
pub fn resetFiberPooled(fiber: *EffectFiber, body: EffectBodyFn) void {
    pooled_fiber_body = body;
    fiber.reset(&pooledFiberWrapper);
}

// ============================================================
// §3. WakeHandle — suspend/wake primitive for fibers
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
        });
    }

    /// Suspend the fiber until wh.wake() is called.
    /// The WakeHandle must live on the fiber's stack (frozen while suspended).
    pub fn @"suspend"(self: *EffectContext, wh: *WakeHandle) void {
        _ = self.handle.yield(.{ .kind = .@"suspend", .id = SUSPEND_WAKE, .value_ptr = @ptrCast(wh) });
    }
};
