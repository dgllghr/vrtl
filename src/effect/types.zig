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

pub const EffectKind = enum(u8) { perform, emit };

pub fn effectId(comptime E: type) usize {
    return @intFromPtr(&struct { const _: type = E; }._);
}

// ============================================================
// §2. RawEffect — type-erased, yielded through the fiber
// ============================================================

pub const RawEffect = struct {
    id: usize,
    kind: EffectKind,
    value_ptr: *anyopaque,
    value_size: usize,
    /// For perform: pointer to the resume slot on the performer's
    /// stack frame. The continuation writes the resume value here.
    resume_ptr: ?*anyopaque = null,
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

// ============================================================
// §4. EffectContext — used inside effectful fiber bodies
// ============================================================

pub const EffectContext = struct {
    handle: *EffectFiber.Handle,

    pub fn init(handle: *EffectFiber.Handle) EffectContext {
        return .{ .handle = handle };
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
            .value_size = @sizeOf(E.Value),
            .resume_ptr = if (@sizeOf(E.Resume) > 0) @ptrCast(&result) else null,
        });
        return result;
    }

    /// Emit: yield to observers, then resume. Synchronous from
    /// the fiber's perspective — returns once all observers run.
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
};
