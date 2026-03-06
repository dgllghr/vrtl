const std = @import("std");
const types = @import("types.zig");
const handler_mod = @import("handler.zig");

const RawEffect = types.RawEffect;
const EffectFiber = types.EffectFiber;
const HandlerSet = handler_mod.HandlerSet;

// ============================================================
// §7. Runner
// ============================================================

/// Walk the handler chain (child → parent) looking for a perform handler.
/// Returns the next effect from the handler that accepted, or resumes with
/// a zeroed value if the chain is exhausted.
pub fn dispatchPerform(eff: *const RawEffect, fiber: *EffectFiber, handlers: *const HandlerSet) ?RawEffect {
    var level: ?*const HandlerSet = handlers;
    while (level) |hs| : (level = hs.parent) {
        for (hs.perform_bindings.items) |binding| {
            if (binding.id == eff.id) {
                switch (binding.handler(eff, fiber, binding.ctx)) {
                    .handled => |next| return next,
                    .skipped => {},
                }
            }
        }
    }
    // Chain exhausted — no handler accepted. Resume with zeroed value.
    return fiber.resumeVoid();
}

/// Walk the entire handler chain, calling ALL matching emit observers at
/// every level. Child-level observers run first, then parent, then grandparent.
pub fn dispatchEmit(eff: *const RawEffect, fiber: *EffectFiber, handlers: *const HandlerSet) void {
    _ = fiber;
    var level: ?*const HandlerSet = handlers;
    while (level) |hs| : (level = hs.parent) {
        for (hs.emit_bindings.items) |binding| {
            if (binding.id == eff.id) {
                binding.handler(eff, binding.ctx);
            }
        }
    }
}

pub fn run(fib: *EffectFiber, handlers: *const HandlerSet) void {
    var maybe_eff = fib.start();
    while (maybe_eff) |eff| {
        switch (eff.kind) {
            .emit => {
                dispatchEmit(&eff, fib, handlers);
                maybe_eff = fib.resumeVoid();
            },
            .perform => {
                maybe_eff = dispatchPerform(&eff, fib, handlers);
            },
            .io_wait => unreachable, // io_wait is only valid inside Scheduler
            .park => unreachable, // park is only valid inside Scheduler
        }
    }
}
