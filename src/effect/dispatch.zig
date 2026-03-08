const std = @import("std");
const types = @import("types.zig");
const handler_mod = @import("handler.zig");

const RawEffect = types.RawEffect;
const EffectFiber = types.EffectFiber;
const EffectContext = types.EffectContext;
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
/// every level. Each observer runs in a temp fiber inline (for run() mode).
pub fn dispatchEmit(eff: *const RawEffect, handlers: *const HandlerSet) void {
    var level: ?*const HandlerSet = handlers;
    while (level) |hs| : (level = hs.parent) {
        for (hs.emit_bindings.items) |binding| {
            if (binding.id == eff.id) {
                handler_mod.emit_fiber_ctx_tls = .{
                    .value_ptr = eff.value_ptr,
                    .user_ctx = binding.ctx,
                };
                var hfib: EffectFiber = undefined;
                types.initFiberDefault(&hfib, binding.fiber_body) catch @panic("OOM: emit handler fiber");
                defer hfib.deinit();

                // Start fiber — it copies value to its stack and yields suspend
                var heff = hfib.start();
                // Skip the initial suspend (value copy done)
                if (heff) |h| {
                    if (h.kind == .@"suspend") {
                        heff = hfib.resumeVoid();
                    }
                }
                // Process handler's effects inline
                while (heff) |h| {
                    switch (h.kind) {
                        .emit => {
                            if (hs.parent) |p| {
                                dispatchEmit(&h, p);
                            }
                            heff = hfib.resumeVoid();
                        },
                        .perform => {
                            heff = dispatchPerform(&h, &hfib, hs.parent orelse hs);
                        },
                        .@"suspend" => unreachable,
                    }
                }
            }
        }
    }
}

pub fn run(fib: *EffectFiber, handlers: *const HandlerSet) void {
    var maybe_eff = fib.start();
    while (maybe_eff) |eff| {
        switch (eff.kind) {
            .emit => {
                dispatchEmit(&eff, handlers);
                maybe_eff = fib.resumeVoid();
            },
            .perform => {
                maybe_eff = dispatchPerform(&eff, fib, handlers);
            },
            .@"suspend" => unreachable, // suspend is only valid inside Scheduler
        }
    }
}
