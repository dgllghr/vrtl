const std = @import("std");
const types = @import("types.zig");
const handler_mod = @import("handler.zig");

const RawEffect = types.RawEffect;
const EffectFiber = types.EffectFiber;
const initFiberDefault = types.initFiberDefault;
const HandlerSet = handler_mod.HandlerSet;
const HandlerFiberCtx = handler_mod.HandlerFiberCtx;

// ============================================================
// §7. Runner
// ============================================================

/// Walk the handler chain (child → parent) looking for a perform handler.
/// Returns the next effect from the handler that accepted, or resumes with
/// a zeroed value if the chain is exhausted.
pub fn dispatchPerform(eff: *const RawEffect, fiber: *EffectFiber, handlers: *const HandlerSet) ?RawEffect {
    var level: ?*const HandlerSet = handlers;
    while (level) |hs| : (level = hs.parent) {
        for (hs.bindings.items) |binding| {
            if (binding.kind == .perform and binding.id == eff.id) {
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
    var level: ?*const HandlerSet = handlers;
    while (level) |hs| : (level = hs.parent) {
        for (hs.bindings.items) |binding| {
            if (binding.kind == .emit and binding.id == eff.id) {
                _ = binding.handler(eff, fiber, binding.ctx);
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
            .io_wait => unreachable, // io_wait is only valid inside IoScheduler
        }
    }
}

// ============================================================
// §7b. Scheduler — handlers as fibers
// ============================================================

pub const Scheduler = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Scheduler {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Scheduler) void {
        _ = self;
    }

    pub fn run(self: *Scheduler, fib: *EffectFiber, handlers: *const HandlerSet) void {
        var maybe_eff = fib.start();
        while (maybe_eff) |eff| {
            switch (eff.kind) {
                .emit => {
                    dispatchEmit(&eff, fib, handlers);
                    maybe_eff = fib.resumeVoid();
                },
                .perform => {
                    maybe_eff = self.dispatchPerformScheduled(&eff, fib, handlers);
                },
                .io_wait => unreachable, // io_wait is only valid inside IoScheduler
            }
        }
    }

    /// Walk the handler chain (child → parent). At each level, try simple
    /// bindings first, then effectful bindings. Effectful handlers run in
    /// their own fibers; their effects are dispatched to the parent scope.
    fn dispatchPerformScheduled(
        self: *Scheduler,
        eff: *const RawEffect,
        origin_fiber: *EffectFiber,
        handlers: ?*const HandlerSet,
    ) ?RawEffect {
        var level: ?*const HandlerSet = handlers;
        while (level) |hs| : (level = hs.parent) {
            // Simple bindings (same logic as dispatchPerform)
            for (hs.bindings.items) |binding| {
                if (binding.kind == .perform and binding.id == eff.id) {
                    switch (binding.handler(eff, origin_fiber, binding.ctx)) {
                        .handled => |next| return next,
                        .skipped => {},
                    }
                }
            }

            // Effectful bindings
            for (hs.effectful_bindings.items) |binding| {
                if (binding.id == eff.id) {
                    var hctx = HandlerFiberCtx{
                        .raw = eff,
                        .user_ctx = binding.ctx,
                    };
                    handler_mod.handler_fiber_ctx_tls = &hctx;

                    var hfib = initFiberDefault(binding.fiber_body) catch @panic("OOM");
                    defer hfib.deinit();

                    // Run handler fiber — dispatch ITS effects to parent scope
                    var heff = hfib.start();
                    while (heff) |h| {
                        switch (h.kind) {
                            .emit => {
                                if (hs.parent) |p| {
                                    dispatchEmit(&h, &hfib, p);
                                }
                                heff = hfib.resumeVoid();
                            },
                            .perform => {
                                heff = self.dispatchPerformScheduled(&h, &hfib, hs.parent);
                            },
                            .io_wait => unreachable,
                        }
                    }

                    // Handler fiber completed — check outcome
                    if (hctx.delegated) continue;
                    if (hctx.resumed) return origin_fiber.resumeVoid();
                    if (hctx.dropped) {
                        origin_fiber.deinit();
                        return null;
                    }
                    // auto-drop (shouldn't happen — wrapper sets dropped)
                    origin_fiber.deinit();
                    return null;
                }
            }
        }

        // Chain exhausted — no handler accepted. Resume with zeroed value.
        return origin_fiber.resumeVoid();
    }
};
