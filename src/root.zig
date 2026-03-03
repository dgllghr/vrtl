//! vrtl — algebraic effects for Zig.
//!
//! Declare effects as types:
//!   const Write = vt.Perform(Request, WriteResult);
//!   const Log = vt.Emit(LogEntry);
//!
//! Bind handlers and run:
//!   var handlers = vt.HandlerSet.init(allocator);
//!   handlers.onPerform(Write, &writeHandler, null);
//!   handlers.onEmit(Log, &logHandler, null);
//!   vt.run(&fiber, &handlers);

const effect = @import("effect.zig");

// Effect declaration
pub const Perform = effect.Perform;
pub const Emit = effect.Emit;

// Context for effectful fiber bodies
pub const EffectContext = effect.EffectContext;

// Fiber creation
pub const initFiber = effect.initFiber;
pub const initFiberDefault = effect.initFiberDefault;

// Handler binding
pub const HandlerSet = effect.HandlerSet;

// Running
pub const run = effect.run;
pub const Scheduler = effect.Scheduler;

// Continuations (used in handler signatures)
pub const Cont = effect.Cont;
pub const SchedulerCont = effect.SchedulerCont;

// Handler function types (useful for explicit type annotations)
pub const PerformHandlerFn = effect.PerformHandlerFn;
pub const EmitHandlerFn = effect.EmitHandlerFn;
pub const EffectfulHandlerFn = effect.EffectfulHandlerFn;

// Low-level / advanced
pub const EffectFiber = effect.EffectFiber;
pub const EffectBodyFn = effect.EffectBodyFn;
pub const RawEffect = effect.RawEffect;
pub const EffectKind = effect.EffectKind;
pub const effectId = effect.effectId;

// Sub-modules (for direct access if needed)
pub const fiber = @import("fiber.zig");

test {
    _ = @import("effect.zig");
    _ = @import("fiber.zig");
}

