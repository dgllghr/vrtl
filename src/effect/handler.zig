const std = @import("std");
const types = @import("types.zig");
const cont_mod = @import("cont.zig");

const EffectKind = types.EffectKind;
const RawEffect = types.RawEffect;
const EffectFiber = types.EffectFiber;
const EffectBodyFn = types.EffectBodyFn;
const EffectContext = types.EffectContext;
const effectId = types.effectId;

const Cont = cont_mod.Cont;
const SchedulerCont = cont_mod.SchedulerCont;

// ============================================================
// §5. Handler types
// ============================================================

pub fn PerformHandlerFn(comptime E: type) type {
    return *const fn (value: *E.Value, cont: *Cont(E), ctx: ?*anyopaque) void;
}

pub fn EmitHandlerFn(comptime E: type) type {
    return *const fn (value: *const E.Value, ctx: ?*anyopaque) void;
}

pub fn EffectfulHandlerFn(comptime E: type) type {
    return *const fn (
        value: *E.Value,
        cont: *SchedulerCont(E),
        ctx: *EffectContext,
        user_ctx: ?*anyopaque,
    ) void;
}

// ============================================================
// §6. HandlerSet — type-safe binding, type-erased storage
// ============================================================

const HandlerResult = union(enum) {
    handled: ?RawEffect,
    skipped,
};

const ErasedHandlerFn = *const fn (raw: *const RawEffect, fiber: *EffectFiber, ctx: ?*anyopaque) HandlerResult;

const Binding = struct {
    id: usize,
    kind: EffectKind,
    handler: ErasedHandlerFn,
    ctx: ?*anyopaque,
};

pub const HandlerFiberCtx = struct {
    raw: *const RawEffect,
    user_ctx: ?*anyopaque,
    // Outputs — written by SchedulerCont, read by scheduler
    resumed: bool = false,
    dropped: bool = false,
    delegated: bool = false,
};

pub threadlocal var handler_fiber_ctx_tls: *HandlerFiberCtx = undefined;

const EffectfulBinding = struct {
    id: usize,
    fiber_body: EffectBodyFn,
    ctx: ?*anyopaque,
};

pub const HandlerSet = struct {
    bindings: std.ArrayListUnmanaged(Binding),
    effectful_bindings: std.ArrayListUnmanaged(EffectfulBinding),
    allocator: std.mem.Allocator,
    parent: ?*const HandlerSet = null,

    pub fn init(allocator: std.mem.Allocator) HandlerSet {
        return .{ .bindings = .{}, .effectful_bindings = .{}, .allocator = allocator };
    }

    pub fn deinit(self: *HandlerSet) void {
        self.bindings.deinit(self.allocator);
        self.effectful_bindings.deinit(self.allocator);
    }

    pub fn setParent(self: *HandlerSet, p: *const HandlerSet) void {
        self.parent = p;
    }

    /// Bind a typed perform handler. Compile error if handler
    /// signature doesn't match E's value and resume types.
    pub fn onPerform(self: *HandlerSet, comptime E: type, handler: PerformHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn erased(raw: *const RawEffect, fiber: *EffectFiber, user_ctx: ?*anyopaque) HandlerResult {
                const val: *E.Value = @ptrCast(@alignCast(raw.value_ptr));
                var cont = Cont(E){
                    .fiber = fiber,
                    .resume_ptr = raw.resume_ptr,
                };
                handler(val, &cont, user_ctx);
                if (cont.delegated) return .skipped;
                if (cont.alive) {
                    // Handler returned without resuming, dropping, or delegating.
                    // Drop the fiber to prevent leaks.
                    cont.drop();
                }
                return .{ .handled = cont.next_effect };
            }
        };
        self.bindings.append(self.allocator, .{
            .id = effectId(E),
            .kind = .perform,
            .handler = Gen.erased,
            .ctx = ctx,
        }) catch @panic("OOM");
    }

    /// Bind a typed emit observer. Compile error if handler
    /// signature doesn't match E's value type.
    pub fn onEmit(self: *HandlerSet, comptime E: type, handler: EmitHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn erased(raw: *const RawEffect, _: *EffectFiber, user_ctx: ?*anyopaque) HandlerResult {
                const val: *const E.Value = @ptrCast(@alignCast(raw.value_ptr));
                handler(val, user_ctx);
                return .{ .handled = null };
            }
        };
        self.bindings.append(self.allocator, .{
            .id = effectId(E),
            .kind = .emit,
            .handler = Gen.erased,
            .ctx = ctx,
        }) catch @panic("OOM");
    }

    /// Bind an effectful perform handler. The handler runs in its own
    /// fiber and receives an EffectContext to re-perform effects that
    /// propagate to the parent scope.
    pub fn onPerformEffect(self: *HandlerSet, comptime E: type, handler: EffectfulHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn fiber_body(ectx: *EffectContext) void {
                const hctx = handler_fiber_ctx_tls;
                const val: *E.Value = @ptrCast(@alignCast(hctx.raw.value_ptr));
                var sched_cont = SchedulerCont(E){
                    .resume_ptr = hctx.raw.resume_ptr,
                };
                handler(val, &sched_cont, ectx, hctx.user_ctx);
                // Write outcome flags to shared context
                hctx.resumed = sched_cont.resumed;
                hctx.dropped = sched_cont.dropped;
                hctx.delegated = sched_cont.delegated;
                if (sched_cont.alive and !sched_cont.delegated) {
                    hctx.dropped = true; // auto-drop
                }
            }
        };
        self.effectful_bindings.append(self.allocator, .{
            .id = effectId(E),
            .fiber_body = Gen.fiber_body,
            .ctx = ctx,
        }) catch @panic("OOM");
    }
};
