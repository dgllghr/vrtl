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
        cont: *Cont(E),
        ctx: *EffectContext,
        user_ctx: ?*anyopaque,
    ) void;
}

// ============================================================
// §6. HandlerSet — type-safe binding, type-erased storage
// ============================================================

pub const PerformResult = union(enum) {
    handled: ?RawEffect,
    skipped,
};

const ErasedPerformFn = *const fn (raw: *const RawEffect, fiber: *EffectFiber, ctx: ?*anyopaque) PerformResult;
const ErasedEmitFn = *const fn (raw: *const RawEffect, ctx: ?*anyopaque) void;

const PerformBinding = struct {
    id: usize,
    handler: ErasedPerformFn,
    ctx: ?*anyopaque,
};

const EmitBinding = struct {
    id: usize,
    handler: ErasedEmitFn,
    ctx: ?*anyopaque,
};

pub const HandlerFiberCtx = struct {
    raw: *const RawEffect,
    user_ctx: ?*anyopaque,
    // Outputs — written by Cont, read by scheduler
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
    perform_bindings: std.ArrayListUnmanaged(PerformBinding) = .{},
    emit_bindings: std.ArrayListUnmanaged(EmitBinding) = .{},
    effectful_bindings: std.ArrayListUnmanaged(EffectfulBinding) = .{},
    allocator: std.mem.Allocator,
    parent: ?*const HandlerSet = null,

    pub fn init(allocator: std.mem.Allocator) HandlerSet {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *HandlerSet) void {
        self.perform_bindings.deinit(self.allocator);
        self.emit_bindings.deinit(self.allocator);
        self.effectful_bindings.deinit(self.allocator);
    }

    pub fn setParent(self: *HandlerSet, p: *const HandlerSet) void {
        self.parent = p;
    }

    /// Bind a typed perform handler. Compile error if handler
    /// signature doesn't match E's value and resume types.
    pub fn onPerform(self: *HandlerSet, comptime E: type, comptime handler: PerformHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn erased(raw: *const RawEffect, fiber: *EffectFiber, user_ctx: ?*anyopaque) PerformResult {
                const val: *E.Value = @ptrCast(@alignCast(raw.value_ptr));
                var cont = Cont(E){
                    .resume_ptr = raw.resume_ptr,
                };
                handler(val, &cont, user_ctx);
                if (cont.delegated) return .skipped;
                if (cont.resumed) return .{ .handled = fiber.resumeVoid() };
                // dropped or auto-drop
                fiber.deinit();
                return .{ .handled = null };
            }
        };
        self.perform_bindings.append(self.allocator, .{
            .id = effectId(E),
            .handler = Gen.erased,
            .ctx = ctx,
        }) catch @panic("OOM");
    }

    /// Bind a typed emit observer. Compile error if handler
    /// signature doesn't match E's value type.
    pub fn onEmit(self: *HandlerSet, comptime E: type, comptime handler: EmitHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn erased(raw: *const RawEffect, user_ctx: ?*anyopaque) void {
                const val: *const E.Value = @ptrCast(@alignCast(raw.value_ptr));
                handler(val, user_ctx);
            }
        };
        self.emit_bindings.append(self.allocator, .{
            .id = effectId(E),
            .handler = Gen.erased,
            .ctx = ctx,
        }) catch @panic("OOM");
    }

    /// Bind an effectful perform handler. The handler runs in its own
    /// fiber and receives an EffectContext to re-perform effects that
    /// propagate to the parent scope.
    pub fn onPerformEffect(self: *HandlerSet, comptime E: type, comptime handler: EffectfulHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn fiber_body(ectx: *EffectContext) void {
                const hctx = handler_fiber_ctx_tls;
                const val: *E.Value = @ptrCast(@alignCast(hctx.raw.value_ptr));
                var cont = Cont(E){
                    .resume_ptr = hctx.raw.resume_ptr,
                };
                handler(val, &cont, ectx, hctx.user_ctx);
                // Write outcome flags to shared context
                hctx.resumed = cont.resumed;
                hctx.dropped = cont.dropped;
                hctx.delegated = cont.delegated;
                if (cont.alive and !cont.delegated) {
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
