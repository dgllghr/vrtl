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

pub fn PerformSyncHandlerFn(comptime E: type) type {
    return *const fn (value: *E.Value, cont: *Cont(E), ctx: ?*anyopaque) void;
}

pub fn EmitHandlerFn(comptime E: type) type {
    return *const fn (value: *const E.Value, ctx: *EffectContext, user_ctx: ?*anyopaque) void;
}

pub fn PerformHandlerFn(comptime E: type) type {
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

const PerformBinding = struct {
    id: usize,
    handler: ErasedPerformFn,
    ctx: ?*anyopaque,
};

pub const EmitFiberCtx = struct {
    value_ptr: *anyopaque,
    user_ctx: ?*anyopaque,
};

pub threadlocal var emit_fiber_ctx_tls: EmitFiberCtx = undefined;

const EmitBinding = struct {
    id: usize,
    fiber_body: EffectBodyFn,
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

    /// Bind a typed synchronous perform handler. Compile error if handler
    /// signature doesn't match E's value and resume types.
    pub fn onPerformSync(self: *HandlerSet, comptime E: type, comptime handler: PerformSyncHandlerFn(E), ctx: ?*anyopaque) void {
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

    /// Bind a typed emit observer. The handler runs in its own fiber
    /// with an EffectContext, so it can suspend, re-emit, and re-perform.
    pub fn onEmit(self: *HandlerSet, comptime E: type, comptime handler: EmitHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn fiber_body(ectx: *EffectContext) void {
                const ectx_info = emit_fiber_ctx_tls;
                var value_copy: E.Value = undefined;
                if (@sizeOf(E.Value) > 0) {
                    const src: *const E.Value = @ptrCast(@alignCast(ectx_info.value_ptr));
                    value_copy = src.*;
                }
                // Signal: value copied, emitter can resume
                _ = ectx.handle.yield(.{ .kind = .@"suspend" });
                handler(&value_copy, ectx, ectx_info.user_ctx);
            }
        };
        self.emit_bindings.append(self.allocator, .{
            .id = effectId(E),
            .fiber_body = Gen.fiber_body,
            .ctx = ctx,
        }) catch @panic("OOM");
    }

    /// Bind an effectful perform handler. The handler runs in its own
    /// persistent fiber and receives an EffectContext to re-perform
    /// effects that propagate to the parent scope. The fiber loops
    /// across invocations — dispatch is a single context switch
    /// (resumeVoid) after the first call.
    pub fn onPerform(self: *HandlerSet, comptime E: type, comptime handler: PerformHandlerFn(E), ctx: ?*anyopaque) void {
        const Gen = struct {
            fn fiber_body(ectx: *EffectContext) void {
                while (true) {
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
                    // Yield sentinel — scheduler returns fiber to persistent cache
                    _ = ectx.handle.yield(.{ .kind = .@"suspend", .id = types.SUSPEND_HANDLER_DONE });
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
