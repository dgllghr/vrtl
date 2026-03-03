// Low-level Zig bindings to minicoro. Thin wrapper — exposes the
// minicoro API with Zig types. Higher-level Fiber and Effect APIs
// build on top of this.

const std = @import("std");

// ============================================================
// §1. C bindings
// ============================================================

pub const c = @cImport({
    @cInclude("minicoro.h");
});

pub const Coro = c.mco_coro;
pub const Desc = c.mco_desc;
pub const State = c.mco_state;
pub const Result = c.mco_result;

// ============================================================
// §2. Zig wrappers
// ============================================================

pub const CoroError = error{
    GenericError,
    InvalidPointer,
    InvalidCoroutine,
    NotSuspended,
    NotRunning,
    MakeContextError,
    SwitchContextError,
    NotEnoughSpace,
    OutOfMemory,
    InvalidArguments,
    InvalidOperation,
    StackOverflow,
};

fn mapResult(res: Result) CoroError!void {
    return switch (res) {
        c.MCO_SUCCESS => {},
        c.MCO_GENERIC_ERROR => CoroError.GenericError,
        c.MCO_INVALID_POINTER => CoroError.InvalidPointer,
        c.MCO_INVALID_COROUTINE => CoroError.InvalidCoroutine,
        c.MCO_NOT_SUSPENDED => CoroError.NotSuspended,
        c.MCO_NOT_RUNNING => CoroError.NotRunning,
        c.MCO_MAKE_CONTEXT_ERROR => CoroError.MakeContextError,
        c.MCO_SWITCH_CONTEXT_ERROR => CoroError.SwitchContextError,
        c.MCO_NOT_ENOUGH_SPACE => CoroError.NotEnoughSpace,
        c.MCO_OUT_OF_MEMORY => CoroError.OutOfMemory,
        c.MCO_INVALID_ARGUMENTS => CoroError.InvalidArguments,
        c.MCO_INVALID_OPERATION => CoroError.InvalidOperation,
        c.MCO_STACK_OVERFLOW => CoroError.StackOverflow,
        else => CoroError.GenericError,
    };
}

/// Initialize a coroutine descriptor with a callback function.
pub fn descInit(func: *const fn (?*Coro) callconv(.c) void, stack_size: usize) Desc {
    const desc = c.mco_desc_init(func, stack_size);
    return desc;
}

/// Create a coroutine from a descriptor.
pub fn create(desc: *Desc) CoroError!*Coro {
    var co: ?*Coro = null;
    try mapResult(c.mco_create(&co, desc));
    return co orelse CoroError.InvalidPointer;
}

/// Resume a suspended coroutine.
pub fn @"resume"(co: *Coro) CoroError!void {
    return mapResult(c.mco_resume(co));
}

/// Yield the currently running coroutine.
pub fn yield(co: *Coro) CoroError!void {
    return mapResult(c.mco_yield(co));
}

/// Destroy a coroutine.
pub fn destroy(co: *Coro) CoroError!void {
    return mapResult(c.mco_destroy(co));
}

/// Get the state of a coroutine.
pub fn status(co: *Coro) State {
    return c.mco_status(co);
}

/// Get the currently running coroutine (thread-local).
pub fn running() ?*Coro {
    return c.mco_running();
}

/// Push data into a coroutine's storage (for passing between yield/resume).
pub fn push(co: *Coro, data: []const u8) CoroError!void {
    return mapResult(c.mco_push(co, data.ptr, data.len));
}

/// Pop data from a coroutine's storage.
pub fn pop(co: *Coro, buf: []u8) CoroError!void {
    return mapResult(c.mco_pop(co, buf.ptr, buf.len));
}

/// Get/set the user_data pointer.
pub fn getUserData(co: *Coro) ?*anyopaque {
    return c.mco_get_user_data(co);
}

pub fn setUserData(desc: *Desc, data: ?*anyopaque) void {
    desc.user_data = data;
}
