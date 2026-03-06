// Pure Zig coroutine runtime — replaces minicoro.
//
// Stack layout (per coroutine):
//
//   Low address                                High address
//   +───────────+──────────────────────────────+
//   │ Guard(16K)│         Usable Stack         │
//   +───────────+──────────────────────────────+
//   ^           ^                              ^
//   block_base  stack_base                SP starts here
//
// Guard page triggers SIGBUS on overflow. Stack grows downward.
// Coro struct is owned by the caller (embedded in Fiber/FiberEntry).
//
// Context switch follows the pattern from Zig's standard library
// (std.Io.fiber): minimal context (sp, fp, pc) with full clobber list.
// The compiler saves/restores all callee-saved registers around the
// inline asm; we only switch the stack, frame pointer, and return address.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

// ============================================================
// §1. Public types
// ============================================================

pub const State = enum { suspended, running, normal, dead };

pub const CoroError = error{
    NotSuspended,
    NotRunning,
    OutOfMemory,
    InvalidCoroutine,
    StackOverflow,
};

pub const EntryFn = *const fn (*Coro) void;

pub const AllocFn = *const fn (usize, ?*anyopaque) ?[*]u8;
pub const DeallocFn = *const fn ([*]u8, usize, ?*anyopaque) void;

pub const Desc = struct {
    func: EntryFn,
    stack_size: usize,
    user_data: ?*anyopaque = null,
    alloc_fn: ?AllocFn = null,
    dealloc_fn: ?DeallocFn = null,
    allocator_data: ?*anyopaque = null,
    /// Skip mprotect guard page setup (for recycled stacks that already have one).
    skip_guard_page: bool = false,
};

pub const Coro = struct {
    ctx: Context = std.mem.zeroes(Context),
    caller_ctx: Context = std.mem.zeroes(Context),
    state: State = .suspended,
    func: EntryFn,
    user_data: ?*anyopaque = null,
    prev_co: ?*Coro = null,

    // Stack bookkeeping
    stack_base: [*]u8, // bottom of usable stack (above guard)
    stack_size: usize, // usable stack bytes
    block_base: [*]u8, // start of entire mmap'd block
    block_size: usize, // entire block size

    // Deallocator (null = default page_allocator)
    dealloc_fn: ?DeallocFn = null,
    dealloc_data: ?*anyopaque = null,
};

// ============================================================
// §2. Context — minimal, matches std.Io.fiber.Context
// ============================================================

const Context = switch (builtin.cpu.arch) {
    .aarch64 => extern struct {
        sp: u64 = 0,
        fp: u64 = 0,
        pc: u64 = 0,
        lr: u64 = 0, // x30 — saved explicitly (LLVM ignores x30 clobbers)
    },
    else => @compileError("unsupported architecture"),
};

// Struct for passing both context pointers through a single register.
const SwitchArgs = extern struct {
    from: usize, // *Context (save current state here)
    to: usize, // *const Context (load target state from here)
};

// ============================================================
// §3. Context switch (inline fn, full clobbers)
// ============================================================

/// Switch from one context to another. Saves sp/fp/pc to `from`,
/// loads sp/fp/pc from `to`, branches to target's saved pc.
/// All other registers are declared clobbered — the compiler
/// saves/restores callee-saved registers on the stack around
/// the asm, so switching sp implicitly switches those too.
inline fn switchContext(from: *Context, to: *const Context) void {
    var args = SwitchArgs{ .from = @intFromPtr(from), .to = @intFromPtr(to) };
    _ = switch (builtin.cpu.arch) {
        .aarch64 => asm volatile (
        // x1 = &args; load from/to pointers
            \\ldp x0, x2, [x1]
        // Save current sp, fp to from
            \\mov x3, sp
            \\stp x3, x29, [x0]
        // Capture resume label + save lr
            \\adr x3, 0f
            \\stp x3, x30, [x0, #16]
        // Load target sp, fp
            \\ldp x3, x29, [x2]
            \\mov sp, x3
        // Load target pc + lr, branch to pc
            \\ldp x3, x30, [x2, #16]
            \\br x3
        // Resume point
            \\0:
            : [_] "={x1}" (-> usize),
            : [args] "{x1}" (&args),
            : .{
                .x0 = true,
                .x2 = true,
                .x3 = true,
                .x4 = true,
                .x5 = true,
                .x6 = true,
                .x7 = true,
                .x8 = true,
                .x9 = true,
                .x10 = true,
                .x11 = true,
                .x12 = true,
                .x13 = true,
                .x14 = true,
                .x15 = true,
                .x16 = true,
                .x17 = true,
                // x18: platform-reserved, don't clobber
                .x19 = true,
                .x20 = true,
                .x21 = true,
                .x22 = true,
                .x23 = true,
                .x24 = true,
                .x25 = true,
                .x26 = true,
                .x27 = true,
                .x28 = true,
                // x29 (fp) and x30 (lr): handled explicitly in asm
                .d0 = true,
                .d1 = true,
                .d2 = true,
                .d3 = true,
                .d4 = true,
                .d5 = true,
                .d6 = true,
                .d7 = true,
                .d8 = true,
                .d9 = true,
                .d10 = true,
                .d11 = true,
                .d12 = true,
                .d13 = true,
                .d14 = true,
                .d15 = true,
                .d16 = true,
                .d17 = true,
                .d18 = true,
                .d19 = true,
                .d20 = true,
                .d21 = true,
                .d22 = true,
                .d23 = true,
                .d24 = true,
                .d25 = true,
                .d26 = true,
                .d27 = true,
                .d28 = true,
                .d29 = true,
                .d30 = true,
                .d31 = true,
                .memory = true,
            }
        ),
        else => @compileError("unsupported architecture"),
    };
}

// ============================================================
// §4. Trampoline and entry
// ============================================================

/// Entry point for new coroutines. sp points to a slot holding
/// the Coro pointer (placed there by makeContext).
fn trampoline() callconv(.naked) void {
    switch (builtin.cpu.arch) {
        .aarch64 => asm volatile (
            \\ldr x0, [sp]
            \\b %[entry]
            :
            : [entry] "X" (&coroEntry),
        ),
        else => @compileError("unsupported architecture"),
    }
}

/// Runs the user function, marks the coroutine dead, switches
/// back to the caller. Called via tail-call from trampoline.
fn coroEntry(co: *Coro) callconv(.c) noreturn {
    co.func(co);
    co.state = .dead;
    switchContext(&co.ctx, &co.caller_ctx);
    unreachable;
}

// ============================================================
// §5. Context initialization
// ============================================================

const PAGE_SIZE: usize = std.heap.page_size_min;

fn makeContext(co: *Coro) void {
    const stack_top = @intFromPtr(co.stack_base) + co.stack_size;
    const aligned_top = stack_top & ~@as(usize, 15);

    // Place Coro pointer at top of stack for trampoline to load
    const entry_sp = aligned_top - 16; // 16-byte aligned
    const slot: *usize = @ptrFromInt(entry_sp);
    slot.* = @intFromPtr(co);

    co.ctx = .{
        .sp = entry_sp,
        .fp = 0,
        .pc = @intFromPtr(&trampoline),
    };
}

// ============================================================
// §6. Default allocator (mmap via page_allocator)
// ============================================================

const page_alloc = std.heap.page_allocator;

fn defaultAlloc(size: usize, _: ?*anyopaque) ?[*]u8 {
    const buf = page_alloc.alloc(u8, size) catch return null;
    return buf.ptr;
}

fn defaultDealloc(ptr: [*]u8, size: usize, _: ?*anyopaque) void {
    // Bypass page_alloc.free() because the Allocator interface
    // @memsets freed memory with 0xaa in debug mode, which would
    // write into the PROT_NONE guard page and trigger SIGBUS.
    posix.munmap(@alignCast(ptr[0..size]));
}

// ============================================================
// §7. Guard page via mmap(MAP_FIXED)
// ============================================================

pub fn setGuardPage(addr: [*]align(std.heap.page_size_min) u8) !void {
    // Use mprotect to set PROT_NONE in-place. This keeps the original
    // mapping intact (unlike mmap(MAP_FIXED)), so munmap on the whole
    // block works correctly.
    if (std.c.mprotect(addr, PAGE_SIZE, .{}) != 0) return error.StackOverflow;
}

// ============================================================
// §8. Public API
// ============================================================

threadlocal var current_co: ?*Coro = null;

const DEFAULT_STACK_SIZE: usize = 64 * 1024;
const MIN_STACK_SIZE: usize = 16 * 1024;

pub fn descInit(func: EntryFn, stack_size: usize) Desc {
    return .{ .func = func, .stack_size = stack_size };
}

pub fn create(co: *Coro, desc: *Desc) CoroError!void {
    const func = desc.func;
    const raw_stack = if (desc.stack_size == 0) DEFAULT_STACK_SIZE else desc.stack_size;
    const stack_size = std.mem.alignForward(usize, @max(raw_stack, MIN_STACK_SIZE), PAGE_SIZE);

    // Block layout: [guard page] [usable stack]
    const block_size = PAGE_SIZE + stack_size;

    const alloc_fn = desc.alloc_fn orelse &defaultAlloc;
    const dealloc_fn = desc.dealloc_fn orelse &defaultDealloc;
    const alloc_data = desc.allocator_data;

    const block_ptr = alloc_fn(block_size, alloc_data) orelse return CoroError.OutOfMemory;

    co.* = .{
        .func = func,
        .user_data = desc.user_data,
        .stack_base = block_ptr + PAGE_SIZE,
        .stack_size = stack_size,
        .block_base = block_ptr,
        .block_size = block_size,
        .dealloc_fn = dealloc_fn,
        .dealloc_data = alloc_data,
    };

    // Set guard page at block start — skip for recycled blocks
    if (!desc.skip_guard_page) {
        setGuardPage(@alignCast(block_ptr)) catch {};
    }

    makeContext(co);
}

pub fn @"resume"(co: *Coro) CoroError!void {
    if (co.state != .suspended) return CoroError.NotSuspended;

    co.state = .running;
    co.prev_co = current_co;
    if (current_co) |prev| prev.state = .normal;
    current_co = co;

    switchContext(&co.caller_ctx, &co.ctx);

    // Back — restore previous coroutine
    current_co = co.prev_co;
    if (co.prev_co) |prev| prev.state = .running;
    co.prev_co = null;
}

pub fn yield(co: *Coro) CoroError!void {
    if (co.state != .running) return CoroError.NotRunning;
    co.state = .suspended;
    switchContext(&co.ctx, &co.caller_ctx);
}

pub fn destroy(co: *Coro) CoroError!void {
    if (co.block_size == 0) return; // already destroyed
    const dealloc = co.dealloc_fn orelse &defaultDealloc;
    const data = co.dealloc_data;
    const block = co.block_base;
    const size = co.block_size;
    co.block_size = 0;
    dealloc(block, size, data);
}

pub fn status(co: *Coro) State {
    return co.state;
}

pub fn running() ?*Coro {
    return current_co;
}

pub fn getUserData(co: *Coro) ?*anyopaque {
    return co.user_data;
}

pub fn setUserData(desc: *Desc, data: ?*anyopaque) void {
    desc.user_data = data;
}

// ============================================================
// §9. Tests
// ============================================================

test "basic context switch" {
    var desc = descInit(&struct {
        fn body(co: *Coro) void {
            yield(co) catch unreachable;
            yield(co) catch unreachable;
        }
    }.body, 0);
    var co: Coro = undefined;
    try create(&co, &desc);
    defer destroy(&co) catch {};

    try @"resume"(&co);
    try std.testing.expectEqual(State.suspended, co.state);
    try @"resume"(&co);
    try std.testing.expectEqual(State.suspended, co.state);
    try @"resume"(&co);
    try std.testing.expectEqual(State.dead, co.state);
}

test "nested coroutines" {
    var outer_desc = descInit(&struct {
        fn body(co: *Coro) void {
            var desc = descInit(&struct {
                fn inner(ico: *Coro) void {
                    yield(ico) catch unreachable;
                }
            }.inner, 0);
            var inner: Coro = undefined;
            create(&inner, &desc) catch unreachable;
            defer destroy(&inner) catch {};

            @"resume"(&inner) catch unreachable;
            yield(co) catch unreachable;
            @"resume"(&inner) catch unreachable;
        }
    }.body, 0);

    var outer: Coro = undefined;
    try create(&outer, &outer_desc);
    defer destroy(&outer) catch {};

    try @"resume"(&outer);
    try std.testing.expectEqual(State.suspended, outer.state);
    try @"resume"(&outer);
    try std.testing.expectEqual(State.dead, outer.state);
}

test "callee-saved register preservation (1M round-trips)" {
    const ITERS = 1_000_000;
    var desc = descInit(&struct {
        fn body(co: *Coro) void {
            for (0..ITERS) |_| {
                yield(co) catch unreachable;
            }
        }
    }.body, 0);
    var co: Coro = undefined;
    try create(&co, &desc);
    defer destroy(&co) catch {};

    for (0..ITERS) |_| {
        try @"resume"(&co);
    }
    try @"resume"(&co);
    try std.testing.expectEqual(State.dead, co.state);
}

test "user_data round-trip" {
    var payload: u64 = 42;
    var desc = descInit(&struct {
        fn body(co: *Coro) void {
            const ptr: *u64 = @ptrCast(@alignCast(getUserData(co).?));
            ptr.* += 1;
        }
    }.body, 0);
    setUserData(&desc, @ptrCast(&payload));
    var co: Coro = undefined;
    try create(&co, &desc);
    defer destroy(&co) catch {};

    try @"resume"(&co);
    try std.testing.expectEqual(@as(u64, 43), payload);
}

test "threadlocal running()" {
    var desc = descInit(&struct {
        fn body(co: *Coro) void {
            std.debug.assert(running().? == co);
            yield(co) catch unreachable;
        }
    }.body, 0);
    var co: Coro = undefined;
    try create(&co, &desc);
    defer destroy(&co) catch {};

    try std.testing.expectEqual(@as(?*Coro, null), running());
    try @"resume"(&co);
    try std.testing.expectEqual(@as(?*Coro, null), running());
    try @"resume"(&co);
}
