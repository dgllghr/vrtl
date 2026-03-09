// StackPool — freelist allocator for coroutine stacks.
//
// Recycles stack blocks in userspace: destroy pushes to a
// singly-linked freelist, create pops from it. The freelist node
// is stored at block_base + PAGE_SIZE (start of stack area, above
// the guard page, always writable).
//
// Blocks are bucketed by size class so small handler stacks don't
// consume large recycled blocks (and vice versa).

const std = @import("std");
const coro = @import("coro.zig");

const PAGE_SIZE = std.heap.page_size_min;

pub const StackPool = struct {
    const NUM_CLASSES = 3;

    freelists: [NUM_CLASSES]?*FreeNode = .{ null, null, null },

    const FreeNode = struct {
        next: ?*FreeNode,
        block_size: usize,
    };

    fn sizeClass(block_size: usize) usize {
        if (block_size <= 48 * 1024) return 0; // handler stacks
        if (block_size <= 128 * 1024) return 1; // default stacks
        return 2; // large stacks
    }

    /// Override alloc/dealloc on a coroutine descriptor to route through this pool.
    /// Guard page setup is handled here (fresh allocs only), so coro.create skips it.
    pub fn patchDesc(self: *StackPool, desc: *coro.Desc) void {
        desc.alloc_fn = &poolAlloc;
        desc.dealloc_fn = &poolDealloc;
        desc.allocator_data = @ptrCast(self);
        desc.skip_guard_page = true;
    }

    /// Free all blocks still on the freelist.
    pub fn deinit(self: *StackPool) void {
        for (&self.freelists) |*fl| {
            var node = fl.*;
            while (node) |n| {
                const size = n.block_size;
                // FreeNode is at block_base + PAGE_SIZE; recover block_base
                const block_base: [*]u8 = @as([*]u8, @ptrCast(n)) - PAGE_SIZE;
                node = n.next;
                // Bypass page_allocator.free() — its debug @memset would
                // write into the PROT_NONE guard page and SIGBUS.
                std.posix.munmap(@alignCast(block_base[0..size]));
            }
            fl.* = null;
        }
    }

    /// Allocate a stack block from the pool. Returns a block with guard
    /// page already set (recycled blocks reuse existing guard pages).
    pub fn allocBlock(self: *StackPool, size: usize) ?[*]u8 {
        return poolAlloc(size, @ptrCast(self));
    }

    fn poolAlloc(size: usize, allocator_data: ?*anyopaque) ?[*]u8 {
        const self: *StackPool = @ptrCast(@alignCast(allocator_data));
        const cls = sizeClass(size);
        // Recycled block — guard page already set
        if (self.freelists[cls]) |node| {
            if (node.block_size >= size) {
                self.freelists[cls] = node.next;
                // FreeNode is at block_base + PAGE_SIZE; return block_base
                return @as([*]u8, @ptrCast(node)) - PAGE_SIZE;
            }
        }
        // Fresh allocation — set guard page here (coro.create skips it for pooled descs)
        const ptr = (std.heap.page_allocator.alloc(u8, size) catch return null).ptr;
        coro.setGuardPage(@alignCast(ptr)) catch return null;
        return ptr;
    }

    fn poolDealloc(ptr: [*]u8, size: usize, allocator_data: ?*anyopaque) void {
        const self: *StackPool = @ptrCast(@alignCast(allocator_data));
        const cls = sizeClass(size);
        // FreeNode is stored at block_base + PAGE_SIZE (stack area above guard).
        const node: *FreeNode = @ptrCast(@alignCast(ptr + PAGE_SIZE));
        node.* = .{ .next = self.freelists[cls], .block_size = size };
        self.freelists[cls] = node;
    }
};

test "StackPool recycles coroutine blocks" {
    var pool = StackPool{};
    defer pool.deinit();

    var desc1 = coro.descInit(&struct {
        fn entry(_: *coro.Coro) void {}
    }.entry, 0);
    pool.patchDesc(&desc1);

    var co1: coro.Coro = undefined;
    try coro.create(&co1, &desc1);
    const addr1 = @intFromPtr(co1.block_base);
    try coro.destroy(&co1);

    // Second create should reuse the recycled block.
    var desc2 = coro.descInit(&struct {
        fn entry(_: *coro.Coro) void {}
    }.entry, 0);
    pool.patchDesc(&desc2);

    var co2: coro.Coro = undefined;
    try coro.create(&co2, &desc2);
    try std.testing.expectEqual(addr1, @intFromPtr(co2.block_base));
    try coro.destroy(&co2);
}

test "StackPool size classes prevent cross-class reuse" {
    var pool = StackPool{};
    defer pool.deinit();

    // Allocate a large block (default 64KB stack → class 1)
    var desc_big = coro.descInit(&struct {
        fn entry(_: *coro.Coro) void {}
    }.entry, 0);
    pool.patchDesc(&desc_big);

    var co_big: coro.Coro = undefined;
    try coro.create(&co_big, &desc_big);
    try coro.destroy(&co_big);

    // Allocate a small block (handler-sized stack → class 0)
    // Should NOT reuse the large block
    var desc_small = coro.descInit(&struct {
        fn entry(_: *coro.Coro) void {}
    }.entry, std.heap.page_size_min);
    pool.patchDesc(&desc_small);

    var co_small: coro.Coro = undefined;
    try coro.create(&co_small, &desc_small);
    // The small block should be a fresh allocation, not the recycled large block
    try std.testing.expect(@intFromPtr(co_small.block_base) != @intFromPtr(co_big.block_base));
    try coro.destroy(&co_small);
}
