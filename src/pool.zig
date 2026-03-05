// StackPool — freelist allocator for coroutine stacks.
//
// Recycles coroutine blocks in userspace: destroy pushes to a
// singly-linked freelist, create pops from it. The freelist node
// is stored in-place at the start of the freed block (the Coro
// header area, which is always writable — the guard page is after it).

const std = @import("std");
const coro = @import("coro.zig");

pub const StackPool = struct {
    freelist: ?*FreeNode = null,

    const FreeNode = struct {
        next: ?*FreeNode,
        block_size: usize,
    };

    /// Override alloc/dealloc on a coroutine descriptor to route through this pool.
    pub fn patchDesc(self: *StackPool, desc: *coro.Desc) void {
        desc.alloc_fn = &poolAlloc;
        desc.dealloc_fn = &poolDealloc;
        desc.allocator_data = @ptrCast(self);
    }

    /// Free all blocks still on the freelist.
    pub fn deinit(self: *StackPool) void {
        var node = self.freelist;
        while (node) |n| {
            const size = n.block_size;
            const ptr: [*]u8 = @ptrCast(n);
            node = n.next;
            // Bypass page_allocator.free() — its debug @memset would
            // write into the PROT_NONE guard page and SIGBUS.
            std.posix.munmap(@alignCast(ptr[0..size]));
        }
        self.freelist = null;
    }

    fn poolAlloc(size: usize, allocator_data: ?*anyopaque) ?[*]u8 {
        const self: *StackPool = @ptrCast(@alignCast(allocator_data));
        if (self.freelist) |node| {
            if (node.block_size >= size) {
                self.freelist = node.next;
                return @ptrCast(node);
            }
        }
        return (std.heap.page_allocator.alloc(u8, size) catch return null).ptr;
    }

    fn poolDealloc(ptr: [*]u8, size: usize, allocator_data: ?*anyopaque) void {
        const self: *StackPool = @ptrCast(@alignCast(allocator_data));
        // FreeNode is stored at block start (Coro header area), which is
        // before the guard page and always writable.
        const node: *FreeNode = @ptrCast(@alignCast(ptr));
        node.* = .{ .next = self.freelist, .block_size = size };
        self.freelist = node;
    }
};

test "StackPool recycles coroutine blocks" {
    var pool = StackPool{};
    defer pool.deinit();

    var desc1 = coro.descInit(&struct {
        fn entry(_: *coro.Coro) void {}
    }.entry, 0);
    pool.patchDesc(&desc1);

    const co1 = try coro.create(&desc1);
    const addr1 = @intFromPtr(co1);
    try coro.destroy(co1);

    // Second create should reuse the recycled block.
    var desc2 = coro.descInit(&struct {
        fn entry(_: *coro.Coro) void {}
    }.entry, 0);
    pool.patchDesc(&desc2);

    const co2 = try coro.create(&desc2);
    try std.testing.expectEqual(addr1, @intFromPtr(co2));
    try coro.destroy(co2);
}
