// StackPool — freelist allocator for minicoro stacks.
//
// Recycles coroutine blocks in userspace: deinit pushes to a
// singly-linked freelist, init pops from it. The freelist node
// is stored in-place at the start of the freed block.

const std = @import("std");
const coro = @import("coro.zig");

pub const StackPool = struct {
    freelist: ?*FreeNode = null,

    const FreeNode = struct {
        next: ?*FreeNode,
    };

    /// Override alloc/dealloc on a minicoro descriptor to route through this pool.
    pub fn patchDesc(self: *StackPool, desc: *coro.Desc) void {
        desc.alloc_cb = &poolAlloc;
        desc.dealloc_cb = &poolDealloc;
        desc.allocator_data = @ptrCast(self);
    }

    /// Free all blocks still on the freelist.
    pub fn deinit(self: *StackPool) void {
        var node = self.freelist;
        while (node) |n| {
            node = n.next;
            std.c.free(@ptrCast(n));
        }
        self.freelist = null;
    }

    fn poolAlloc(size: usize, allocator_data: ?*anyopaque) callconv(.c) ?*anyopaque {
        const self: *StackPool = @ptrCast(@alignCast(allocator_data));
        if (self.freelist) |node| {
            self.freelist = node.next;
            return @ptrCast(node);
        }
        return std.c.calloc(1, size);
    }

    fn poolDealloc(ptr: ?*anyopaque, _: usize, allocator_data: ?*anyopaque) callconv(.c) void {
        const self: *StackPool = @ptrCast(@alignCast(allocator_data));
        const node: *FreeNode = @ptrCast(@alignCast(ptr));
        node.next = self.freelist;
        self.freelist = node;
    }
};

test "StackPool recycles coroutine blocks" {
    var pool = StackPool{};
    defer pool.deinit();

    var desc1 = coro.descInit(&struct {
        fn entry(_: ?*coro.Coro) callconv(.c) void {}
    }.entry, 0);
    pool.patchDesc(&desc1);

    const co1 = try coro.create(&desc1);
    const addr1 = @intFromPtr(co1);
    try coro.destroy(co1);

    // Second create should reuse the recycled block.
    var desc2 = coro.descInit(&struct {
        fn entry(_: ?*coro.Coro) callconv(.c) void {}
    }.entry, 0);
    pool.patchDesc(&desc2);

    const co2 = try coro.create(&desc2);
    try std.testing.expectEqual(addr1, @intFromPtr(co2));
    try coro.destroy(co2);
}
