const std = @import("std");

/// Chase-Lev work-stealing deque.
///
/// Owner pushes/pops from the bottom (LIFO, good cache locality).
/// Thieves steal from the top (FIFO). All operations use atomic
/// load/store/CAS so the structure is ready for multi-threaded use.
///
/// The buffer is accessed via a BufRef pointer (single word, atomically
/// loadable). On grow, the old BufRef is retired into a linked list and
/// freed in deinit(), so thieves never touch freed memory.
pub fn WorkStealingDeque(comptime T: type) type {
    return struct {
        const Self = @This();

        const BufRef = struct {
            data: [*]T,
            mask: isize,
            alloc_len: usize, // for freeing
            prev: ?*BufRef, // retired-buffer chain
        };

        buf: *BufRef,
        bottom: isize align(std.atomic.cache_line) = 0,
        top: isize align(std.atomic.cache_line) = 0,
        allocator: std.mem.Allocator,

        pub const StealResult = union(enum) {
            success: T,
            empty,
            abort,
        };

        pub fn init(allocator: std.mem.Allocator, initial_capacity: usize) !Self {
            const cap = std.math.ceilPowerOfTwo(usize, initial_capacity) catch initial_capacity;
            const data = try allocator.alloc(T, cap);
            const buf = try allocator.create(BufRef);
            buf.* = .{ .data = data.ptr, .mask = @intCast(cap - 1), .alloc_len = cap, .prev = null };
            return .{ .buf = buf, .allocator = allocator };
        }

        pub fn deinit(self: *Self) void {
            var cur: ?*BufRef = self.buf;
            while (cur) |b| {
                const prev = b.prev;
                self.allocator.free(b.data[0..b.alloc_len]);
                self.allocator.destroy(b);
                cur = prev;
            }
        }

        /// Owner: push to bottom.
        pub fn push(self: *Self, item: T) !void {
            const b = @atomicLoad(isize, &self.bottom, .monotonic);
            const t = @atomicLoad(isize, &self.top, .acquire);
            var buf = self.buf;
            if (b - t > buf.mask) {
                buf = try self.grow(buf);
            }
            buf.data[@intCast(b & buf.mask)] = item;
            // Release store: item write visible before bottom is published.
            @atomicStore(isize, &self.bottom, b + 1, .release);
        }

        /// Owner: pop from bottom (LIFO).
        pub fn pop(self: *Self) ?T {
            const b = @atomicLoad(isize, &self.bottom, .monotonic) - 1;
            // SeqCst store + SeqCst load provide the StoreLoad barrier
            // between bottom and top that Chase-Lev requires. On ARM64
            // (RCPC), acquire loads use ldapr which is NOT ordered after
            // stlr to a different address — seq_cst (ldar) is required.
            @atomicStore(isize, &self.bottom, b, .seq_cst);
            const t = @atomicLoad(isize, &self.top, .seq_cst);

            if (t <= b) {
                const buf = self.buf;
                const item = buf.data[@intCast(b & buf.mask)];
                if (t == b) {
                    // Last element — race with steal.
                    if (@cmpxchgStrong(isize, &self.top, t, t + 1, .seq_cst, .monotonic) != null) {
                        @atomicStore(isize, &self.bottom, b + 1, .monotonic);
                        return null;
                    }
                    @atomicStore(isize, &self.bottom, b + 1, .monotonic);
                }
                return item;
            } else {
                @atomicStore(isize, &self.bottom, b + 1, .monotonic);
                return null;
            }
        }

        /// Thief: steal from top (FIFO).
        pub fn steal(self: *Self) StealResult {
            const t = @atomicLoad(isize, &self.top, .seq_cst);
            const b = @atomicLoad(isize, &self.bottom, .seq_cst);

            if (t < b) {
                // Acquire load pairs with release store in grow(), so we
                // see a fully-constructed BufRef (data + mask consistent).
                // A stale pointer is also safe: old buffers are never freed
                // or mutated, and the CAS below guards correctness.
                const buf = @atomicLoad(*BufRef, &self.buf, .acquire);
                const item = buf.data[@intCast(t & buf.mask)];
                if (@cmpxchgStrong(isize, &self.top, t, t + 1, .seq_cst, .monotonic) != null) {
                    return .abort;
                }
                return .{ .success = item };
            }
            return .empty;
        }

        pub fn len(self: *const Self) usize {
            const b = @atomicLoad(isize, &self.bottom, .acquire);
            const t = @atomicLoad(isize, &self.top, .acquire);
            const size = b - t;
            return if (size > 0) @intCast(size) else 0;
        }

        fn grow(self: *Self, old: *BufRef) !*BufRef {
            const new_cap = old.alloc_len * 2;
            const new_data = try self.allocator.alloc(T, new_cap);
            const t = @atomicLoad(isize, &self.top, .monotonic);
            const b = @atomicLoad(isize, &self.bottom, .monotonic);
            const new_mask: isize = @intCast(new_cap - 1);
            var i = t;
            while (i < b) : (i += 1) {
                new_data[@intCast(i & new_mask)] = old.data[@intCast(i & old.mask)];
            }
            const new_buf = try self.allocator.create(BufRef);
            new_buf.* = .{ .data = new_data.ptr, .mask = new_mask, .alloc_len = new_cap, .prev = old };
            // Release store: thieves acquire-load this pointer and see
            // the fully-constructed BufRef.
            @atomicStore(*BufRef, &self.buf, new_buf, .release);
            return new_buf;
        }
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "WorkStealingDeque: push and pop" {
    var d = try WorkStealingDeque(u32).init(testing.allocator, 4);
    defer d.deinit();

    try d.push(1);
    try d.push(2);
    try d.push(3);
    try testing.expectEqual(@as(usize, 3), d.len());

    // Pop is LIFO
    try testing.expectEqual(@as(u32, 3), d.pop().?);
    try testing.expectEqual(@as(u32, 2), d.pop().?);
    try testing.expectEqual(@as(u32, 1), d.pop().?);
    try testing.expectEqual(@as(?u32, null), d.pop());
}

test "WorkStealingDeque: push and pop interleaved" {
    var d = try WorkStealingDeque(u32).init(testing.allocator, 4);
    defer d.deinit();

    try d.push(10);
    try d.push(20);
    try testing.expectEqual(@as(u32, 20), d.pop().?);
    try d.push(30);
    try testing.expectEqual(@as(u32, 30), d.pop().?);
    try testing.expectEqual(@as(u32, 10), d.pop().?);
    try testing.expectEqual(@as(?u32, null), d.pop());
}

test "WorkStealingDeque: grow beyond initial capacity" {
    var d = try WorkStealingDeque(u32).init(testing.allocator, 2);
    defer d.deinit();

    // Push more items than initial capacity (2)
    for (0..16) |i| {
        try d.push(@intCast(i));
    }
    try testing.expectEqual(@as(usize, 16), d.len());

    // Pop all (LIFO order)
    var i: u32 = 15;
    while (d.pop()) |val| {
        try testing.expectEqual(i, val);
        if (i == 0) break;
        i -= 1;
    }
}

test "WorkStealingDeque: steal basics" {
    var d = try WorkStealingDeque(u32).init(testing.allocator, 4);
    defer d.deinit();

    // Steal from empty deque
    try testing.expectEqual(WorkStealingDeque(u32).StealResult.empty, d.steal());

    try d.push(1);
    try d.push(2);
    try d.push(3);

    // Steal is FIFO (from top)
    try testing.expectEqual(@as(u32, 1), d.steal().success);
    try testing.expectEqual(@as(u32, 2), d.steal().success);

    // Pop gets remaining (from bottom)
    try testing.expectEqual(@as(u32, 3), d.pop().?);
    try testing.expectEqual(@as(?u32, null), d.pop());
}

test "WorkStealingDeque: steal and pop race on last element" {
    var d = try WorkStealingDeque(u32).init(testing.allocator, 4);
    defer d.deinit();

    try d.push(42);

    // Single-threaded: pop wins over steal since pop adjusts top via CAS.
    try testing.expectEqual(@as(u32, 42), d.pop().?);
    try testing.expectEqual(WorkStealingDeque(u32).StealResult.empty, d.steal());
}

test "WorkStealingDeque: concurrent owner + thieves" {
    // Topology:
    //   1 owner thread — pushes items in waves, pops between waves
    //   4 thief threads — steal concurrently throughout
    //
    // Invariant: every value 0..N is observed exactly once across all threads.
    // This exercises push/pop vs steal races, CAS contention between thieves,
    // grow under contention, and the last-element pop-vs-steal race.

    const num_thieves = 4;
    const items_per_wave = 200;
    const num_waves = 50;
    const total_items = items_per_wave * num_waves;

    const Deque = WorkStealingDeque(u32);

    // Start small to exercise grow() under contention.
    var deque = try Deque.init(testing.allocator, 8);
    defer deque.deinit();
    var owner_done: u32 = 0; // atomic flag

    const ThiefCtx = struct {
        deque: *Deque,
        owner_done: *u32,
        collected: []u32,
        count: usize,
    };

    var thief_bufs: [num_thieves][]u32 = undefined;
    var thief_ctxs: [num_thieves]ThiefCtx = undefined;
    for (0..num_thieves) |i| {
        thief_bufs[i] = try testing.allocator.alloc(u32, total_items);
        thief_ctxs[i] = .{
            .deque = &deque,
            .owner_done = &owner_done,
            .collected = thief_bufs[i],
            .count = 0,
        };
    }
    defer for (0..num_thieves) |i| {
        testing.allocator.free(thief_bufs[i]);
    };

    // Spawn thief threads.
    var thief_threads: [num_thieves]std.Thread = undefined;
    for (0..num_thieves) |i| {
        thief_threads[i] = try std.Thread.spawn(.{}, struct {
            fn run(ctx: *ThiefCtx) void {
                while (true) {
                    switch (ctx.deque.steal()) {
                        .success => |val| {
                            ctx.collected[ctx.count] = val;
                            ctx.count += 1;
                        },
                        .abort => continue,
                        .empty => {
                            if (@atomicLoad(u32, ctx.owner_done, .acquire) == 1) {
                                // Drain stragglers after owner signals done.
                                while (true) {
                                    switch (ctx.deque.steal()) {
                                        .success => |val| {
                                            ctx.collected[ctx.count] = val;
                                            ctx.count += 1;
                                        },
                                        .abort => continue,
                                        .empty => return,
                                    }
                                }
                            }
                        },
                    }
                }
            }
        }.run, .{&thief_ctxs[i]});
    }

    // Owner: push waves of items, pop some between waves.
    var owner_buf: [total_items]u32 = undefined;
    var owner_count: usize = 0;
    var next_val: u32 = 0;

    for (0..num_waves) |_| {
        // Push a wave.
        for (0..items_per_wave) |_| {
            try deque.push(next_val);
            next_val += 1;
        }
        // Pop roughly half back (owner competes with thieves for these).
        for (0..items_per_wave / 2) |_| {
            if (deque.pop()) |val| {
                owner_buf[owner_count] = val;
                owner_count += 1;
            }
        }
    }
    // Owner drains remaining.
    while (deque.pop()) |val| {
        owner_buf[owner_count] = val;
        owner_count += 1;
    }

    // Signal thieves to finish.
    @atomicStore(u32, &owner_done, 1, .release);

    for (0..num_thieves) |i| {
        thief_threads[i].join();
    }

    // Verify: every value in 0..total_items observed exactly once.
    var seen = try testing.allocator.alloc(bool, total_items);
    defer testing.allocator.free(seen);
    @memset(seen, false);

    for (owner_buf[0..owner_count]) |v| {
        try testing.expect(v < total_items);
        try testing.expect(!seen[v]);
        seen[v] = true;
    }
    for (0..num_thieves) |i| {
        for (thief_ctxs[i].collected[0..thief_ctxs[i].count]) |v| {
            try testing.expect(v < total_items);
            try testing.expect(!seen[v]);
            seen[v] = true;
        }
    }
    for (seen) |s| {
        try testing.expect(s);
    }
}
