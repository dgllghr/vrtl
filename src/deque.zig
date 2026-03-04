const std = @import("std");

/// Chase-Lev work-stealing deque.
///
/// Owner pushes/pops from the bottom (LIFO, good cache locality).
/// Thieves steal from the top (FIFO). All operations use atomic
/// load/store/CAS so the structure is ready for multi-threaded use.
pub fn WorkStealingDeque(comptime T: type) type {
    return struct {
        const Self = @This();

        buffer: []T,
        top: isize = 0,
        bottom: isize = 0,
        allocator: std.mem.Allocator,

        pub const StealResult = union(enum) {
            success: T,
            empty,
            abort,
        };

        pub fn init(allocator: std.mem.Allocator, initial_capacity: usize) !Self {
            const cap = std.math.ceilPowerOfTwo(usize, initial_capacity) catch initial_capacity;
            const buffer = try allocator.alloc(T, cap);
            return .{ .buffer = buffer, .allocator = allocator };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buffer);
        }

        /// Owner: push to bottom.
        pub fn push(self: *Self, item: T) !void {
            const b = @atomicLoad(isize, &self.bottom, .monotonic);
            const t = @atomicLoad(isize, &self.top, .acquire);
            if (b - t >= @as(isize, @intCast(self.buffer.len))) {
                try self.grow();
            }
            self.buffer[self.idx(b)] = item;
            // Release store: item write visible before bottom is published.
            @atomicStore(isize, &self.bottom, b + 1, .release);
        }

        /// Owner: pop from bottom (LIFO).
        pub fn pop(self: *Self) ?T {
            const b = @atomicLoad(isize, &self.bottom, .monotonic) - 1;
            // SeqCst store ensures stealers see the decremented bottom
            // before we read top (replaces relaxed store + seq_cst fence).
            @atomicStore(isize, &self.bottom, b, .seq_cst);
            const t = @atomicLoad(isize, &self.top, .acquire);

            if (t <= b) {
                const item = self.buffer[self.idx(b)];
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
            // SeqCst load of top orders with the seq_cst store to bottom
            // in pop(), ensuring we see a consistent snapshot.
            const t = @atomicLoad(isize, &self.top, .seq_cst);
            const b = @atomicLoad(isize, &self.bottom, .acquire);

            if (t < b) {
                const item = self.buffer[self.idx(t)];
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

        fn idx(self: *const Self, i: isize) usize {
            return @intCast(i & (@as(isize, @intCast(self.buffer.len)) - 1));
        }

        fn grow(self: *Self) !void {
            const old_cap = self.buffer.len;
            const new_cap = old_cap * 2;
            const new_buf = try self.allocator.alloc(T, new_cap);
            const t = @atomicLoad(isize, &self.top, .monotonic);
            const b = @atomicLoad(isize, &self.bottom, .monotonic);
            const new_mask: isize = @intCast(new_cap - 1);
            const old_mask: isize = @intCast(old_cap - 1);
            var i = t;
            while (i < b) : (i += 1) {
                new_buf[@intCast(i & new_mask)] = self.buffer[@intCast(i & old_mask)];
            }
            self.allocator.free(self.buffer);
            self.buffer = new_buf;
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
