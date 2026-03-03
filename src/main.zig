const std = @import("std");

pub fn main() !void {
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});
}

test "simple test" {
    const gpa = std.testing.allocator;
    var list: std.ArrayList(i32) = .empty;
    defer list.deinit(gpa);
    try list.append(gpa, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "fuzz example" {
    const Context = struct {
        fn testOne(context: @This(), smith: *std.testing.Smith) anyerror!void {
            _ = context;
            var buf: [12]u8 = undefined;
            smith.bytesWithHash(&buf, @as(u32, 0));
            try std.testing.expect(!std.mem.eql(u8, "canyoufindme", &buf));
        }
    };
    try std.testing.fuzz(Context{}, Context.testOne, .{});
}
