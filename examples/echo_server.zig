// TCP echo server built with vrtl.
//
// Fibers yield on accept/read/write via vtable interception — BG threads
// do the blocking POSIX calls so worker threads stay free for other fibers.
//
// Build & run:
//   ~/.local/zig/zig build echo-server
//
// Test:
//   echo "hello" | nc localhost 8080

const std = @import("std");
const vrtl = @import("vrtl");
const net = std.Io.net;

// ---------------------------------------------------------------------------
// Effects
// ---------------------------------------------------------------------------

/// Log message emitted by connection handlers.
const Log = vrtl.Emit([]const u8);

// ---------------------------------------------------------------------------
// Unbuffered stream IO helpers
//
// std.Io.net.Stream only exposes buffered Reader/Writer which try to fill
// entire buffers — unusable for echo-style read-some/write-some patterns.
// These wrap the raw vtable calls with a sane interface.
// ---------------------------------------------------------------------------

fn streamRead(io: std.Io, handle: net.Socket.Handle, buf: []u8) !usize {
    var iov = [_][]u8{buf};
    return io.vtable.netRead(io.userdata, handle, &iov);
}

fn streamWriteAll(io: std.Io, handle: net.Socket.Handle, data: []const u8) !void {
    const splat = [_][]const u8{&.{}};
    var off: usize = 0;
    while (off < data.len) {
        const n = try io.vtable.netWrite(io.userdata, handle, data[off..], &splat, 0);
        if (n == 0) return error.ConnectionResetByPeer;
        off += n;
    }
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

var server: net.Server = undefined;
var conn_counter: u32 = 0;

// ---------------------------------------------------------------------------
// Connection handler (runs as a fiber)
// ---------------------------------------------------------------------------

fn echoHandler(ctx: *vrtl.EffectContext) void {
    const io = ctx.io;
    while (true) {
        const stream = server.accept(io) catch break;

        const id = @atomicRmw(u32, &conn_counter, .Add, 1, .monotonic);
        var msg_buf: [48]u8 = undefined;

        const connected_msg = std.fmt.bufPrint(&msg_buf, "conn {d}: connected", .{id}) catch "connected";
        ctx.emit(Log, connected_msg);

        echoLoop(io, stream);

        const disc_msg = std.fmt.bufPrint(&msg_buf, "conn {d}: disconnected", .{id}) catch "disconnected";
        ctx.emit(Log, disc_msg);

        stream.close(io);
    }
}

fn echoLoop(io: std.Io, stream: net.Stream) void {
    const handle = stream.socket.handle;
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = streamRead(io, handle, &buf) catch break;
        if (n == 0) break;
        streamWriteAll(io, handle, buf[0..n]) catch break;
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const PORT = 8080;
const NUM_WORKERS = 4;
const NUM_FIBERS = 64;

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    const io = init.io;

    // -- Create TCP listener via POSIX --
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.SocketFailed;
    errdefer _ = std.c.close(fd);

    const one: c_int = 1;
    _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.REUSEADDR, &one, @sizeOf(c_int));

    var addr: std.c.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, PORT),
        .addr = 0, // INADDR_ANY
    };
    if (std.c.bind(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) < 0)
        return error.BindFailed;
    if (std.c.listen(fd, 128) < 0)
        return error.ListenFailed;

    defer _ = std.c.close(fd);

    // Wrap raw fd in net.Server for the vtable-based accept path
    server = .{ .socket = .{ .handle = fd, .address = undefined } };

    std.debug.print("echo server listening on :{d} ({d} workers, {d} fibers)\n", .{ PORT, NUM_WORKERS, NUM_FIBERS });

    // -- Set up scheduler --
    var sched = try vrtl.Scheduler.init(alloc, NUM_WORKERS);
    defer sched.deinit();
    sched.setIo(io);

    // -- Handlers --
    var handlers = vrtl.HandlerSet.init(alloc);
    defer handlers.deinit();
    handlers.onEmit(Log, &struct {
        fn handle(msg: *const []const u8, _: ?*anyopaque) void {
            std.debug.print("[log] {s}\n", .{msg.*});
        }
    }.handle, null);

    // -- Spawn fibers --
    var fibers: [NUM_FIBERS]*vrtl.Scheduler.FiberEntry = undefined;
    for (0..NUM_FIBERS) |i| {
        fibers[i] = try sched.createFiber(&echoHandler, 0);
        try sched.spawn(fibers[i], &handlers);
    }

    // -- Run forever (until listener fd is closed / accept fails) --
    sched.run();

    for (&fibers) |f| sched.destroyFiber(f);
}
