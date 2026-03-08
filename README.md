# vrtl

Algebraic effects for Zig with multicore work-stealing.

**This is a research prototype.** It works on Zig 0.16-dev (nightly) only.
ARM64 (Apple Silicon) only for now.

Pure Zig coroutine runtime — no C dependencies. Multicore scheduler with
N worker threads, per-worker Chase-Lev deques, and futex-based parking.

## What it does

Effects let you write code that *describes* what it needs (read a file, log a
message, look up a value) without hard-coding how those things happen. Handlers
are bound separately and can be swapped, layered, and composed.

Two kinds of effects:

- **Perform** — the fiber suspends, a handler runs, and the fiber receives a
  result. Like a function call that goes through an indirection layer.
- **Emit** — the fiber notifies observers and continues. Fire-and-observe.

A multicore `Scheduler` manages fibers across N worker threads with
work-stealing and integrates with Zig's `std.Io` so that fibers yield on IO
operations and other fibers can run in the meantime.

## Usage

Add vrtl as a dependency in `build.zig.zon`, then import the module:

```zig
const vt = @import("vrtl");
```

### Declaring effects

Effects are types. The type encodes the value you send and (for perform) the
value you get back:

```zig
const ReadFile = vt.Perform([]const u8, []const u8);  // send filename, get contents
const Log = vt.Emit([]const u8);                       // send a message
```

### Writing an effectful fiber

```zig
var fib: vt.EffectFiber = undefined;
try vt.initFiberDefault(&fib, &struct {
    fn body(ctx: *vt.EffectContext) void {
        const contents = ctx.perform(ReadFile, "config.json");
        ctx.emit(Log, contents);
    }
}.body);
defer fib.deinit();
```

### Binding handlers

Perform handlers run in their own fiber and receive an `EffectContext`, so they
can perform and emit effects themselves:

```zig
var handlers = vt.HandlerSet.init(allocator);
defer handlers.deinit();

handlers.onPerform(ReadFile, &struct {
    fn handle(
        filename: *ReadFile.Value,
        cont: *vt.Cont(ReadFile),
        ctx: *vt.EffectContext,
        _: ?*anyopaque,
    ) void {
        ctx.emit(Log, "reading file");
        // In real code you'd read the file here.
        cont.@"resume"("file contents");
    }
}.handle, null);

handlers.onEmit(Log, &struct {
    fn handle(msg: *const Log.Value, _: *vt.EffectContext, _: ?*anyopaque) void {
        std.debug.print("{s}\n", .{msg.*});
    }
}.handle, null);
```

For handlers that don't need their own fiber (no effects, just
resume/drop/delegate), use `onPerformSync`:

```zig
handlers.onPerformSync(ReadFile, &struct {
    fn handle(filename: *ReadFile.Value, cont: *vt.Cont(ReadFile), _: ?*anyopaque) void {
        cont.@"resume"("file contents");
    }
}.handle, null);
```

### Running

For a single fiber (no scheduler, synchronous dispatch — only `onPerformSync`
handlers):

```zig
vt.run(&fib, &handlers);
```

For multiple fibers with IO scheduling:

```zig
var sched = try vt.Scheduler.init(allocator, 4); // 4 worker threads
defer sched.deinit();
sched.setIo(io); // optional: integrate with std.Io

const entry = try sched.createFiber(&myBody, 0); // 0 = default stack size
defer sched.destroyFiber(entry);

try sched.spawn(entry, &handlers);
sched.run(); // blocks until all fibers complete
```

### Handler composition

Handlers can be layered. A child handler can delegate effects it doesn't
handle to a parent:

```zig
var cache = vt.HandlerSet.init(allocator);
cache.onPerform(ReadFile, &struct {
    fn handle(
        key: *ReadFile.Value,
        cont: *vt.Cont(ReadFile),
        ctx: *vt.EffectContext,
        _: ?*anyopaque,
    ) void {
        if (std.mem.eql(u8, key.*, "cached.txt")) {
            cont.@"resume"("from cache");
        } else {
            // Re-perform to parent scope
            const result = ctx.perform(ReadFile, key.*);
            cont.@"resume"(result);
        }
    }
}.handle, null);

var db = vt.HandlerSet.init(allocator);
db.onPerformSync(ReadFile, &struct {
    fn handle(_: *ReadFile.Value, cont: *vt.Cont(ReadFile), _: ?*anyopaque) void {
        cont.@"resume"("from db");
    }
}.handle, null);

cache.setParent(&db);
try sched.spawn(entry, &cache);
```

### Suspend / wake

Fibers can suspend until explicitly woken by another fiber or thread:

```zig
fn body(ctx: *vt.EffectContext) void {
    var wh: vt.WakeHandle = .{};

    // Pass wh to another fiber/thread, then suspend.
    // The other side calls wh.wake() to resume this fiber.
    ctx.@"suspend"(&wh);

    // Continues here after wake.
}
```

`wh.wake()` is safe to call from any thread. If called before the scheduler
processes the suspend, the fiber resumes immediately (early wake).

### Continuation API

The `Cont(E)` type is used in all perform handlers:

- `cont.resume(value)` — deliver a value and resume the performer
- `cont.drop()` — destroy the fiber without resuming (performer never returns)
- `cont.delegate()` — pass the effect to the parent handler scope

If the handler returns without calling any of these, the fiber is auto-dropped.

## Building

Requires Zig 0.16-dev. No C dependencies.

```
zig build test
zig build bench
```

## License

MIT
