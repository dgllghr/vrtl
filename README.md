# vrtl

Experimental algebraic effects for Zig, built on [minicoro](https://github.com/edubart/minicoro).

**This is a research prototype.** It works on Zig 0.16-dev (nightly) only.
Performance is not competitive with native runtime implementations like
OCaml 5. Single-threaded only.

The stackless coroutines
[being considered](https://github.com/ziglang/zig/issues/23446) for a future
Zig release would be a much better foundation than minicoro's stackful
coroutines — smaller memory footprint, no C FFI overhead on context switch,
and potential for compiler-driven optimizations.

## What it does

Effects let you write code that *describes* what it needs (read a file, log a
message, look up a value) without hard-coding how those things happen. Handlers
are bound separately and can be swapped, layered, and composed.

Two kinds of effects:

- **Perform** — the fiber suspends, a handler runs, and the fiber receives a
  result. Like a function call that goes through an indirection layer.
- **Emit** — the fiber notifies observers and continues. Fire-and-observe.

A `Scheduler` manages multiple fibers cooperatively and integrates with Zig's
`std.Io` so that fibers yield on IO operations and other fibers can run in the
meantime.

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
var fib = try vt.initFiberDefault(&struct {
    fn body(ctx: *vt.EffectContext) void {
        const contents = ctx.perform(ReadFile, "config.json");
        ctx.emit(Log, contents);
    }
}.body);
defer fib.deinit();
```

### Binding handlers

```zig
var handlers = vt.HandlerSet.init(allocator);
defer handlers.deinit();

handlers.onPerform(ReadFile, &struct {
    fn handle(filename: *ReadFile.Value, cont: *vt.Cont(ReadFile), _: ?*anyopaque) void {
        // In real code you'd read the file here.
        cont.@"resume"("file contents");
    }
}.handle, null);

handlers.onEmit(Log, &struct {
    fn handle(msg: *const Log.Value, _: ?*anyopaque) void {
        std.debug.print("{s}\n", .{msg.*});
    }
}.handle, null);
```

### Running

For a single fiber:

```zig
vt.run(&fib, &handlers);
```

For multiple fibers with IO scheduling:

```zig
var sched = vt.Scheduler.init(allocator);
defer sched.deinit();

var res = try sched.createFiber(&myBody, io, 0);
defer res.fiber.deinit();

try sched.spawn(&res.fiber, res.handle, &handlers);
sched.run();
```

### Handler composition

Handlers can be layered. A child handler can delegate effects it doesn't
handle to a parent:

```zig
var cache = vt.HandlerSet.init(allocator);
cache.onPerform(ReadFile, &struct {
    fn handle(key: *ReadFile.Value, cont: *vt.Cont(ReadFile), _: ?*anyopaque) void {
        if (std.mem.eql(u8, key.*, "cached.txt")) {
            cont.@"resume"("from cache");
        } else {
            cont.delegate();  // pass to parent
        }
    }
}.handle, null);

var db = vt.HandlerSet.init(allocator);
db.onPerform(ReadFile, &struct {
    fn handle(_: *ReadFile.Value, cont: *vt.Cont(ReadFile), _: ?*anyopaque) void {
        cont.@"resume"("from db");
    }
}.handle, null);

cache.setParent(&db);
vt.run(&fib, &cache);
```

## Building

Requires Zig 0.16-dev and a C compiler (for minicoro).

```
zig build test
zig build bench
```

## License

MIT
