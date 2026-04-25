# datastream

[![Hex](https://img.shields.io/hexpm/v/datastream)](https://hex.pm/packages/datastream)
[![Hex Downloads](https://img.shields.io/hexpm/dt/datastream)](https://hex.pm/packages/datastream)
[![CI](https://github.com/nao1215/datastream/actions/workflows/ci.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/ci.yml)

`datastream` is a pull-based stream library for Gleam.

It is meant for pipelines that should stay lazy, repeatable, and explicit
about effects. A `Stream(a)` is a pipeline definition, not a materialized
collection, so each terminal operation runs the source again.

## Install

```sh
gleam add datastream
```

API reference: <https://hexdocs.pm/datastream>

## Target support

- Erlang target: every module in this package
- JavaScript target: the cross-target core only
- `datastream/erlang/*` modules are BEAM-only
- On JavaScript, `datastream` does not provide async streaming I/O
  adapters. Resolve async I/O outside the library, then feed the data into
  the core with constructors such as `source.from_list`,
  `source.from_bit_array`, or `source.once`

## Use cases

- Build pipelines from lists, ranges, options, results, or custom state
- Transform infinite or finite streams with `map`, `filter`, `take`, and
  `flat_map`
- Process chunked text or bytes without joining the whole input first
- Wrap your own synchronous resources with `source.resource` and
  `source.try_resource`
- On Erlang, work with subjects, timers, and bounded parallelism through
  `datastream/erlang/*`

## When to use

Reach for `datastream` when:

- The input is large or unbounded and shouldn't fit in memory all at
  once.
- A pipeline wraps a real resource (file handle, socket, cursor) that
  must be released on every termination path.
- You need built-in back-pressure for parallel `map` / `each` on
  Erlang (`datastream/erlang/par`).
- You need chunk-boundary-aware framing for byte or text protocols
  (`datastream/text`, `datastream/binary`).

Stick with `gleam/list` when the input already fits in memory and you
don't need lazy pulls, repeatable runs, or resource cleanup. `List`
is simpler and faster for the small-finite case.

## Examples

Each example below is a complete `src/app.gleam` you can paste in
after `gleam new app && gleam add datastream`, then run with `gleam run`.

### Basic pipeline

```gleam
import datastream/fold
import datastream/source
import datastream/stream
import gleam/io

pub fn main() {
  let result =
    source.iterate(from: 1, with: fn(x) { x + 1 })
    |> stream.map(with: fn(x) { x * 2 })
    |> stream.take(up_to: 5)
    |> fold.to_list
  io.debug(result)
  // [2, 4, 6, 8, 10]
}
```

### Line-oriented text

```gleam
import datastream/fold
import datastream/source
import datastream/text
import gleam/io

pub fn main() {
  let lines =
    source.from_list(["hel", "lo\nwor", "ld\n"])
    |> text.lines
    |> fold.to_list
  io.debug(lines)
  // ["hello", "world"]
}
```

### Binary framing

```gleam
import datastream/binary
import datastream/fold
import datastream/source
import gleam/io

pub fn main() {
  let frames =
    source.from_list([<<2, 65>>, <<66, 1, 67>>])
    |> binary.length_prefixed(prefix_size: 1)
    |> fold.to_list
  io.debug(frames)
  // [Ok(<<65, 66>>), Ok(<<67>>)]
  // A truncated trailing frame surfaces as one
  // Error(IncompleteFrame(expected:, got:)) before the stream halts,
  // so callers can react instead of silently losing data.
}
```

### Result-shaped streams

```gleam
import datastream/fold
import datastream/source
import gleam/io

pub fn main() {
  let result =
    source.from_list([Ok(1), Ok(2), Error("bad input")])
    |> fold.collect_result
  io.debug(result)
  // Error("bad input") : Result(List(Int), String)
}
```

### Use with dataprep

`datastream` does not depend on `dataprep`. Adding it (`gleam add
dataprep`) and combining `fold.fold` with a small applicative
`combine` step accumulates every per-element error in a single pass —
the right tool when reporting all failures matters more than stopping
on the first one.

```gleam
import dataprep/non_empty_list
import dataprep/validated.{type Validated, Invalid, Valid}
import datastream/fold
import datastream/source
import gleam/io

pub fn main() {
  source.from_list([
    Valid(1),
    Invalid(non_empty_list.single("row 2 bad")),
    Valid(3),
    Invalid(non_empty_list.single("row 4 bad")),
  ])
  |> fold.fold(from: Valid([]), with: combine)
  |> io.debug
  // Invalid(NonEmptyList("row 2 bad", ["row 4 bad"]))
}

fn combine(
  acc: Validated(List(Int), String),
  next: Validated(Int, String),
) -> Validated(List(Int), String) {
  case acc, next {
    Valid(xs), Valid(x) -> Valid([x, ..xs])
    Valid(_), Invalid(es) -> Invalid(es)
    Invalid(es), Valid(_) -> Invalid(es)
    Invalid(a), Invalid(b) -> Invalid(non_empty_list.append(a, b))
  }
}
```

For the simpler short-circuit case use `fold.collect_result` /
`fold.partition_result` on `Stream(Result(a, e))` instead — `Validated`
is for accumulating every error, not stopping on the first.

### Resource-backed stream

`source.resource` opens once on the first pull, calls `next` for each
element, and runs `close` exactly once on every termination path
(normal end, downstream early-exit via `take`, fold short-circuit,
`sink.try_each` failure).

```gleam
import datastream.{Done, Next}
import datastream/fold
import datastream/source
import datastream/stream
import gleam/io

pub fn main() {
  // A toy resource: a counter that yields 1, 2, 3 then halts.
  // `open` returns the initial state; `next` advances it; `close`
  // would release a real handle (file, socket, cursor).
  let stream =
    source.resource(
      open: fn() {
        io.println("open")
        1
      },
      next: fn(n) {
        case n > 3 {
          True -> Done
          False -> Next(element: n, state: n + 1)
        }
      },
      close: fn(_state) { io.println("close") },
    )

  // Take only the first element. `close` still runs because of the
  // early-exit contract.
  stream
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> io.debug
  // open
  // close
  // [1]
}
```

### Resource with fallible open

`source.try_resource` is the variant whose `open` and per-element `next`
can fail. A failed open emits exactly one `Error(source.OpenError(e))`
element and halts without calling `close`; per-element failures surface
as `Error(source.NextError(e))` and do not halt the stream.

```gleam
import datastream.{Done, Next}
import datastream/fold
import datastream/source
import gleam/io

pub fn main() {
  let stream =
    source.try_resource(
      open: fn() -> Result(Int, String) { Error("not available") },
      next: fn(n) {
        case n > 3 {
          True -> Done
          False -> Next(element: Ok(n), state: n + 1)
        }
      },
      close: fn(_state) { Nil },
    )

  stream
  |> fold.to_list
  |> io.debug
  // [Error(OpenError("not available"))]
}
```

### Line-by-line processing over a resource

The common real-world pipeline — open a file or cursor, stream lines,
filter, release the handle — composes `source.resource` with
`text.lines` and an early-exit terminal. The close contract releases
the handle on every termination path, so the example below releases
the pre-built "handle" regardless of how the downstream ends.

In production, `open` would return a real file handle, `next` would
call something like `file.read_line(handle)` (on Erlang) and wrap the
result in `Next(line, handle)` or `Done`, and `close` would release
the handle. The toy version below uses a list of chunks in place of
a real handle so the example runs on both targets.

```gleam
import datastream.{Done, Next}
import datastream/fold
import datastream/source
import datastream/stream
import datastream/text
import gleam/io

pub fn main() {
  source.resource(
    open: fn() { ["INFO hello\nWARN slow\n", "INFO bye\n"] },
    next: fn(state) {
      case state {
        [] -> Done
        [head, ..tail] -> Next(element: head, state: tail)
      }
    },
    close: fn(_state) { Nil },
  )
  |> text.lines
  |> stream.take(up_to: 2)
  |> fold.to_list
  |> io.debug
  // ["INFO hello", "WARN slow"]
}
```

### Bounded parallel map (BEAM)

```gleam
import datastream/erlang/par
import datastream/fold
import datastream/source
import datastream/stream
import gleam/io

pub fn main() {
  source.iterate(from: 1, with: fn(x) { x + 1 })
  |> par.map_unordered(with: fn(x) { x * x })
  |> stream.take(up_to: 5)
  |> fold.to_list
  |> io.debug
  // [1, 4, 9, 16, 25]   (or any permutation; map_unordered emits as workers finish)
}
```

For deterministic order use `par.map_ordered` (input order preserved
at the cost of a small reorder buffer). Tune concurrency with
`par.map_unordered_with(over:, with:, max_workers:, max_buffer:)`.

### Time-bucketed stream (BEAM)

```gleam
import datastream/chunk
import datastream/erlang/source as beam_source
import datastream/erlang/time
import datastream/fold
import datastream/stream
import gleam/io
import gleam/list

pub fn main() {
  // Tick every 20 ms; bucket arrivals into 60 ms windows; take the
  // first window's chunk.
  beam_source.ticks(every: 20)
  |> time.window_time(span: 60)
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> list.flat_map(chunk.to_list)
  |> list.length
  |> io.debug
  // 3   (approximately — three ticks land in one 60 ms window)
}
```

## Module guide

- `datastream`: defines `Stream(a)` and `Step(a, state)`
- `datastream/source`: constructors for streams and resources
- `datastream/stream`: lazy combinators and composition
- `datastream/fold`: pure terminal operations
- `datastream/sink`: effectful terminal operations
- `datastream/chunk`: opaque finite chunks
- `datastream/text`: chunk-aware text helpers
- `datastream/binary`: chunk-aware byte and framing helpers
- `datastream/erlang/source`: BEAM-only subject / timer sources and per-element `timeout`
- `datastream/erlang/sink`: BEAM-only subject sink
- `datastream/erlang/par`: BEAM-only bounded parallel combinators and `race`
- `datastream/erlang/time`: BEAM-only time-based combinators

## Semantics

- Streams are lazy: user callbacks run only when a fold or sink pulls the
  stream
- Streams are repeatable: running two terminals on the same stream reruns
  the source
- In the cross-target core, resource-backed sources are closed on normal
  completion and on early exit
- Errors are carried in the element type, for example
  `Stream(Result(a, e))`

## License

MIT — see [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and pull
request expectations. Bug reports and proposals via GitHub Issues.
