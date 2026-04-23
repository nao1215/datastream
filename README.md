# datastream

`datastream` is a pull-based stream library for Gleam.

It is meant for pipelines that should stay lazy, repeatable, and explicit
about effects. A `Stream(a)` is a pipeline definition, not a materialized
collection, so each terminal operation runs the source again.

## Status

Pre-1.0. The public API may change between any 0.x release. Pin a
specific version in your `gleam.toml` and review the [CHANGELOG](CHANGELOG.md)
before upgrading.

## Install

```sh
gleam add datastream
```

API reference: <https://hexdocs.pm/datastream>

## Use cases

- Build pipelines from lists, ranges, options, results, or custom state
- Transform infinite or finite streams with `map`, `filter`, `take`, and
  `flat_map`
- Process chunked text or bytes without joining the whole input first
- Wrap your own synchronous resources with `source.resource` and
  `source.try_resource`
- On Erlang, work with subjects, timers, and bounded parallelism through
  `datastream/erlang/*`

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
  // [<<65, 66>>, <<67>>]
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

## Module guide

- `datastream`: defines `Stream(a)` and `Step(a, state)`
- `datastream/source`: constructors for streams and resources
- `datastream/stream`: lazy combinators and composition
- `datastream/fold`: pure terminal operations
- `datastream/sink`: effectful terminal operations
- `datastream/chunk`: opaque finite chunks
- `datastream/text`: chunk-aware text helpers
- `datastream/binary`: chunk-aware byte and framing helpers
- `datastream/dataprep`: helpers for `Validated`
- `datastream/erlang/source`: BEAM-only subject / timer sources and per-element `timeout`
- `datastream/erlang/sink`: BEAM-only subject sink
- `datastream/erlang/par`: BEAM-only bounded parallel combinators and `race`
- `datastream/erlang/time`: BEAM-only time-based combinators

## Target support

- Erlang target: every module in this package
- JavaScript target: the cross-target core only
- `datastream/erlang/*` modules are BEAM-only
- On JavaScript, `datastream` does not provide async streaming I/O
  adapters. Resolve async I/O outside the library, then feed the data into
  the core with constructors such as `source.from_list`,
  `source.from_bit_array`, or `source.once`

## Semantics

- Streams are lazy: user callbacks run only when a fold or sink pulls the
  stream
- Streams are repeatable: running two terminals on the same stream reruns
  the source
- In the cross-target core, resource-backed sources are closed on normal
  completion and on early exit
- Errors are carried in the element type, for example
  `Stream(Result(a, e))`
