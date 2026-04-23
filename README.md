# datastream

[![CI](https://github.com/nao1215/datastream/actions/workflows/ci.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/ci.yml)
[![Hex Package](https://img.shields.io/hexpm/v/datastream.svg)](https://hex.pm/packages/datastream)

`datastream` is a compositional, resource-safe stream library for
[Gleam](https://gleam.run). It treats values as a lazily-evaluated
pipeline that is generated, transformed, and consumed through small,
composable functions — similar in spirit to widely-known stream
abstractions but designed around Gleam's type system and runtime
constraints.

## Status

**Pre-release.** v0.1.0 is in active development. The full
specification lives at [`doc/reference/spec.md`](./doc/reference/spec.md)
and the implementation roadmap is tracked in
[issue #1](https://github.com/nao1215/datastream/issues/1).

## Design highlights

- **Serial, lazy, pull-based core.** Building a pipeline triggers no
  user callbacks until a terminal operation runs.
- **Resource safety from day one.** `source.resource` /
  `source.try_resource` carry an explicit close contract that holds
  through composition combinators (`flat_map`, `append`, `concat`,
  `zip`) and through early-exit reducers (`take`, `first`, `find`,
  `try_each`).
- **Errors travel as elements, not as a side channel.** The library
  does not impose its own error type — callers carry `Result(a, e)`,
  `Validated(a, e)`, or any user-defined sum type.
- **Module-level responsibility separation.** Generation lives in
  `source`, middle transforms in `stream`, pure reductions in `fold`,
  effectful consumers in `sink`, chunked values in `chunk`, text
  operations in `text`, binary framing in `binary`.
- **Concurrency and time are quarantined to the BEAM target.** The
  cross-target core stays serial and clock-blind. Bounded-concurrency
  combinators (`erlang/par`) and time-based combinators (`erlang/time`)
  live in dedicated extension modules.

## Target support

The library compiles for both runtimes Gleam targets, but the surface
available on each differs:

| Module | Erlang target | JavaScript target |
| --- | --- | --- |
| `datastream` | ✅ | ✅ |
| `datastream/source` | ✅ | ✅ |
| `datastream/stream` | ✅ | ✅ |
| `datastream/fold` | ✅ | ✅ |
| `datastream/sink` | ✅ | ✅ |
| `datastream/chunk` | ✅ | ✅ |
| `datastream/text` | ✅ | ✅ |
| `datastream/binary` | ✅ | ✅ |
| `datastream/dataprep` | ✅ | ✅ |
| `datastream/erlang/source` | ✅ | — |
| `datastream/erlang/sink` | ✅ | — |
| `datastream/erlang/par` | ✅ | — |
| `datastream/erlang/time` | ✅ | — |

Streaming I/O on the JavaScript target is intentionally not part of
v0.1.0. JavaScript callers are expected to perform I/O synchronously
up front (read the file, fetch the response body, etc.) and then
construct a stream from the resulting `BitArray`, `String`, or `List`.
See [`doc/reference/spec.md`](./doc/reference/spec.md) for the full
rationale.

## Installation

```console
gleam add datastream
```

(Available once v0.1.0 is published to Hex.)

## Quick start

The example below will work as soon as Phase 1 lands. Until then it is
illustrative only.

```gleam
import datastream/fold
import datastream/source
import datastream/stream

pub fn main() {
  source.from_list([1, 2, 3, 4])
  |> stream.map(fn(x) { x * 2 })
  |> stream.filter(fn(x) { x > 4 })
  |> fold.to_list
  // => [6, 8]
}
```

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the local setup and the
test commands. The short version:

```console
mise install
gleam deps download
just ci
```

## License

[MIT](./LICENSE) © CHIKAMATSU Naohiro
