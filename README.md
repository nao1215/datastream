# datastream

[![CI](https://github.com/nao1215/datastream/actions/workflows/ci.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/ci.yml)
[![Gitleaks](https://github.com/nao1215/datastream/actions/workflows/gitleaks.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/gitleaks.yml)
[![Trivy](https://github.com/nao1215/datastream/actions/workflows/trivy.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/trivy.yml)
[![OSV-Scanner](https://github.com/nao1215/datastream/actions/workflows/osv-scan.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/osv-scan.yml)
[![Hex](https://img.shields.io/hexpm/v/datastream)](https://hex.pm/packages/datastream)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/datastream/)
[![Hex Downloads](https://img.shields.io/hexpm/dt/datastream)](https://hex.pm/packages/datastream)
[![License](https://img.shields.io/github/license/nao1215/datastream)](./LICENSE)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/nao1215/datastream/badge)](https://scorecard.dev/viewer/?uri=github.com/nao1215/datastream)

datastream is a pull-based stream library for Gleam.

A `Stream(a)` describes a pipeline. Nothing runs until a terminal
operation pulls from it, and each terminal operation runs the source again
from the beginning.

Use it when the input is too large to load at once, when the pipeline
owns a file handle, socket, or cursor, when the data arrives as chunked
text or bytes, or when you need bounded parallel or time-based operators on
Erlang. If the whole input is already in memory, `gleam/list` is enough.

## Install

```sh
gleam add datastream
```

API reference: <https://hexdocs.pm/datastream>

## Quick start

```gleam
import datastream/sink
import datastream/source
import datastream/stream

pub fn main() {
  let result =
    source.iterate(from: 1, with: fn(x) { x + 1 })
    |> stream.map(with: fn(x) { x * 2 })
    |> stream.take(up_to: 5)
    |> sink.to_list

  echo result
  // [2, 4, 6, 8, 10]
}
```

## Log-ingest example: bytes -> lines -> per-level counts

Chunks from a file or socket are split into lines, malformed rows are
dropped, and levels are counted in one pass. `text.lines` buffers partial
lines, so a record cut across two chunks still comes out as one string.

```gleam
import datastream/fold
import datastream/source
import datastream/stream
import datastream/text
import gleam/dict
import gleam/option.{type Option, None, Some}
import gleam/string

pub type Level {
  Info
  Warn
  LogError
}

pub fn main() {
  // Input arrives in arbitrary chunks. The `WARN` and `INFO` records
  // straddle chunk boundaries and `text.lines` reassembles them.
  let chunks = [
    "INFO  user_id=42 logged in\nWARN  ", "user_id=42 retry\nERROR ",
    "user_id=99 timeout\nbogus line with no level\nINFO ",
    "user_id=42 ok\n",
  ]

  source.from_list(chunks)
  |> text.lines
  |> stream.filter_map(with: parse_line)
  |> fold.fold(from: dict.new(), with: bump)
  |> echo
  // dict.from_list([#(Info, 2), #(Warn, 1), #(LogError, 1)])
}

fn parse_line(line: String) -> Option(Level) {
  case string.split_once(line, on: " ") {
    Ok(#("INFO", _)) -> Some(Info)
    Ok(#("WARN", _)) -> Some(Warn)
    Ok(#("ERROR", _)) -> Some(LogError)
    _ -> None
  }
}

fn bump(acc: dict.Dict(Level, Int), level: Level) -> dict.Dict(Level, Int) {
  let current = case dict.get(acc, level) {
    Ok(n) -> n
    Error(Nil) -> 0
  }
  dict.insert(acc, level, current + 1)
}
```

With a real file or socket, replace `source.from_list(chunks)` with
`source.resource`; the rest of the pipeline stays the same. Full source:
[`test/examples/log_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/log_pipeline_example.gleam).

## NDJSON example: chunked bytes -> typed records

One lazy pass decodes UTF-8, splits lines, and parses each record without
holding the whole payload in memory.

```gleam
import datastream/fold
import datastream/source
import datastream/stream
import datastream/text
import gleam/int
import gleam/string

pub type Record {
  Record(id: Int, body: String)
}

pub type ParseError {
  EmptyLine
  MissingId(line: String)
  BadId(raw: String)
}

pub fn main() {
  // The first chunk ends mid-line; the second completes that record.
  let chunks =
    source.from_list([
      <<"1 first record\n2 second">>,
      <<" record\n3 third record\n">>,
    ])

  chunks
  |> text.utf8_decode_lossy
  |> text.lines
  |> stream.map(with: parse_record)
  |> fold.collect_result
  |> echo
  // Ok([Record(1, "first record"), Record(2, "second record"), ...])
}

fn parse_record(line: String) -> Result(Record, ParseError) {
  case line {
    "" -> Error(EmptyLine)
    _ ->
      case string.split_once(line, on: " ") {
        Error(_) -> Error(MissingId(line: line))
        Ok(#(raw_id, body)) ->
          case int.parse(raw_id) {
            Ok(id) -> Ok(Record(id: id, body: body))
            Error(_) -> Error(BadId(raw: raw_id))
          }
      }
  }
}
```

`fold.collect_result` stops at the first parse failure. `fold.partition_result`
returns all successes and all errors instead. Put your JSON decoder in
`parse_record`. Full source:
[`test/examples/ndjson_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/ndjson_pipeline_example.gleam).

## Resource-backed streams

`source.resource` opens on the first pull and closes exactly once, both
when the source finishes and when the library stops early (here, `take`).

```gleam
import datastream.{Done, Next}
import datastream/fold
import datastream/source
import datastream/stream

pub fn main() {
  let numbers =
    source.resource(
      open: fn() { 1 },
      next: fn(state) {
        case state <= 3 {
          True -> Next(element: state, state: state + 1)
          False -> Done
        }
      },
      close: fn(_state) { Nil },
    )

  numbers
  |> stream.take(up_to: 2)
  |> fold.to_list
  |> echo
  // [1, 2]
}
```

Use `source.try_resource` when opening or reading can fail and you want
the failure on the typed path as `Result`.

## JavaScript async I/O

`Stream(a)` is synchronous on both targets: each pull returns the next
element or `Done` right away. On JavaScript, await async input in host code
first, then pass the bounded value or batch in through `source.once`,
`source.from_list`, `source.from_bit_array`, or a resource constructor. To
hand a pipeline back to host code, use
`datastream/javascript/async.to_async_iterable`.

Gleam:

```gleam
import datastream/javascript/async as js_async
import datastream/source
import datastream/stream
import gleam/string

pub fn lines_for_host() -> js_async.AsyncIterable(String) {
  source.from_list(["a", "b", "c"])
  |> stream.map(with: string.uppercase)
  |> js_async.to_async_iterable
}
```

Host JavaScript:

```js
import { lines_for_host } from "./build/dev/javascript/app/app.mjs";

for await (const line of lines_for_host()) {
  console.log(line);
  if (line === "B") break;
}
```

Breaking out of the `for await` loop closes the underlying stream once,
so resource-backed pipelines still release handles promptly.

## Example pipelines

These run in CI from `test/examples/`.

| Example | Shape |
| --- | --- |
| [`log_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/log_pipeline_example.gleam) | bytes -> `text.lines` -> validation -> per-level counts |
| [`ndjson_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/ndjson_pipeline_example.gleam) | bytes -> UTF-8 decode -> lines -> per-record parse |
| [`length_prefixed_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/length_prefixed_pipeline_example.gleam) | chunked bytes -> `binary.length_prefixed_with` -> collection |
| [`parallel_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/parallel_pipeline_example.gleam) | BEAM-only bounded parallel map |
| [`dataprep_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/dataprep_pipeline_example.gleam) | per-row validation with accumulated errors |

## Module guide

- `datastream`: defines `Stream(a)` and `Step(a, state)`
- `datastream/source`: constructors for list-backed, generated, and resource-backed streams
- `datastream/stream`: lazy combinators such as `map`, `filter`, `flat_map`, `zip`, `take`, and `chunks_of`
- `datastream/sink`: all terminals, both pure reductions (`to_list`, `count`, `fold`, `first`, `find`, `collect_result`, ...) and side-effecting consumers (`each`, `try_each`, `println`)
- `datastream/fold`: legacy alias for the pure reductions in `sink`; use `datastream/sink` in new code
- `datastream/chunk`: opaque finite batches
- `datastream/text`: chunk-aware UTF-8 decode and line splitting
- `datastream/binary`: byte and framing helpers
- `datastream/erlang/source`: BEAM-only subject, timer, and timeout helpers
- `datastream/erlang/sink`: BEAM-only subject sink
- `datastream/erlang/par`: BEAM-only bounded parallel combinators and `race`
- `datastream/erlang/time`: BEAM-only time-based combinators
- `datastream/javascript/async`: JavaScript-only async iterable adapter for leaving the synchronous core

## Target support

- Erlang target: the full package
- JavaScript target: the cross-target core and `datastream/javascript/async`
- `datastream/erlang/*` modules are BEAM-only

## Checked constructors

Constructors panic on invalid numeric arguments such as a negative `take`.
When the value comes from user input or config, use the `*_checked`
variant, which returns a `Result`:

- `stream.take_checked`, `stream.drop_checked`
- `stream.buffer_checked`, `stream.chunks_of_checked`
- `binary.length_prefixed_checked`, `binary.length_prefixed_with_checked`, `binary.fixed_size_checked`
- `datastream/erlang/par.*_checked`

## Backpressure

- `stream.buffer(stream, prefetch:)` reads up to `prefetch` elements ahead of one consumer.
- `stream.broadcast(stream, n)` fans out to `n` consumers with unbounded per-consumer queues. A stalled consumer on an unbounded source (`source.iterate`, `source.repeat`) grows memory without limit.
- `stream.broadcast_bounded(stream, n, max_queue:)` panics when a queue exceeds `max_queue`, so a stalled consumer crashes instead of running out of memory. Prefer it for unbounded or production fan-outs.

## Web framework compatibility

`datastream` depends on `gleam_erlang >= 1.3.0` and
`gleam_stdlib >= 0.44.0`. Older releases of some web packages pin
`gleam_erlang < 1.0.0`, which conflicts with `datastream`.

Use at least these versions with a web stack:

| Package | Minimum compatible version |
| --- | --- |
| `wisp` | `>= 2.0.0` |
| `mist` | `>= 6.0.0` |
| `gleam_httpc` | `>= 5.0.0` |

## Testing

| Technique | Bug class caught here |
|---|---|
| Examples and unit tests | [Wrong elements, resource leaks, broken argument checks](doc/testing.md#examples-and-unit-tests) |
| Generated properties | [Operators that disagree with gleam/list and gleam/string](doc/testing.md#generated-properties) |
| Metamorphic relations | [Parsers whose output depends on where the input was cut into chunks](doc/testing.md#metamorphic-relations) |
| Mutation testing | [Assertions that miss a changed operator or branch](doc/testing.md#mutation-testing) |

## Verifying releases

Each GitHub Release attaches the Hex package tarball (`datastream-<version>.tar`),
`checksums.txt`, a cosign signature bundle (`checksums.txt.sigstore.json`) and
build provenance (`datastream-<version>.intoto.jsonl`). Download the assets, then:

```sh
gh attestation verify datastream-<version>.tar --repo nao1215/datastream

cosign verify-blob \
  --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/nao1215/datastream/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt
sha256sum --check checksums.txt
```
