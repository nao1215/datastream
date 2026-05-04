# datastream

datastream is a pull-based stream library for Gleam.

A `Stream(a)` is a pipeline definition, not a materialized collection.
Each terminal operation runs the source again from the beginning, so the
library fits work that should stay lazy, repeatable, and explicit about
effects.

## Install

```sh
gleam add datastream
```

API reference: <https://hexdocs.pm/datastream>

## When to use it

Use `datastream` when you need one or more of these:

- the input is large or unbounded and should not be loaded all at once
- the pipeline owns a real resource such as a file handle, socket, or cursor
- the work is naturally chunked text or bytes
- the Erlang target needs bounded parallel work or time-based stream operators

For a hands-on tour, jump to the runnable
[Log-ingest example](#log-ingest-example-bytes--lines--per-level-counts)
or the [NDJSON example](#ndjson-example-chunked-bytes--typed-records).
The full catalog of compile-checked end-to-end pipelines lives under
[Example pipelines](#example-pipelines).

Stay with `gleam/list` when the whole input is already in memory and you
do not need lazy pulls, replayable pipelines, or deterministic cleanup.

## Quick start

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

## Log-ingest example: bytes -> lines -> per-level counts

A common shape: bytes arrive in arbitrary chunks (file, socket), the
pipeline reassembles them into lines, drops malformed rows, and produces
per-level counts in a single pass. `text.lines` owns the line buffering,
so the chunk boundaries shown below never split a record across two
emitted strings.

```gleam
import datastream/fold
import datastream/source
import datastream/stream
import datastream/text
import gleam/dict
import gleam/io
import gleam/option.{type Option, None, Some}
import gleam/string

pub type Level {
  Info
  Warn
  LogError
}

pub fn main() {
  // Real-world input arrives in arbitrary chunks. Note how `WARN` and
  // `INFO` records straddle chunk boundaries — `text.lines` reassembles.
  let chunks = [
    "INFO  user_id=42 logged in\nWARN  ", "user_id=42 retry\nERROR ",
    "user_id=99 timeout\nbogus line with no level\nINFO ",
    "user_id=42 ok\n",
  ]

  source.from_list(chunks)
  |> text.lines
  |> stream.filter_map(with: parse_line)
  |> fold.fold(from: dict.new(), with: bump)
  |> io.debug
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

Production callers swap `source.from_list(chunks)` for a
`source.resource` over a real file handle or socket — the rest of the
pipeline is unchanged. Full source:
[`test/examples/log_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/log_pipeline_example.gleam).

## NDJSON example: chunked bytes -> typed records

Newline-delimited JSON (one record per line) over a chunked byte source.
The same lazy pass does UTF-8 decode, line framing, and per-record
parsing without holding the whole payload in memory.

```gleam
import datastream/fold
import datastream/source
import datastream/stream
import datastream/text
import gleam/int
import gleam/io
import gleam/option.{type Option, None, Some}
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
  |> text.utf8_decode
  |> stream.filter_map(with: ok_to_option)
  |> text.lines
  |> stream.map(with: parse_record)
  |> fold.collect_result
  |> io.debug
  // Ok([Record(1, "first record"), Record(2, "second record"), ...])
}

fn ok_to_option(r: Result(String, Nil)) -> Option(String) {
  case r {
    Ok(s) -> Some(s)
    Error(Nil) -> None
  }
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

`fold.collect_result` short-circuits at the first parse failure; swap in
`fold.partition_result` to surface every successful record alongside
every error. Drop in your JSON decoder of choice at `parse_record`.
Full source:
[`test/examples/ndjson_pipeline_example.gleam`](https://github.com/nao1215/datastream/blob/main/test/examples/ndjson_pipeline_example.gleam).

## Resource-backed streams

`source.resource` opens lazily on the first pull, yields values one by
one, and closes exactly once on normal completion and on the early-stop
paths the library controls.

```gleam
import datastream.{Done, Next}
import datastream/fold
import datastream/source
import datastream/stream
import gleam/io

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
  |> io.debug
  // [1, 2]
}
```

Use `source.try_resource` when opening or reading can fail and you want
the failure on the typed path as `Result`.

## JavaScript async I/O

The core `Stream(a)` stays synchronous on both targets. That is a
deliberate design choice: each pull either returns the next element
immediately or reports `Done`.

Because of that, the official JavaScript boundary is:

- async input stays in host JavaScript until it has been reduced to a bounded value or batch, then enters `datastream` through `source.once`, `source.from_list`, `source.from_bit_array`, or a resource constructor
- synchronous `Stream(a)` pipelines leave `datastream` through `datastream/javascript/async.to_async_iterable`

This means `datastream` does not pretend that a host-side
`AsyncIterable` is the same thing as a pure `Stream(a)`. The adapter is
honest about where `await` lives.

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

Compile-checked examples live under `test/examples/` and run in CI. The
log-ingest and NDJSON entries are inlined above; the rest are linked
straight to source.

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
- `datastream/fold`: pure terminals such as `to_list`, `sum`, `first`, `find`, and `collect_result`
- `datastream/sink`: effectful terminals such as `each` and `try_each`
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

Some constructors reject invalid numeric arguments at construction time.
Use the panicking variants when the value is a trusted constant in your
own code. Use the matching `*_checked` variant when the value comes from
CLI flags, config files, request parameters, or any other dynamic input.

The main checked families are:

- `stream.take_checked`, `stream.drop_checked`
- `stream.buffer_checked`, `stream.chunks_of_checked`
- `binary.length_prefixed_checked`, `binary.length_prefixed_with_checked`, `binary.fixed_size_checked`
- `datastream/erlang/par.*_checked`

## Web framework compatibility

`datastream` depends on `gleam_erlang >= 1.3.0` and
`gleam_stdlib >= 0.44.0`. Older releases of some web packages pin
`gleam_erlang < 1.0.0`, which conflicts with `datastream`.

Use these versions or newer when combining `datastream` with a web stack:

| Package | Minimum compatible version |
| --- | --- |
| `wisp` | `>= 2.0.0` |
| `mist` | `>= 6.0.0` |
| `gleam_httpc` | `>= 5.0.0` |
