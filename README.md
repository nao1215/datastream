# datastream

[![CI](https://github.com/nao1215/datastream/actions/workflows/ci.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/ci.yml)
[![Gitleaks](https://github.com/nao1215/datastream/actions/workflows/gitleaks.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/gitleaks.yml)
[![Trivy](https://github.com/nao1215/datastream/actions/workflows/trivy.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/trivy.yml)
[![OSV-Scanner](https://github.com/nao1215/datastream/actions/workflows/osv-scan.yml/badge.svg)](https://github.com/nao1215/datastream/actions/workflows/osv-scan.yml)
[![Hex](https://img.shields.io/hexpm/v/datastream)](https://hex.pm/packages/datastream)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/datastream/)
[![Hex Downloads](https://img.shields.io/hexpm/dt/datastream)](https://hex.pm/packages/datastream)
[![License](https://img.shields.io/github/license/nao1215/datastream)](https://github.com/nao1215/datastream/blob/main/LICENSE)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/nao1215/datastream/badge)](https://scorecard.dev/viewer/?uri=github.com/nao1215/datastream)

datastream processes streams of values, text, and bytes in Gleam.

Use it to read input a piece at a time and run cleanup callbacks, such as closing a file, when a pipeline finishes or stops early. `gleam/list` works with a list already in memory; a datastream pipeline requests each next element as it runs. The synchronous core runs on Erlang and JavaScript. Parallel mapping and timers require Erlang.

## Installation

```sh
gleam add datastream@0
```

Requires Gleam 1.14 or later.

## Usage

```gleam
import datastream
import gleam/io
import gleam/string

pub fn main() {
  datastream.iterate(from: 1, with: fn(x) { x + 1 })
  |> datastream.map(with: fn(x) { x * 2 })
  |> datastream.take(up_to: 5)
  |> datastream.to_list
  |> string.inspect
  |> io.println
}
```

This prints `[2, 4, 6, 8, 10]`. [`resource`](https://hexdocs.pm/datastream/datastream.html#resource) connects a pipeline to a file, socket, or cursor using open, next, and close callbacks. It opens on the first pull and closes once on completion, early termination, or when a callback raises. A pipeline that is never pulled opens nothing.

The API uses one import, `datastream`. See [HexDocs](https://hexdocs.pm/datastream/datastream.html) for construction, transformations, terminals, text parsing, and binary framing.

## Examples

The [example pipelines](https://github.com/nao1215/datastream/tree/main/test/examples) run in the test suite:

- [Count log levels](https://github.com/nao1215/datastream/blob/main/test/examples/log_pipeline_example.gleam) from text split across chunks.
- [Decode and parse records](https://github.com/nao1215/datastream/blob/main/test/examples/ndjson_pipeline_example.gleam) from a byte stream.
- [Read length-prefixed frames](https://github.com/nao1215/datastream/blob/main/test/examples/length_prefixed_pipeline_example.gleam).
- [Map with parallel workers](https://github.com/nao1215/datastream/blob/main/test/examples/parallel_pipeline_example.gleam) on Erlang.
- [Validate rows with dataprep](https://github.com/nao1215/datastream/blob/main/test/examples/dataprep_pipeline_example.gleam).

## Resource and target behavior

Each terminal operation starts a fresh run of an ordinary pipeline. Consumers returned by `broadcast`, `broadcast_bounded`, or `unzip` share one upstream and are single-use. On Erlang, consume them in the process that created them. An unbounded broadcast retains unread values for slow consumers; use `broadcast_bounded` to report a queue overflow and close the upstream.

Text and binary parsers retain unfinished records between chunks. Memory use depends on the largest unfinished record. Length-prefixed framing accepts a frame-size limit.

On JavaScript, `to_async_iterable` exports a synchronous pipeline for a host `for await` loop. Breaking out of the loop closes the stream. The [JavaScript adapter guide](https://hexdocs.pm/datastream/javascript.html) describes the adapter and its target-specific API.

## Contributing

See [CONTRIBUTING.md](https://github.com/nao1215/datastream/blob/main/CONTRIBUTING.md) for setup and release verification, and [the testing guide](https://github.com/nao1215/datastream/blob/main/doc/testing.md) for properties and chunk-boundary checks.

## License

[MIT](https://github.com/nao1215/datastream/blob/main/LICENSE).
