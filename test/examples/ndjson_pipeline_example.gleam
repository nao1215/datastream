//// End-to-end pipeline #3: chunked byte source → UTF-8 decode →
//// line-delimited records → typed objects.
////
//// Models an NDJSON (newline-delimited JSON) payload streaming in
//// from a socket or HTTP body. Each line is one JSON record. The
//// pipeline does the byte-to-text decode, the line framing, and the
//// per-record parse in one lazy pass — the whole payload never has
//// to fit in memory at once.
////
//// `gleam_json` is intentionally NOT pulled in: real callers can
//// drop in their JSON decoder of choice at the `parse_record` step.
//// The example uses a small hand-written parser so it stays
//// self-contained.
////
//// Cross-target: `text.utf8_decode`, `text.lines`, and
//// `fold.collect_result` run on both the Erlang and JavaScript
//// targets.

import datastream/fold
import datastream/source
import datastream/stream
import datastream/text
import gleam/int
import gleam/string
import gleeunit/should

pub fn main() -> Nil {
  Nil
}

/// Decoded record shape. `id` is the integer prefix, `body` is the
/// remaining text after the first space — enough to demonstrate a
/// per-line parse without leaning on a full JSON decoder.
pub type Record {
  Record(id: Int, body: String)
}

pub type ParseError {
  EmptyLine
  MissingId(line: String)
  BadId(raw: String)
}

/// Run the example pipeline against a chunked byte source and return
/// either the decoded records or the first parse failure.
pub fn run() -> Result(List(Record), ParseError) {
  // Bytes arrive in arbitrary chunks. The first chunk ends mid-line,
  // the second chunk completes that line and begins another.
  let chunks =
    source.from_list([
      <<"1 first record\n2 second">>,
      <<" record\n3 third record\n">>,
    ])

  chunks
  // BitArray → Stream(String), dropping any UTF-8 decode errors.
  // Production code that needs to surface those errors should use
  // `text.utf8_decode` and route the resulting `Stream(Result(...))`
  // via `fold.partition_result` instead.
  |> text.utf8_decode_lossy
  // Reassemble lines (handles the chunk-spanning record).
  |> text.lines
  // String → Result(Record, ParseError).
  |> stream.map(with: parse_record)
  // Stop at the first parse failure.
  |> fold.collect_result
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

pub fn ndjson_pipeline_example_test() {
  run()
  |> should.equal(
    Ok([
      Record(id: 1, body: "first record"),
      Record(id: 2, body: "second record"),
      Record(id: 3, body: "third record"),
    ]),
  )
}
