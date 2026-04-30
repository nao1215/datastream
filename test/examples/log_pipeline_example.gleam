//// End-to-end pipeline #1: file-shaped input → text lines →
//// per-line validation → aggregation.
////
//// Models a log-ingestion pass: bytes arrive from a file or socket
//// in arbitrary chunks, the pipeline reassembles them into lines,
//// rejects malformed lines, and aggregates the per-level counts in a
//// single pass.
////
//// Cross-target: every combinator used here runs on both the Erlang
//// and JavaScript targets.

import datastream/fold
import datastream/source
import datastream/stream
import datastream/text
import gleam/dict.{type Dict}
import gleam/option.{type Option, None, Some}
import gleam/string
import gleeunit/should

pub fn main() -> Nil {
  Nil
}

// `LogError` rather than `Error` to avoid shadowing `Result.Error`
// at every call site.
pub type Level {
  Info
  Warn
  LogError
}

/// Counts of each `Level` observed in a log stream. The dictionary is
/// the natural shape for "k buckets, count each" — callers can read
/// individual buckets with `dict.get` or fold over the whole map.
pub type LevelCounts =
  Dict(Level, Int)

/// Run the example pipeline against in-memory test fixtures and
/// return the per-level counts. The shape mirrors what production
/// code would write — only the source differs (real callers wire a
/// `source.resource` over a file handle or socket).
pub fn run() -> LevelCounts {
  // Real-world input arrives in arbitrary chunks. `text.lines` owns
  // the line-buffering, so callers never see a partial line.
  let chunks = [
    "INFO  user_id=42 logged in\nWARN  ", "user_id=42 retry\nERROR ",
    "user_id=99 timeout\nbogus line with no level\nINFO ", "user_id=42 ok\n",
  ]

  source.from_list(chunks)
  |> text.lines
  |> stream.filter_map(with: parse_line)
  |> fold.fold(from: dict.new(), with: bump)
}

fn bump(acc: LevelCounts, level: Level) -> LevelCounts {
  let current = case dict.get(acc, level) {
    Ok(n) -> n
    Error(Nil) -> 0
  }
  dict.insert(acc, level, current + 1)
}

fn parse_line(line: String) -> Option(Level) {
  // Lines that do not start with a recognised level are skipped via
  // `None`; `filter_map` drops them without emitting an element.
  case string.split_once(line, on: " ") {
    Ok(#("INFO", _)) -> Some(Info)
    Ok(#("WARN", _)) -> Some(Warn)
    Ok(#("ERROR", _)) -> Some(LogError)
    _ -> None
  }
}

pub fn log_pipeline_example_test() {
  let counts = run()
  dict.get(counts, Info) |> should.equal(Ok(2))
  dict.get(counts, Warn) |> should.equal(Ok(1))
  dict.get(counts, LogError) |> should.equal(Ok(1))
  // The "bogus line with no level" entry is dropped, not counted.
  dict.size(counts) |> should.equal(3)
}
