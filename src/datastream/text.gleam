//// Chunk-boundary-aware text stream operations on `Stream(String)`.
////
//// Real text inputs arrive in arbitrary chunks (file reads, network
//// reads). A naïve `string.split` per chunk is wrong because line
//// terminators and delimiters land on chunk boundaries. The functions
//// here absorb that with a small, bounded amount of internal buffering
//// — they never materialise the full input.
////
//// Both functions consume `Stream(String)` rather than
//// `Stream(BitArray)`. The byte → string boundary is owned by
//// `text.utf8_decode` (separate issue) so the cost lives in one place.

import datastream.{type Step, type Stream, Done, Next}
import gleam/option.{type Option, None, Some}
import gleam/string

/// Split a stream of strings into a stream of lines, treating both
/// `\n` and `\r\n` as terminators.
///
/// The terminator is NOT included in emitted lines. A trailing partial
/// line (input that does not end with a terminator) is emitted as a
/// final element so callers do not silently drop the last record.
///
/// Chunk-boundary handling: when a `\r` lands at the end of one chunk
/// and the next chunk starts with `\n`, the pair is treated as a
/// single `\r\n` boundary. A lone `\r` with no following `\n` (either
/// because the next chunk does not start with `\n`, or because the
/// stream ended) is treated as a line terminator.
pub fn lines(over stream: Stream(String)) -> Stream(String) {
  lines_active(stream, "", False)
}

fn lines_active(
  source: Stream(String),
  buffer: String,
  source_drained: Bool,
) -> Stream(String) {
  datastream.make(
    pull: fn() { lines_pull(source, buffer, source_drained) },
    close: fn() {
      case source_drained {
        True -> Nil
        False -> datastream.close(source)
      }
    },
  )
}

fn lines_pull(
  source: Stream(String),
  buffer: String,
  source_drained: Bool,
) -> Step(String, Stream(String)) {
  case extract_line(buffer, "", source_drained) {
    Some(#(line, rest)) ->
      Next(line, lines_active(source, rest, source_drained))
    None ->
      case source_drained {
        True ->
          case buffer {
            "" -> Done
            _ -> Next(buffer, lines_active(source, "", True))
          }
        False ->
          case datastream.pull(source) {
            Next(chunk, source_rest) ->
              lines_pull(source_rest, buffer <> chunk, False)
            Done -> lines_pull(source, buffer, True)
          }
      }
  }
}

fn extract_line(
  remaining: String,
  acc: String,
  source_drained: Bool,
) -> Option(#(String, String)) {
  case string.pop_grapheme(remaining) {
    Error(_) -> None
    Ok(#(g, rest)) ->
      case g {
        "\n" -> Some(#(acc, rest))
        "\r\n" -> Some(#(acc, rest))
        "\r" -> handle_cr(rest, acc, source_drained)
        _ -> extract_line(rest, acc <> g, source_drained)
      }
  }
}

fn handle_cr(
  rest: String,
  acc: String,
  source_drained: Bool,
) -> Option(#(String, String)) {
  case string.pop_grapheme(rest) {
    Ok(#("\n", rest_after)) -> Some(#(acc, rest_after))
    Ok(_) -> Some(#(acc, rest))
    Error(_) ->
      case source_drained {
        True -> Some(#(acc, ""))
        False -> None
      }
  }
}

/// Split a stream of strings on `delimiter`, emitting every separated
/// piece including the empty pieces between consecutive delimiters and
/// at the start / end of the input.
///
/// `delimiter == ""` triggers the grapheme-cluster splitter: each
/// emitted element is exactly one Unicode grapheme cluster of the
/// input, in source order.
///
/// Chunk-boundary handling: pieces that span a chunk boundary are
/// joined; the trailing in-flight piece is held in a small buffer and
/// emitted only when a delimiter or end-of-stream is reached.
pub fn split(
  over stream: Stream(String),
  on delimiter: String,
) -> Stream(String) {
  case delimiter {
    "" -> graphemes_active(stream, [], False)
    _ -> split_active(stream, "", delimiter, False)
  }
}

fn split_active(
  source: Stream(String),
  buffer: String,
  delimiter: String,
  has_seen_input: Bool,
) -> Stream(String) {
  datastream.make(
    pull: fn() { split_pull(source, buffer, delimiter, has_seen_input) },
    close: fn() { datastream.close(source) },
  )
}

fn split_pull(
  source: Stream(String),
  buffer: String,
  delimiter: String,
  has_seen_input: Bool,
) -> Step(String, Stream(String)) {
  case string.split_once(buffer, on: delimiter) {
    Ok(#(before, after)) ->
      Next(before, split_active(source, after, delimiter, True))
    Error(_) ->
      case datastream.pull(source) {
        Next(chunk, source_rest) ->
          split_pull(source_rest, buffer <> chunk, delimiter, True)
        Done ->
          case has_seen_input {
            True -> Next(buffer, split_drained())
            False -> Done
          }
      }
  }
}

fn split_drained() -> Stream(String) {
  datastream.make(pull: fn() { Done }, close: fn() { Nil })
}

fn graphemes_active(
  source: Stream(String),
  pending: List(String),
  source_drained: Bool,
) -> Stream(String) {
  datastream.make(
    pull: fn() { graphemes_pull(source, pending, source_drained) },
    close: fn() {
      case source_drained {
        True -> Nil
        False -> datastream.close(source)
      }
    },
  )
}

fn graphemes_pull(
  source: Stream(String),
  pending: List(String),
  source_drained: Bool,
) -> Step(String, Stream(String)) {
  case pending {
    [grapheme, ..rest] ->
      Next(grapheme, graphemes_active(source, rest, source_drained))
    [] ->
      case source_drained {
        True -> Done
        False ->
          case datastream.pull(source) {
            Done -> Done
            Next(chunk, source_rest) ->
              graphemes_pull(source_rest, string.to_graphemes(chunk), False)
          }
      }
  }
}
