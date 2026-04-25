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
import gleam/bit_array
import gleam/list
import gleam/string

/// Split a stream of strings into a stream of lines, treating `\n`,
/// `\r\n`, and lone `\r` as terminators (matching Python's
/// `str.splitlines()`).
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
  lines_active(stream, "", [], False, False)
}

fn maybe_close(source: Stream(a), source_drained: Bool) -> Nil {
  case source_drained {
    True -> Nil
    False -> datastream.close(source)
  }
}

/// `pending` accumulates graphemes of the current line in reverse
/// order across chunk boundaries; `cr_pending` is True iff the
/// previous chunk ended with a lone `\r` whose `\r\n` vs `\r`
/// classification needs the next chunk to resolve.
fn lines_active(
  source: Stream(String),
  buffer: String,
  pending: List(String),
  cr_pending: Bool,
  source_drained: Bool,
) -> Stream(String) {
  datastream.make(
    pull: fn() {
      lines_pull(source, buffer, pending, cr_pending, source_drained)
    },
    close: fn() { maybe_close(source, source_drained) },
  )
}

fn lines_pull(
  source: Stream(String),
  buffer: String,
  pending: List(String),
  cr_pending: Bool,
  source_drained: Bool,
) -> Step(String, Stream(String)) {
  case scan_line(buffer, pending, cr_pending) {
    LineFound(line, rest) ->
      Next(line, lines_active(source, rest, [], False, source_drained))
    LineExhausted(new_pending, new_cr_pending) ->
      case source_drained {
        True ->
          case new_pending, new_cr_pending {
            [], False -> Done
            _, _ ->
              Next(
                reverse_concat(new_pending),
                lines_active(source, "", [], False, True),
              )
          }
        False ->
          case datastream.pull(source) {
            Next(chunk, source_rest) ->
              lines_pull(source_rest, chunk, new_pending, new_cr_pending, False)
            Done -> lines_pull(source, "", new_pending, new_cr_pending, True)
          }
      }
  }
}

type ScanResult {
  LineFound(line: String, remaining: String)
  LineExhausted(pending: List(String), cr_pending: Bool)
}

fn scan_line(
  buffer: String,
  pending: List(String),
  cr_pending: Bool,
) -> ScanResult {
  case cr_pending {
    True -> resume_cr(buffer, pending)
    False -> walk(buffer, pending)
  }
}

fn resume_cr(buffer: String, pending: List(String)) -> ScanResult {
  // The previous chunk ended with a lone `\r`. The line was already
  // terminated; we just need to consume an immediately following `\n`
  // so it isn't counted as a separate empty line.
  case string.pop_grapheme(buffer) {
    Ok(#("\n", rest_after)) -> LineFound(reverse_concat(pending), rest_after)
    Ok(_) -> LineFound(reverse_concat(pending), buffer)
    Error(_) -> LineExhausted(pending, True)
  }
}

fn walk(remaining: String, acc: List(String)) -> ScanResult {
  case string.pop_grapheme(remaining) {
    Error(_) -> LineExhausted(acc, False)
    Ok(#(g, rest)) ->
      case g {
        "\n" -> LineFound(reverse_concat(acc), rest)
        "\r\n" -> LineFound(reverse_concat(acc), rest)
        "\r" -> lookahead_cr(rest, acc)
        _ -> walk(rest, [g, ..acc])
      }
  }
}

fn lookahead_cr(rest: String, acc: List(String)) -> ScanResult {
  case string.pop_grapheme(rest) {
    Ok(#("\n", rest_after)) -> LineFound(reverse_concat(acc), rest_after)
    Ok(_) -> LineFound(reverse_concat(acc), rest)
    Error(_) -> LineExhausted(acc, True)
  }
}

fn reverse_concat(reversed_graphemes: List(String)) -> String {
  string.concat(list.reverse(reversed_graphemes))
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
    close: fn() { maybe_close(source, source_drained) },
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

// --- UTF-8 decode / encode ------------------------------------------------

/// Decode a stream of UTF-8 byte chunks into a stream of decoded
/// strings, reassembling multi-byte codepoints split across chunks.
///
/// Each successfully decoded codepoint surfaces as `Ok(string)`. An
/// invalid byte (lead byte that is not a valid UTF-8 start, or a
/// missing/invalid continuation) emits exactly one `Error(Nil)` and
/// decoding resumes from the next byte. An incomplete trailing
/// sequence at end-of-stream emits a final `Error(Nil)` before halting.
///
/// The decoder holds a small internal buffer for partial multi-byte
/// codepoints; it never materialises the full input.
pub fn utf8_decode(over stream: Stream(BitArray)) -> Stream(Result(String, Nil)) {
  utf8_decode_active(stream, <<>>, False)
}

fn utf8_decode_active(
  source: Stream(BitArray),
  buffer: BitArray,
  source_drained: Bool,
) -> Stream(Result(String, Nil)) {
  datastream.make(
    pull: fn() { utf8_decode_pull(source, buffer, source_drained) },
    close: fn() { maybe_close(source, source_drained) },
  )
}

fn utf8_decode_pull(
  source: Stream(BitArray),
  buffer: BitArray,
  source_drained: Bool,
) -> Step(Result(String, Nil), Stream(Result(String, Nil))) {
  case extract_run(buffer, source_drained) {
    RunOkAccum(bytes, remaining) ->
      case bit_array.to_string(bytes) {
        Ok(s) ->
          Next(Ok(s), utf8_decode_active(source, remaining, source_drained))
        Error(_) ->
          Next(
            Error(Nil),
            utf8_decode_active(source, remaining, source_drained),
          )
      }
    RunInvalid(remaining) ->
      Next(Error(Nil), utf8_decode_active(source, remaining, source_drained))
    RunNeedMore ->
      case datastream.pull(source) {
        Next(chunk, source_rest) ->
          utf8_decode_pull(source_rest, bit_array.append(buffer, chunk), False)
        Done -> utf8_decode_pull(source, buffer, True)
      }
    RunDone -> Done
  }
}

type Run {
  RunOkAccum(bytes: BitArray, remaining: BitArray)
  RunInvalid(remaining: BitArray)
  RunNeedMore
  RunDone
}

fn extract_run(buffer: BitArray, source_drained: Bool) -> Run {
  do_extract_run(buffer, <<>>, source_drained)
}

fn do_extract_run(
  buffer: BitArray,
  accumulated: BitArray,
  source_drained: Bool,
) -> Run {
  case extract_codepoint(buffer, source_drained) {
    DecodeOk(bytes, rest) ->
      do_extract_run(rest, bit_array.append(accumulated, bytes), source_drained)
    DecodeInvalid(rest) ->
      case bit_array.byte_size(accumulated) {
        0 -> RunInvalid(rest)
        _ -> RunOkAccum(accumulated, buffer)
      }
    DecodeNeedMore ->
      case bit_array.byte_size(accumulated) {
        0 -> RunNeedMore
        _ -> RunOkAccum(accumulated, buffer)
      }
    DecodeDone ->
      case bit_array.byte_size(accumulated) {
        0 -> RunDone
        _ -> RunOkAccum(accumulated, <<>>)
      }
  }
}

type Decoded {
  DecodeOk(bytes: BitArray, rest: BitArray)
  DecodeInvalid(rest: BitArray)
  DecodeNeedMore
  DecodeDone
}

fn extract_codepoint(buffer: BitArray, source_drained: Bool) -> Decoded {
  case buffer {
    <<>> ->
      case source_drained {
        True -> DecodeDone
        False -> DecodeNeedMore
      }
    <<lead:size(8), rest:bits>> ->
      classify_and_extract(lead, rest, source_drained)
    _ -> DecodeInvalid(<<>>)
  }
}

fn classify_and_extract(
  lead: Int,
  rest: BitArray,
  source_drained: Bool,
) -> Decoded {
  case lead {
    l if l < 0x80 -> DecodeOk(<<l>>, rest)
    l if l < 0xC2 -> DecodeInvalid(rest)
    l if l < 0xE0 -> collect_continuation(lead, <<>>, rest, 1, source_drained)
    l if l < 0xF0 -> collect_continuation(lead, <<>>, rest, 2, source_drained)
    l if l < 0xF5 -> collect_continuation(lead, <<>>, rest, 3, source_drained)
    _ -> DecodeInvalid(rest)
  }
}

fn collect_continuation(
  lead: Int,
  collected: BitArray,
  rest: BitArray,
  need: Int,
  source_drained: Bool,
) -> Decoded {
  case need {
    0 -> DecodeOk(bit_array.append(<<lead>>, collected), rest)
    _ -> consume_continuation(lead, collected, rest, need, source_drained)
  }
}

fn consume_continuation(
  lead: Int,
  collected: BitArray,
  rest: BitArray,
  need: Int,
  source_drained: Bool,
) -> Decoded {
  case rest {
    <<>> ->
      case source_drained {
        True -> DecodeInvalid(<<>>)
        False -> DecodeNeedMore
      }
    <<b:size(8), more:bits>> ->
      case b >= 0x80 && b < 0xC0 {
        True ->
          collect_continuation(
            lead,
            bit_array.append(collected, <<b>>),
            more,
            need - 1,
            source_drained,
          )
        False -> DecodeInvalid(rest)
      }
    _ -> DecodeInvalid(rest)
  }
}

/// Encode a stream of strings as UTF-8 byte chunks.
///
/// Each input string maps to its UTF-8 encoding as a single
/// `BitArray` element; an empty input string maps to the empty
/// `BitArray`.
pub fn utf8_encode(over stream: Stream(String)) -> Stream(BitArray) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(s, rest) -> Next(bit_array.from_string(s), utf8_encode(over: rest))
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}
