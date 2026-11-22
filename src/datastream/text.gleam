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
  lines_active(LinesState(
    source: stream,
    ready: [],
    partial: [],
    after_cr: False,
    source_drained: False,
  ))
}

fn maybe_close(source: Stream(a), source_drained: Bool) -> Nil {
  case source_drained {
    True -> Nil
    False -> datastream.close(source)
  }
}

// `ready` holds complete lines not yet emitted, in order. `partial`
// holds the pieces of the current unterminated line in reverse, so a
// line spread over many chunks is joined once. `after_cr` is True when
// the text so far ends with a `\r` that already ended a line, so a
// `\n` at the start of the next chunk belongs to that `\r\n`.
type LinesState {
  LinesState(
    source: Stream(String),
    ready: List(String),
    partial: List(String),
    after_cr: Bool,
    source_drained: Bool,
  )
}

fn lines_active(state: LinesState) -> Stream(String) {
  datastream.make(pull: fn() { lines_pull(state) }, close: fn() {
    maybe_close(state.source, state.source_drained)
  })
}

fn lines_pull(state: LinesState) -> Step(String, Stream(String)) {
  case state.ready, state.source_drained {
    [line, ..ready], _ -> Next(line, lines_active(LinesState(..state, ready:)))
    [], True ->
      case state.partial {
        [] -> Done
        partial ->
          Next(
            reverse_concat(partial),
            lines_active(LinesState(..state, partial: [])),
          )
      }
    [], False ->
      case datastream.pull(state.source) {
        Done -> lines_pull(LinesState(..state, source_drained: True))
        Next("", source) -> lines_pull(LinesState(..state, source:))
        Next(chunk, source) ->
          lines_pull(split_chunk_into_lines(LinesState(..state, source:), chunk))
      }
  }
}

// Splits one chunk on `\n`; each piece is then split on `\r`, since a
// lone `\r` also ends a line. Only the text after the last terminator
// is carried over, so every chunk is scanned once.
fn split_chunk_into_lines(state: LinesState, chunk: String) -> LinesState {
  let segments = case state.after_cr, string.split(chunk, "\n") {
    True, ["", ..rest] -> rest
    _, segments -> segments
  }
  collect_lines(segments, state.partial, [])
  |> fn(collected) {
    let #(ready_rev, partial, after_cr) = collected
    LinesState(..state, ready: list.reverse(ready_rev), partial:, after_cr:)
  }
}

fn collect_lines(
  segments: List(String),
  partial: List(String),
  ready_rev: List(String),
) -> #(List(String), List(String), Bool) {
  case segments {
    [] -> #(ready_rev, partial, False)
    [last] -> {
      // Text after the last `\n`: lines ended by `\r` are complete; the
      // rest starts a line that a later chunk continues.
      case split_last(string.split(last, "\r")) {
        #([], tail) -> #(ready_rev, push_piece(partial, tail), False)
        #(complete, tail) -> #(
          emit_lines(complete, partial, ready_rev),
          push_piece([], tail),
          tail == "",
        )
      }
    }
    [segment, ..rest] -> {
      // A segment followed by `\n`. A trailing `\r` is the `\r` of a
      // `\r\n`; any other `\r` ends a line of its own.
      let pieces = string.split(segment, "\r")
      let pieces = case split_last(pieces) {
        #([_, ..] as complete, "") -> complete
        _ -> pieces
      }
      collect_lines(rest, [], emit_lines(pieces, partial, ready_rev))
    }
  }
}

// Prepends `pieces` to `ready_rev` as complete lines, the first one
// joined to the carried-over `partial` line.
fn emit_lines(
  pieces: List(String),
  partial: List(String),
  ready_rev: List(String),
) -> List(String) {
  case pieces {
    [] -> [reverse_concat(partial), ..ready_rev]
    [first, ..others] ->
      list.fold(
        others,
        [reverse_concat(push_piece(partial, first)), ..ready_rev],
        fn(acc, line) { [line, ..acc] },
      )
  }
}

fn push_piece(partial: List(String), piece: String) -> List(String) {
  case piece {
    "" -> partial
    _ -> [piece, ..partial]
  }
}

fn reverse_concat(reversed_pieces: List(String)) -> String {
  string.concat(list.reverse(reversed_pieces))
}

/// Group a stream of lines into records separated by blank lines.
///
/// Each emitted `List(String)` carries the lines of one record in
/// source order. Blank lines act as record separators and are not
/// included in any record. Multiple consecutive blank lines collapse
/// (no empty records are emitted, no spurious record at the start of
/// the stream when the input begins with a blank line). A trailing
/// record without a terminating blank line is still emitted so the
/// last record is not silently dropped.
///
/// Pair with `text.lines` for SSE (Server-Sent Events),
/// blank-line-separated NDJSON variants, mbox / RFC 822-style
/// envelopes, and similar wire formats:
///
/// ```gleam
/// stream
/// |> text.lines
/// |> text.records
/// |> stream.map(parse_event)
/// ```
///
/// Chunk-boundary handling: this operator runs entirely on the line
/// stream, so it inherits the chunk-boundary correctness of `lines`
/// — a record terminator that lands across two chunks is still
/// recognised because `lines` already absorbed the boundary before
/// emitting.
pub fn records(over stream: Stream(String)) -> Stream(List(String)) {
  records_active(stream, [], False)
}

fn records_active(
  source: Stream(String),
  acc_rev: List(String),
  source_drained: Bool,
) -> Stream(List(String)) {
  datastream.make(
    pull: fn() { records_pull(source, acc_rev, source_drained) },
    close: fn() { maybe_close(source, source_drained) },
  )
}

fn records_pull(
  source: Stream(String),
  acc_rev: List(String),
  source_drained: Bool,
) -> Step(List(String), Stream(List(String))) {
  case source_drained {
    True -> finish_records(source, acc_rev)
    False ->
      case datastream.pull(source) {
        Done -> finish_records(source, acc_rev)
        Next("", source_rest) ->
          case acc_rev {
            // Leading blank or back-to-back separator: skip without
            // emitting an empty record and keep pulling. The recursive
            // call only happens after the source has advanced, so this
            // cannot loop on a single value.
            [] -> records_pull(source_rest, [], False)
            _ ->
              Next(
                list.reverse(acc_rev),
                records_active(source_rest, [], False),
              )
          }
        Next(line, source_rest) ->
          records_pull(source_rest, [line, ..acc_rev], False)
      }
  }
}

fn finish_records(
  source: Stream(String),
  acc_rev: List(String),
) -> Step(List(String), Stream(List(String))) {
  case acc_rev {
    [] -> Done
    _ -> Next(list.reverse(acc_rev), records_active(source, [], True))
  }
}

/// Split a stream of strings on `delimiter`, emitting every separated
/// piece including the empty pieces between consecutive delimiters and
/// at the start / end of the input.
///
/// `delimiter == ""` triggers the grapheme-cluster splitter: each
/// emitted element is exactly one Unicode grapheme cluster of the
/// input, in source order. A grapheme that spans a chunk boundary
/// (a combining mark, `\r\n`, a flag or an emoji sequence split by the
/// read) is still emitted as one element: the last grapheme of each
/// chunk is held back until the next chunk or end-of-stream shows
/// whether it continues.
///
/// Chunk-boundary handling: pieces that span a chunk boundary are
/// joined; the trailing in-flight piece is held in a small buffer and
/// emitted only when a delimiter or end-of-stream is reached.
pub fn split(
  over stream: Stream(String),
  on delimiter: String,
) -> Stream(String) {
  case delimiter {
    "" -> graphemes_active(stream, [], "", False)
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

/// `pending` holds the complete graphemes of the chunks read so far;
/// `tail` is the last grapheme seen, which the next chunk may still
/// extend (a combining mark, the `\n` of `\r\n`, the second regional
/// indicator of a flag), so it is re-segmented together with that chunk.
fn graphemes_active(
  source: Stream(String),
  pending: List(String),
  tail: String,
  source_drained: Bool,
) -> Stream(String) {
  datastream.make(
    pull: fn() { graphemes_pull(source, pending, tail, source_drained) },
    close: fn() { maybe_close(source, source_drained) },
  )
}

fn graphemes_pull(
  source: Stream(String),
  pending: List(String),
  tail: String,
  source_drained: Bool,
) -> Step(String, Stream(String)) {
  case pending {
    [grapheme, ..rest] ->
      Next(grapheme, graphemes_active(source, rest, tail, source_drained))
    [] ->
      case source_drained {
        True -> Done
        False ->
          case datastream.pull(source) {
            Done ->
              case tail {
                "" -> Done
                _ -> Next(tail, graphemes_active(source, [], "", True))
              }
            Next(chunk, source_rest) -> {
              let #(complete, new_tail) =
                split_last(string.to_graphemes(tail <> chunk))
              graphemes_pull(source_rest, complete, new_tail, False)
            }
          }
      }
  }
}

/// Split off the last element; an empty list yields `#([], "")`.
fn split_last(graphemes: List(String)) -> #(List(String), String) {
  case list.reverse(graphemes) {
    [] -> #([], "")
    [last, ..rest_rev] -> #(list.reverse(rest_rev), last)
  }
}

// --- UTF-8 decode / encode ------------------------------------------------

/// Decode a stream of UTF-8 byte chunks into a stream of decoded
/// strings, reassembling multi-byte codepoints split across chunks.
///
/// Each run of valid UTF-8 that the decoder has buffered surfaces as
/// one `Ok(string)`, so how the decoded text is divided into elements
/// follows the chunk boundaries; the concatenated text does not. An
/// invalid byte (a byte that cannot start a sequence, or a missing or
/// out-of-range continuation, including overlong encodings, encoded
/// surrogates and code points above U+10FFFF) emits exactly one
/// `Error(Nil)`, and decoding resumes at the byte that broke the
/// sequence. An incomplete trailing sequence at end-of-stream emits a
/// final `Error(Nil)` before halting.
///
/// The decoder holds a small internal buffer for partial multi-byte
/// codepoints; it never materialises the full input.
pub fn utf8_decode(
  over stream: Stream(BitArray),
) -> Stream(Result(String, Nil)) {
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
      case is_continuation(lead, bit_array.byte_size(collected), b) {
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

/// Whether `b` may follow `lead` after `seen` continuation bytes.
/// Every continuation byte is in 0x80..0xBF, and the byte right after
/// the lead is narrower for four leads (RFC 3629, section 4): 0xE0 and
/// 0xF0 exclude overlong encodings, 0xED excludes the UTF-16 surrogates
/// U+D800..U+DFFF and 0xF4 excludes code points above U+10FFFF.
fn is_continuation(lead: Int, seen: Int, b: Int) -> Bool {
  let #(low, high) = case seen, lead {
    0, 0xE0 -> #(0xA0, 0xBF)
    0, 0xED -> #(0x80, 0x9F)
    0, 0xF0 -> #(0x90, 0xBF)
    0, 0xF4 -> #(0x80, 0x8F)
    _, _ -> #(0x80, 0xBF)
  }
  b >= low && b <= high
}

/// Decode a stream of UTF-8 byte chunks into a stream of decoded
/// strings, silently dropping any chunk that fails to decode.
///
/// Convenience wrapper for the common "I just want strings" pipeline:
/// `chunks |> text.utf8_decode_lossy |> text.lines |> ...` typechecks
/// without a hand-rolled `Result(String, Nil) -> Option(String)` shim.
///
/// Use this when invalid UTF-8 should be tolerated as data corruption
/// the caller does not need to observe (most file/socket reads of
/// known-good text). Use `utf8_decode` directly when the caller needs
/// to surface decode errors — for example via
/// `fold.partition_result` to log them while still streaming the
/// successful records through.
pub fn utf8_decode_lossy(over stream: Stream(BitArray)) -> Stream(String) {
  utf8_decode_lossy_active(utf8_decode(over: stream))
}

fn utf8_decode_lossy_active(
  source: Stream(Result(String, Nil)),
) -> Stream(String) {
  datastream.make(pull: fn() { utf8_decode_lossy_pull(source) }, close: fn() {
    datastream.close(source)
  })
}

fn utf8_decode_lossy_pull(
  source: Stream(Result(String, Nil)),
) -> Step(String, Stream(String)) {
  case datastream.pull(source) {
    Done -> Done
    Next(Ok(s), rest) -> Next(s, utf8_decode_lossy_active(rest))
    Next(Error(Nil), rest) -> utf8_decode_lossy_pull(rest)
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
