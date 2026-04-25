//// Framing operations on `Stream(BitArray)`.
////
//// Real byte sources (sockets, files, pipes) deliver bytes in
//// arbitrary chunks. A frame may straddle chunks, several frames may
//// sit in one chunk, or a chunk may end mid-prefix. Every framing
//// combinator here owns the buffer needed to reassemble.
////
//// All combinators are lazy and chunk-boundary-aware; none of them
//// materialises the full input.

import datastream.{type Step, type Stream, Done, Next}
import gleam/bit_array

// --- bytes ----------------------------------------------------------------

/// Yield each byte of each chunk in order, as `Int` in `0..255`.
///
/// The trivial degenerate framing: bridges from `Stream(BitArray)` to
/// `Stream(Int)` so callers can move into the byte-stream-shaped world
/// without writing the per-byte `unfold` themselves.
pub fn bytes(over stream: Stream(BitArray)) -> Stream(Int) {
  bytes_active(stream, <<>>, False)
}

fn maybe_close(source: Stream(a), source_drained: Bool) -> Nil {
  case source_drained {
    True -> Nil
    False -> datastream.close(source)
  }
}

fn bytes_active(
  source: Stream(BitArray),
  buffer: BitArray,
  source_drained: Bool,
) -> Stream(Int) {
  datastream.make(
    pull: fn() { bytes_pull(source, buffer, source_drained) },
    close: fn() { maybe_close(source, source_drained) },
  )
}

fn bytes_pull(
  source: Stream(BitArray),
  buffer: BitArray,
  source_drained: Bool,
) -> Step(Int, Stream(Int)) {
  case buffer {
    <<b:size(8), rest:bits>> ->
      Next(b, bytes_active(source, rest, source_drained))
    _ ->
      case source_drained {
        True -> Done
        False ->
          case datastream.pull(source) {
            Next(chunk, source_rest) ->
              bytes_pull(source_rest, bit_array.append(buffer, chunk), False)
            Done -> bytes_pull(source, buffer, True)
          }
      }
  }
}

// --- length-prefixed -----------------------------------------------------

/// Truncation surfaced by `length_prefixed` when the input ends
/// mid-frame.
///
/// `expected` is the number of bytes the combinator was waiting for —
/// either `prefix_size` (when the prefix itself was short) or the
/// declared payload length (when the prefix was complete but the
/// payload was short). `got` is the number of bytes the stream
/// actually delivered toward that requirement (always `< expected`).
pub type IncompleteFrame {
  IncompleteFrame(expected: Int, got: Int)
}

/// Read a `prefix_size` byte big-endian unsigned length, then yield
/// the next `length` bytes as a frame, wrapped in `Ok`.
///
/// `prefix_size` MUST be one of `{1, 2, 4, 8}`. Other values are
/// rejected at construction time with a panic per the `datastream`
/// module-level invalid-argument policy.
///
/// If the input ends mid-frame (the prefix is shorter than
/// `prefix_size` bytes, or the payload is shorter than the declared
/// length), the stream emits exactly one
/// `Error(IncompleteFrame(expected:, got:))` element before halting,
/// so callers can distinguish "no more frames" from "truncated final
/// frame". The `Stream(Result(_, _))` shape mirrors `text.utf8_decode`;
/// chain `fold.collect_result` to stop on the first truncation, or
/// `fold.partition_result` to drive the stream to completion and split
/// well-formed frames from the incomplete tail.
pub fn length_prefixed(
  over stream: Stream(BitArray),
  prefix_size prefix_size: Int,
) -> Stream(Result(BitArray, IncompleteFrame)) {
  case prefix_size {
    1 | 2 | 4 | 8 -> length_prefixed_active(stream, <<>>, prefix_size, False)
    _ ->
      panic as "datastream/binary.length_prefixed: prefix_size must be 1, 2, 4, or 8"
  }
}

fn length_prefixed_active(
  source: Stream(BitArray),
  buffer: BitArray,
  prefix_size: Int,
  source_drained: Bool,
) -> Stream(Result(BitArray, IncompleteFrame)) {
  datastream.make(
    pull: fn() {
      length_prefixed_pull(source, buffer, prefix_size, source_drained)
    },
    close: fn() { maybe_close(source, source_drained) },
  )
}

fn length_prefixed_pull(
  source: Stream(BitArray),
  buffer: BitArray,
  prefix_size: Int,
  source_drained: Bool,
) -> Step(
  Result(BitArray, IncompleteFrame),
  Stream(Result(BitArray, IncompleteFrame)),
) {
  case bit_array.byte_size(buffer) >= prefix_size {
    False ->
      case source_drained {
        True ->
          case bit_array.byte_size(buffer) {
            0 -> Done
            got ->
              Next(
                Error(IncompleteFrame(expected: prefix_size, got: got)),
                length_prefixed_active(source, <<>>, prefix_size, True),
              )
          }
        False -> length_prefixed_pull_more(source, buffer, prefix_size)
      }
    True ->
      length_prefixed_with_prefix(source, buffer, prefix_size, source_drained)
  }
}

fn length_prefixed_with_prefix(
  source: Stream(BitArray),
  buffer: BitArray,
  prefix_size: Int,
  source_drained: Bool,
) -> Step(
  Result(BitArray, IncompleteFrame),
  Stream(Result(BitArray, IncompleteFrame)),
) {
  let length = read_prefix(buffer, prefix_size)
  let total_needed = prefix_size + length
  case bit_array.byte_size(buffer) >= total_needed {
    True ->
      case
        bit_array.slice(buffer, prefix_size, length),
        bit_array.slice(
          buffer,
          total_needed,
          bit_array.byte_size(buffer) - total_needed,
        )
      {
        Ok(frame), Ok(rest) ->
          Next(
            Ok(frame),
            length_prefixed_active(source, rest, prefix_size, source_drained),
          )
        _, _ -> Done
      }
    False ->
      case source_drained {
        True ->
          Next(
            Error(IncompleteFrame(
              expected: length,
              got: bit_array.byte_size(buffer) - prefix_size,
            )),
            length_prefixed_active(source, <<>>, prefix_size, True),
          )
        False -> length_prefixed_pull_more(source, buffer, prefix_size)
      }
  }
}

fn length_prefixed_pull_more(
  source: Stream(BitArray),
  buffer: BitArray,
  prefix_size: Int,
) -> Step(
  Result(BitArray, IncompleteFrame),
  Stream(Result(BitArray, IncompleteFrame)),
) {
  case datastream.pull(source) {
    Next(chunk, source_rest) ->
      length_prefixed_pull(
        source_rest,
        bit_array.append(buffer, chunk),
        prefix_size,
        False,
      )
    Done -> length_prefixed_pull(source, buffer, prefix_size, True)
  }
}

fn read_prefix(buffer: BitArray, size: Int) -> Int {
  case size, buffer {
    1, <<n:size(8), _:bits>> -> n
    2, <<n:size(16), _:bits>> -> n
    4, <<n:size(32), _:bits>> -> n
    8, <<n:size(64), _:bits>> -> n
    _, _ -> 0
  }
}

// --- delimited ------------------------------------------------------------

/// Yield delimiter-separated frames in source order, preserving empty
/// frames between consecutive delimiters and the trailing frame after
/// a trailing delimiter. The delimiter itself is NOT included in any
/// emitted frame.
///
/// If the input does not end with a delimiter, the trailing partial
/// frame is emitted as the last element.
pub fn delimited(
  over stream: Stream(BitArray),
  on delimiter: BitArray,
) -> Stream(BitArray) {
  delimited_active(stream, <<>>, delimiter, False, False, 0)
}

fn delimited_active(
  source: Stream(BitArray),
  buffer: BitArray,
  delimiter: BitArray,
  source_drained: Bool,
  has_seen_input: Bool,
  scan_from: Int,
) -> Stream(BitArray) {
  datastream.make(
    pull: fn() {
      delimited_pull(
        source,
        buffer,
        delimiter,
        source_drained,
        has_seen_input,
        scan_from,
      )
    },
    close: fn() { maybe_close(source, source_drained) },
  )
}

fn delimited_pull(
  source: Stream(BitArray),
  buffer: BitArray,
  delimiter: BitArray,
  source_drained: Bool,
  has_seen_input: Bool,
  scan_from: Int,
) -> Step(BitArray, Stream(BitArray)) {
  case find_delimiter(buffer, delimiter, scan_from) {
    Ok(pos) -> {
      let delim_size = bit_array.byte_size(delimiter)
      case
        bit_array.slice(buffer, 0, pos),
        bit_array.slice(
          buffer,
          pos + delim_size,
          bit_array.byte_size(buffer) - pos - delim_size,
        )
      {
        Ok(frame), Ok(rest) ->
          Next(
            frame,
            delimited_active(source, rest, delimiter, source_drained, True, 0),
          )
        _, _ -> Done
      }
    }
    Error(_) ->
      case source_drained {
        True ->
          case has_seen_input || bit_array.byte_size(buffer) > 0 {
            True -> Next(buffer, delimited_drained())
            False -> Done
          }
        False ->
          case datastream.pull(source) {
            Next(chunk, source_rest) ->
              delimited_pull(
                source_rest,
                bit_array.append(buffer, chunk),
                delimiter,
                False,
                has_seen_input || bit_array.byte_size(chunk) > 0,
                resume_scan_offset(buffer, delimiter),
              )
            Done ->
              delimited_pull(
                source,
                buffer,
                delimiter,
                True,
                has_seen_input,
                scan_from,
              )
          }
      }
  }
}

/// Where to resume scanning the new (longer) buffer after appending a
/// fresh chunk. The previous full scan covered everything except the
/// last `delim_size - 1` bytes, since a delimiter shorter than that
/// would already have matched. Backing off by `delim_size - 1` is
/// enough to catch a delimiter that straddles the chunk boundary.
fn resume_scan_offset(buffer: BitArray, delimiter: BitArray) -> Int {
  let delim_size = bit_array.byte_size(delimiter)
  let buffer_size = bit_array.byte_size(buffer)
  case buffer_size - delim_size + 1 {
    n if n > 0 -> n
    _ -> 0
  }
}

fn delimited_drained() -> Stream(BitArray) {
  datastream.make(pull: fn() { Done }, close: fn() { Nil })
}

fn find_delimiter(
  buffer: BitArray,
  delimiter: BitArray,
  pos: Int,
) -> Result(Int, Nil) {
  let buffer_size = bit_array.byte_size(buffer)
  let delim_size = bit_array.byte_size(delimiter)
  case pos + delim_size > buffer_size {
    True -> Error(Nil)
    False ->
      case bit_array.slice(buffer, pos, delim_size) {
        Ok(window) ->
          case window == delimiter {
            True -> Ok(pos)
            False -> find_delimiter(buffer, delimiter, pos + 1)
          }
        Error(_) -> Error(Nil)
      }
  }
}

// --- fixed-size ----------------------------------------------------------

/// Yield successive `size`-byte frames in source order. The trailing
/// partial frame on EOF is discarded — by definition a fixed-size
/// framing combinator cannot emit a frame of fewer than `size` bytes.
///
/// `size` MUST be `>= 1`. Other values are rejected at construction
/// time with a panic per the `datastream` module-level
/// invalid-argument policy.
pub fn fixed_size(
  over stream: Stream(BitArray),
  size size: Int,
) -> Stream(BitArray) {
  case size >= 1 {
    True -> fixed_size_active(stream, <<>>, size, False)
    False -> panic as "datastream/binary.fixed_size: size must be >= 1"
  }
}

fn fixed_size_active(
  source: Stream(BitArray),
  buffer: BitArray,
  size: Int,
  source_drained: Bool,
) -> Stream(BitArray) {
  datastream.make(
    pull: fn() { fixed_size_pull(source, buffer, size, source_drained) },
    close: fn() { maybe_close(source, source_drained) },
  )
}

fn fixed_size_pull(
  source: Stream(BitArray),
  buffer: BitArray,
  size: Int,
  source_drained: Bool,
) -> Step(BitArray, Stream(BitArray)) {
  case bit_array.byte_size(buffer) >= size {
    True ->
      case
        bit_array.slice(buffer, 0, size),
        bit_array.slice(buffer, size, bit_array.byte_size(buffer) - size)
      {
        Ok(frame), Ok(rest) ->
          Next(frame, fixed_size_active(source, rest, size, source_drained))
        _, _ -> Done
      }
    False ->
      case source_drained {
        True -> Done
        False ->
          case datastream.pull(source) {
            Next(chunk, source_rest) ->
              fixed_size_pull(
                source_rest,
                bit_array.append(buffer, chunk),
                size,
                False,
              )
            Done -> fixed_size_pull(source, buffer, size, True)
          }
      }
  }
}
