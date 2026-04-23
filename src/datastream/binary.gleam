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

fn bytes_active(
  source: Stream(BitArray),
  buffer: BitArray,
  source_drained: Bool,
) -> Stream(Int) {
  datastream.make(
    pull: fn() { bytes_pull(source, buffer, source_drained) },
    close: fn() {
      case source_drained {
        True -> Nil
        False -> datastream.close(source)
      }
    },
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

/// Read a `prefix_size` byte big-endian unsigned length, then yield
/// the next `length` bytes as a frame.
///
/// `prefix_size` MUST be one of `{1, 2, 4, 8}`. Other values are
/// rejected at construction time with a panic.
///
/// Incomplete frames at end-of-input (short prefix or short payload)
/// are NOT emitted; the stream simply halts.
pub fn length_prefixed(
  over stream: Stream(BitArray),
  prefix_size prefix_size: Int,
) -> Stream(BitArray) {
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
) -> Stream(BitArray) {
  datastream.make(
    pull: fn() {
      length_prefixed_pull(source, buffer, prefix_size, source_drained)
    },
    close: fn() {
      case source_drained {
        True -> Nil
        False -> datastream.close(source)
      }
    },
  )
}

fn length_prefixed_pull(
  source: Stream(BitArray),
  buffer: BitArray,
  prefix_size: Int,
  source_drained: Bool,
) -> Step(BitArray, Stream(BitArray)) {
  case bit_array.byte_size(buffer) >= prefix_size {
    False ->
      ensure_more(source, buffer, source_drained, fn(s, b, d) {
        length_prefixed_pull(s, b, prefix_size, d)
      })
    True ->
      length_prefixed_with_prefix(source, buffer, prefix_size, source_drained)
  }
}

fn length_prefixed_with_prefix(
  source: Stream(BitArray),
  buffer: BitArray,
  prefix_size: Int,
  source_drained: Bool,
) -> Step(BitArray, Stream(BitArray)) {
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
            frame,
            length_prefixed_active(source, rest, prefix_size, source_drained),
          )
        _, _ -> Done
      }
    False ->
      ensure_more(source, buffer, source_drained, fn(s, b, d) {
        length_prefixed_pull(s, b, prefix_size, d)
      })
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
  delimited_active(stream, <<>>, delimiter, False, False)
}

fn delimited_active(
  source: Stream(BitArray),
  buffer: BitArray,
  delimiter: BitArray,
  source_drained: Bool,
  has_seen_input: Bool,
) -> Stream(BitArray) {
  datastream.make(
    pull: fn() {
      delimited_pull(source, buffer, delimiter, source_drained, has_seen_input)
    },
    close: fn() {
      case source_drained {
        True -> Nil
        False -> datastream.close(source)
      }
    },
  )
}

fn delimited_pull(
  source: Stream(BitArray),
  buffer: BitArray,
  delimiter: BitArray,
  source_drained: Bool,
  has_seen_input: Bool,
) -> Step(BitArray, Stream(BitArray)) {
  case find_delimiter(buffer, delimiter, 0) {
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
            delimited_active(source, rest, delimiter, source_drained, True),
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
              )
            Done ->
              delimited_pull(source, buffer, delimiter, True, has_seen_input)
          }
      }
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
/// time with a panic.
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
    close: fn() {
      case source_drained {
        True -> Nil
        False -> datastream.close(source)
      }
    },
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

// --- internal helpers ----------------------------------------------------

fn ensure_more(
  source: Stream(BitArray),
  buffer: BitArray,
  source_drained: Bool,
  retry: fn(Stream(BitArray), BitArray, Bool) ->
    Step(BitArray, Stream(BitArray)),
) -> Step(BitArray, Stream(BitArray)) {
  case source_drained {
    True -> Done
    False ->
      case datastream.pull(source) {
        Next(chunk, source_rest) ->
          retry(source_rest, bit_array.append(buffer, chunk), False)
        Done -> retry(source, buffer, True)
      }
  }
}
