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
import datastream/internal/core
import gleam/bit_array
import gleam/list
import gleam/option.{type Option, None, Some}

// --- argument validation --------------------------------------------------

/// Why a checked binary framing constructor refused its argument.
///
/// Returned by `length_prefixed_checked`, `length_prefixed_with_checked`,
/// and `fixed_size_checked`. Lets the caller surface argument
/// validation failures through `Result` instead of crashing the
/// process — useful when the numeric argument comes from CLI flags,
/// config files, or request parameters.
///
/// `function` names the constructor that rejected the value
/// (`"length_prefixed"`, `"fixed_size"`) so a caller routing many
/// checked constructors through the same handler can produce a
/// meaningful error message.
pub type BinaryArgError {
  /// `length_prefixed` requires `prefix_size` to be one of
  /// `{1, 2, 4, 8}`. `given` is the value the caller passed.
  InvalidPrefixSize(function: String, given: Int)
  /// `fixed_size` requires `size` to be `>= 1`. `given` is the value
  /// the caller passed.
  NotPositiveSize(function: String, given: Int)
}

// --- buffered bytes -------------------------------------------------------

// Bytes received but not framed yet. `joined` is one contiguous
// binary; `later` holds the chunks that arrived after it, newest
// first, and is joined only once a frame needs those bytes, so a frame
// assembled from many small chunks is copied once. `size` counts the
// bytes in both.
type Buffered {
  Buffered(joined: BitArray, later: List(BitArray), size: Int)
}

const nothing_buffered = Buffered(joined: <<>>, later: [], size: 0)

fn buffer_chunk(buffered: Buffered, chunk: BitArray) -> Buffered {
  case bit_array.byte_size(chunk) {
    0 -> buffered
    chunk_size ->
      Buffered(
        ..buffered,
        later: [chunk, ..buffered.later],
        size: buffered.size + chunk_size,
      )
  }
}

// Split off the first `count` bytes. The caller checks that
// `buffered.size >= count`.
fn take_bytes(buffered: Buffered, count: Int) -> #(BitArray, Buffered) {
  let joined = case bit_array.byte_size(buffered.joined) >= count {
    True -> buffered.joined
    False -> bit_array.concat([buffered.joined, ..list.reverse(buffered.later)])
  }
  let later = case joined == buffered.joined {
    True -> buffered.later
    False -> []
  }
  case joined {
    <<taken:bytes-size(count), rest:bytes>> -> #(
      taken,
      Buffered(joined: rest, later:, size: buffered.size - count),
    )
    _ -> #(<<>>, buffered)
  }
}

// Pull one more chunk into `buffered`, or `None` once `source` is done.
fn refill(
  source: Stream(BitArray),
  buffered: Buffered,
) -> Option(#(Stream(BitArray), Buffered)) {
  case datastream.pull(source) {
    Next(chunk, rest) -> Some(#(rest, buffer_chunk(buffered, chunk)))
    Done -> None
  }
}

// --- bytes ----------------------------------------------------------------

/// Yield each byte of each chunk in order, as `Int` in `0..255`.
///
/// The trivial degenerate framing: bridges from `Stream(BitArray)` to
/// `Stream(Int)` so callers can move into the byte-stream-shaped world
/// without writing the per-byte `unfold` themselves.
pub fn bytes(over stream: Stream(BitArray)) -> Stream(Int) {
  bytes_active(stream, <<>>)
}

fn bytes_active(source: Stream(BitArray), buffer: BitArray) -> Stream(Int) {
  datastream.make(pull: fn() { bytes_pull(source, buffer) }, close: fn() {
    datastream.close(source)
  })
}

fn bytes_pull(
  source: Stream(BitArray),
  buffer: BitArray,
) -> Step(Int, Stream(Int)) {
  case buffer {
    <<b:size(8), rest:bits>> -> Next(b, bytes_active(source, rest))
    _ ->
      case datastream.pull(source) {
        Next(chunk, source_rest) ->
          bytes_pull(source_rest, bit_array.append(buffer, chunk))
        Done -> Done
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
  /// The prefix declared a payload length exceeding `max_frame_size`.
  /// The stream ends after this element, since the bytes that follow
  /// can no longer be split into frames.
  FrameTooLarge(claimed: Int, max: Int)
}

const default_max_frame_size = 16_777_216

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
///
/// Frames longer than 16 MiB are rejected with `FrameTooLarge`; use
/// `length_prefixed_with` to set another limit.
pub fn length_prefixed(
  over stream: Stream(BitArray),
  prefix_size prefix_size: Int,
) -> Stream(Result(BitArray, IncompleteFrame)) {
  length_prefixed_with(
    over: stream,
    prefix_size: prefix_size,
    max_frame_size: default_max_frame_size,
  )
}

/// Like `length_prefixed`, but with an explicit `max_frame_size` limit.
/// Frames whose prefix claims more than `max_frame_size` bytes are
/// rejected immediately with `Error(FrameTooLarge(claimed:, max:))`
/// without buffering the payload, and the stream ends after that
/// element.
pub fn length_prefixed_with(
  over stream: Stream(BitArray),
  prefix_size prefix_size: Int,
  max_frame_size max_frame_size: Int,
) -> Stream(Result(BitArray, IncompleteFrame)) {
  case
    length_prefixed_with_checked(
      over: stream,
      prefix_size: prefix_size,
      max_frame_size: max_frame_size,
    )
  {
    Ok(framed) -> framed
    Error(_) ->
      panic as "datastream/binary.length_prefixed: prefix_size must be 1, 2, 4, or 8"
  }
}

/// Like `length_prefixed`, but returns the argument-validation
/// failure as a `Result` instead of panicking. Use this when
/// `prefix_size` comes from dynamic input (CLI, config, request
/// parameters); the panicking `length_prefixed` remains the right
/// tool for trusted constants.
///
/// On success the stream behaves identically to
/// `length_prefixed(over: stream, prefix_size: prefix_size)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error`.
pub fn length_prefixed_checked(
  over stream: Stream(BitArray),
  prefix_size prefix_size: Int,
) -> Result(Stream(Result(BitArray, IncompleteFrame)), BinaryArgError) {
  length_prefixed_with_checked(
    over: stream,
    prefix_size: prefix_size,
    max_frame_size: default_max_frame_size,
  )
}

/// Like `length_prefixed_with`, but returns the argument-validation
/// failure as a `Result` instead of panicking. Use this when
/// `prefix_size` comes from dynamic input.
pub fn length_prefixed_with_checked(
  over stream: Stream(BitArray),
  prefix_size prefix_size: Int,
  max_frame_size max_frame_size: Int,
) -> Result(Stream(Result(BitArray, IncompleteFrame)), BinaryArgError) {
  case prefix_size {
    1 | 2 | 4 | 8 ->
      Ok(
        length_prefixed_active(LengthPrefixed(
          source: stream,
          buffered: nothing_buffered,
          prefix_size:,
          max_frame_size:,
          payload_size: None,
        )),
      )
    _ ->
      Error(InvalidPrefixSize(function: "length_prefixed", given: prefix_size))
  }
}

// `payload_size` is `None` while the prefix is being read and
// `Some(length)` once it has been read and the payload is pending.
type LengthPrefixed {
  LengthPrefixed(
    source: Stream(BitArray),
    buffered: Buffered,
    prefix_size: Int,
    max_frame_size: Int,
    payload_size: Option(Int),
  )
}

fn length_prefixed_active(
  state: LengthPrefixed,
) -> Stream(Result(BitArray, IncompleteFrame)) {
  datastream.make(pull: fn() { length_prefixed_pull(state) }, close: fn() {
    datastream.close(state.source)
  })
}

fn length_prefixed_pull(
  state: LengthPrefixed,
) -> Step(
  Result(BitArray, IncompleteFrame),
  Stream(Result(BitArray, IncompleteFrame)),
) {
  let needed = option.unwrap(state.payload_size, state.prefix_size)
  case state.buffered.size >= needed {
    True -> {
      let #(bytes, buffered) = take_bytes(state.buffered, needed)
      let state = LengthPrefixed(..state, buffered:)
      case state.payload_size, read_prefix(bytes, state.prefix_size) {
        Some(_), _ ->
          Next(
            Ok(bytes),
            length_prefixed_active(LengthPrefixed(..state, payload_size: None)),
          )
        None, length if length > state.max_frame_size -> {
          // The bytes after an oversized prefix cannot be framed again,
          // so the stream ends here.
          datastream.close(state.source)
          Next(
            Error(FrameTooLarge(claimed: length, max: state.max_frame_size)),
            core.empty(),
          )
        }
        None, length ->
          length_prefixed_pull(
            LengthPrefixed(..state, payload_size: Some(length)),
          )
      }
    }
    False ->
      case refill(state.source, state.buffered), state.buffered.size {
        Some(#(source, buffered)), _ ->
          length_prefixed_pull(LengthPrefixed(..state, source:, buffered:))
        None, 0 if state.payload_size == None -> Done
        None, got ->
          Next(Error(IncompleteFrame(expected: needed, got:)), core.empty())
      }
  }
}

fn read_prefix(buffer: BitArray, size: Int) -> Int {
  case size, buffer {
    1, <<n:size(8), _:bits>> -> n
    2, <<n:size(16), _:bits>> -> n
    4, <<n:size(32), _:bits>> -> n
    // Two 32-bit halves: a single 64-bit integer segment is truncated on
    // the JavaScript target. The result on Erlang is identical.
    8, <<high:size(32), low:size(32), _:bits>> -> high * 4_294_967_296 + low
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
///
/// ## Panics
///
/// Panics at construction when `delimiter` is empty, since an empty
/// delimiter matches everywhere without consuming any input.
pub fn delimited(
  over stream: Stream(BitArray),
  on delimiter: BitArray,
) -> Stream(BitArray) {
  case delimiter {
    <<>> -> panic as "datastream/binary.delimited: delimiter must not be empty"
    _ ->
      delimited_active(Delimited(
        source: stream,
        delimiter:,
        frame: [],
        recent: [],
        unscanned: <<>>,
        has_seen_input: False,
      ))
  }
}

// The current frame is kept as chunks so that it is joined once, when
// its delimiter is found. `frame` holds the chunks that are certainly
// part of the frame, newest first. `recent` holds the newest scanned
// chunks, newest first, just enough of them to cover
// `byte_size(delimiter) - 1` bytes: a delimiter that starts there may
// end in a chunk that has not arrived yet, so they are scanned again
// together with the next chunk. `unscanned` is the input not searched
// yet.
type Delimited {
  Delimited(
    source: Stream(BitArray),
    delimiter: BitArray,
    frame: List(BitArray),
    recent: List(BitArray),
    unscanned: BitArray,
    has_seen_input: Bool,
  )
}

fn delimited_active(state: Delimited) -> Stream(BitArray) {
  datastream.make(pull: fn() { delimited_pull(state) }, close: fn() {
    datastream.close(state.source)
  })
}

fn delimited_pull(state: Delimited) -> Step(BitArray, Stream(BitArray)) {
  let window = bit_array.concat(list.reverse([state.unscanned, ..state.recent]))
  let delimiter_size = bit_array.byte_size(state.delimiter)
  case find_delimiter(window, state.delimiter, 0) {
    Ok(position) ->
      case window {
        <<
          before:bytes-size(position),
          _:bytes-size(delimiter_size),
          after:bytes,
        >> ->
          Next(
            bit_array.concat(list.reverse([before, ..state.frame])),
            delimited_active(
              Delimited(
                ..state,
                frame: [],
                recent: [],
                unscanned: after,
                has_seen_input: True,
              ),
            ),
          )
        _ -> Done
      }
    Error(_) -> {
      let #(recent, older) =
        core.keep_newest(
          [state.unscanned, ..state.recent],
          holding: delimiter_size - 1,
          measured_by: bit_array.byte_size,
        )
      let state =
        Delimited(
          ..state,
          frame: list.append(older, state.frame),
          recent:,
          unscanned: <<>>,
        )
      case datastream.pull(state.source) {
        Next(chunk, source) ->
          delimited_pull(
            Delimited(
              ..state,
              source:,
              unscanned: chunk,
              has_seen_input: state.has_seen_input
                || bit_array.byte_size(chunk) > 0,
            ),
          )
        Done ->
          case state.has_seen_input {
            True ->
              Next(
                bit_array.concat(list.reverse(list.append(recent, state.frame))),
                core.empty(),
              )
            False -> Done
          }
      }
    }
  }
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
  case fixed_size_checked(over: stream, size: size) {
    Ok(framed) -> framed
    Error(_) -> panic as "datastream/binary.fixed_size: size must be >= 1"
  }
}

/// Like `fixed_size`, but returns the argument-validation failure as
/// a `Result` instead of panicking. Use this when `size` comes from
/// dynamic input (CLI, config, request parameters); the panicking
/// `fixed_size` remains the right tool for trusted constants.
///
/// On success the stream behaves identically to
/// `fixed_size(over: stream, size: size)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error`.
pub fn fixed_size_checked(
  over stream: Stream(BitArray),
  size size: Int,
) -> Result(Stream(BitArray), BinaryArgError) {
  case size >= 1 {
    True -> Ok(fixed_size_active(stream, nothing_buffered, size))
    False -> Error(NotPositiveSize(function: "fixed_size", given: size))
  }
}

fn fixed_size_active(
  source: Stream(BitArray),
  buffered: Buffered,
  size: Int,
) -> Stream(BitArray) {
  datastream.make(
    pull: fn() { fixed_size_pull(source, buffered, size) },
    close: fn() { datastream.close(source) },
  )
}

fn fixed_size_pull(
  source: Stream(BitArray),
  buffered: Buffered,
  size: Int,
) -> Step(BitArray, Stream(BitArray)) {
  case buffered.size >= size {
    True -> {
      let #(frame, buffered) = take_bytes(buffered, size)
      Next(frame, fixed_size_active(source, buffered, size))
    }
    False ->
      case refill(source, buffered) {
        Some(#(source, buffered)) -> fixed_size_pull(source, buffered, size)
        // The trailing partial frame, if any, is discarded.
        None -> Done
      }
  }
}
