//// Side-effecting terminal consumers over `Stream(a)`.
////
//// `sink` is the effectful half of the terminal layer; pure reductions
//// live in `fold`. The split is intentional: a reader can tell from
//// the import whether a function is pure (`fold`) or effectful (`sink`).
////
//// `fold.drain` already covers the "evaluate and discard" case, so a
//// `sink.drain` synonym is intentionally omitted.

import datastream.{type Stream, Done, Next}
import gleam/io

/// Drive the stream to completion, calling `effect` once per element
/// in source order. Returns `Nil`.
///
/// Use this when the work the consumer does is infallible. For
/// fallible consumers use `try_each`.
pub fn each(over stream: Stream(a), with effect: fn(a) -> Nil) -> Nil {
  case datastream.pull(stream) {
    Done -> Nil
    Next(element, rest) -> {
      effect(element)
      each(over: rest, with: effect)
    }
  }
}

/// Drive the stream while `effect` returns `Ok(Nil)`, halting on the
/// first `Error(e)` and returning that error.
///
/// On all-`Ok` returns `Ok(Nil)`. On the first `Error(e)`, no further
/// element is pulled from upstream — this also satisfies the
/// resource-close contract for resource-backed sources (introduced in
/// a later phase): an upstream that holds a handle will see its `Done`
/// branch reached when the unconsumed continuation is dropped.
pub fn try_each(
  over stream: Stream(a),
  with effect: fn(a) -> Result(Nil, e),
) -> Result(Nil, e) {
  case datastream.pull(stream) {
    Done -> Ok(Nil)
    Next(element, rest) ->
      case effect(element) {
        Ok(Nil) -> try_each(over: rest, with: effect)
        Error(e) -> Error(e)
      }
  }
}

/// Print every element of `stream` followed by a newline to standard
/// output, in source order.
///
/// Cross-target convenience over `gleam/io.println`. Target-aware
/// sinks (file, socket, …) belong outside the cross-target core.
pub fn println(stream: Stream(String)) -> Nil {
  each(over: stream, with: io.println)
}
