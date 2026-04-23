//// Pure terminal reductions over `Stream(a)`.
////
//// This module is the `fold` half of the terminal layer: every function
//// here drives a stream to completion (or until an early-exit decision)
//// and returns a plain value. Side-effecting terminals live in `sink`.
////
//// Building combinators alone has no observable effect — calling one of
//// these functions is what actually pulls elements from the source.
//// Pipelines re-run the source on every terminal call; there is no
//// implicit caching.

import datastream.{type Stream, Done, Next}
import gleam/list
import gleam/option.{type Option, None, Some}

/// Materialise the stream into a list, preserving source order.
///
/// Pulls every element. On a `Stream` of `n` elements this allocates
/// `O(n)` cons cells. For terminating streams only.
pub fn to_list(stream: Stream(a)) -> List(a) {
  stream
  |> fold(from: [], with: fn(acc, element) { [element, ..acc] })
  |> list.reverse
}

/// Count the number of elements the stream produces.
///
/// Drives the stream to completion without retaining elements. Returns
/// `0` on an empty stream.
pub fn count(stream: Stream(a)) -> Int {
  fold(over: stream, from: 0, with: fn(acc, _) { acc + 1 })
}

/// Return the first element produced by the stream, if any.
///
/// Pulls at most one element from upstream and stops; this makes `first`
/// safe on infinite streams. Returns `None` if the stream is empty.
pub fn first(stream: Stream(a)) -> Option(a) {
  case datastream.pull(stream) {
    Next(element, _) -> Some(element)
    Done -> None
  }
}

/// Return the last element produced by the stream, if any.
///
/// Drives the stream to completion. Returns `None` on an empty stream.
/// Does not terminate on an infinite stream.
pub fn last(stream: Stream(a)) -> Option(a) {
  fold(over: stream, from: None, with: fn(_, element) { Some(element) })
}

/// Left-fold over the stream, seeded with `initial`.
///
/// Returns `initial` unchanged on an empty stream. The accumulator is
/// updated by `step(acc, element)` in source order.
pub fn fold(
  over stream: Stream(a),
  from initial: b,
  with step: fn(b, a) -> b,
) -> b {
  case datastream.pull(stream) {
    Next(element, rest) ->
      fold(over: rest, from: step(initial, element), with: step)
    Done -> initial
  }
}

/// Left-fold without an explicit seed: the head element seeds the fold.
///
/// Returns `None` on an empty stream, `Some(only)` on a one-element
/// stream, and `Some(fold_of_tail_seeded_with_head)` otherwise.
pub fn reduce(over stream: Stream(a), with step: fn(a, a) -> a) -> Option(a) {
  case datastream.pull(stream) {
    Next(head, rest) -> Some(fold(over: rest, from: head, with: step))
    Done -> None
  }
}

/// Drive the stream to completion and discard every element.
///
/// Useful when the pipeline is built purely for traversal (for example
/// to walk a `tap`-instrumented chain) and the caller does not want to
/// import `sink` for a no-op consumer.
pub fn drain(stream: Stream(a)) -> Nil {
  case datastream.pull(stream) {
    Next(_, rest) -> drain(rest)
    Done -> Nil
  }
}
