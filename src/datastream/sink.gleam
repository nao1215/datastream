//// Terminal consumers over `Stream(a)` — pure reductions and
//// side-effecting drivers.
////
//// `sink` is the unified terminal layer: every operation that drives
//// a stream to completion (or until an early-exit decision) lives
//// here. Pure reductions (`to_list`, `count`, `fold`, …) and
//// side-effecting consumers (`each`, `try_each`, `println`) sit
//// side-by-side; the import path is one line regardless of whether
//// the caller's terminal returns a value or runs a `Nil`-typed
//// effect.
////
//// The pure reductions are also exported from `datastream/fold` for
//// backward compatibility with code written against earlier
//// versions of this library. Prefer the `datastream/sink` import
//// path for new code; the `fold` aliases are kept for now and may
//// be removed in a future major release.

import datastream.{type Stream, Done, Next}
import datastream/fold
import gleam/io
import gleam/option.{type Option}
import gleam/string_tree.{type StringTree}

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
/// element is pulled from upstream and the upstream's `close` callback
/// is invoked before `Error(e)` is returned, so resource-backed
/// sources are released even on the early-exit path.
pub fn try_each(
  over stream: Stream(a),
  with effect: fn(a) -> Result(Nil, e),
) -> Result(Nil, e) {
  case datastream.pull(stream) {
    Done -> Ok(Nil)
    Next(element, rest) ->
      case effect(element) {
        Ok(Nil) -> try_each(over: rest, with: effect)
        Error(e) -> {
          datastream.close(rest)
          Error(e)
        }
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

// ---------------------------------------------------------------------------
// Pure terminal reductions — re-exported from `datastream/fold`.
//
// Every function below delegates to its `datastream/fold` counterpart.
// New code should reach for the `datastream/sink` path so the import
// is one line regardless of whether the terminal returns a value or
// runs a `Nil`-typed effect; the `datastream/fold` module is kept for
// backward compatibility.
// ---------------------------------------------------------------------------

/// Materialise the stream into a list, preserving source order.
/// Re-exported from `datastream/fold.to_list`.
pub fn to_list(stream: Stream(a)) -> List(a) {
  fold.to_list(stream)
}

/// Materialise a `Stream(String)` into a single concatenated `String`.
/// Re-exported from `datastream/fold.to_string`.
pub fn to_string(stream: Stream(String)) -> String {
  fold.to_string(stream)
}

/// Materialise a `Stream(String)` into a `StringTree`, preserving
/// source order. Re-exported from `datastream/fold.to_string_tree`.
pub fn to_string_tree(stream: Stream(String)) -> StringTree {
  fold.to_string_tree(stream)
}

/// Materialise a `Stream(String)` into a single `String` with
/// `separator` between consecutive elements. Streaming counterpart of
/// `gleam/string.join/2`. Re-exported from
/// `datastream/fold.to_string_join`. (#213)
pub fn to_string_join(
  stream stream: Stream(String),
  with separator: String,
) -> String {
  fold.to_string_join(stream:, with: separator)
}

/// `StringTree` variant of `to_string_join`. Re-exported from
/// `datastream/fold.to_string_tree_join`. (#213)
pub fn to_string_tree_join(
  stream stream: Stream(String),
  with separator: String,
) -> StringTree {
  fold.to_string_tree_join(stream:, with: separator)
}

/// Materialise a `Stream(BitArray)` into a single concatenated
/// `BitArray`. Re-exported from `datastream/fold.to_bit_array`.
pub fn to_bit_array(stream: Stream(BitArray)) -> BitArray {
  fold.to_bit_array(stream)
}

/// Count the number of elements the stream produces.
/// Re-exported from `datastream/fold.count`.
pub fn count(stream: Stream(a)) -> Int {
  fold.count(stream)
}

/// Pull until the first element and return it as `Some(a)`, or `None`
/// for an empty stream. Re-exported from `datastream/fold.first`.
pub fn first(stream: Stream(a)) -> Option(a) {
  fold.first(stream)
}

/// Drive the stream to completion and return the final element as
/// `Some(a)`, or `None` for an empty stream. Re-exported from
/// `datastream/fold.last`.
pub fn last(stream: Stream(a)) -> Option(a) {
  fold.last(stream)
}

/// Left-fold the stream with an explicit accumulator. Re-exported
/// from `datastream/fold.fold`.
pub fn fold(
  over stream: Stream(a),
  from initial: acc,
  with step: fn(acc, a) -> acc,
) -> acc {
  fold.fold(over: stream, from: initial, with: step)
}

/// Left-fold using the first element as the seed; returns `None` on
/// an empty stream. Re-exported from `datastream/fold.reduce`.
pub fn reduce(over stream: Stream(a), with step: fn(a, a) -> a) -> Option(a) {
  fold.reduce(over: stream, with: step)
}

/// Drive the stream to completion and discard every element.
/// Re-exported from `datastream/fold.drain`.
pub fn drain(stream: Stream(a)) -> Nil {
  fold.drain(stream)
}

/// Return `True` when every element satisfies `predicate`; `True` for
/// an empty stream. Short-circuits on the first `False`. Re-exported
/// from `datastream/fold.all`.
pub fn all(over stream: Stream(a), satisfying predicate: fn(a) -> Bool) -> Bool {
  fold.all(over: stream, satisfying: predicate)
}

/// Return `True` when at least one element satisfies `predicate`;
/// `False` for an empty stream. Short-circuits on the first `True`.
/// Re-exported from `datastream/fold.any`.
pub fn any(over stream: Stream(a), satisfying predicate: fn(a) -> Bool) -> Bool {
  fold.any(over: stream, satisfying: predicate)
}

/// Return the first element that satisfies `predicate`, or `None`.
/// Re-exported from `datastream/fold.find`.
pub fn find(
  in stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Option(a) {
  fold.find(in: stream, satisfying: predicate)
}

/// Sum every `Int` element. Returns 0 for an empty stream.
/// Re-exported from `datastream/fold.sum_int`.
pub fn sum_int(stream: Stream(Int)) -> Int {
  fold.sum_int(stream)
}

/// Sum every `Float` element. Returns 0.0 for an empty stream.
/// Re-exported from `datastream/fold.sum_float`.
pub fn sum_float(stream: Stream(Float)) -> Float {
  fold.sum_float(stream)
}

/// Multiply every `Int` element. Returns 1 for an empty stream.
/// Re-exported from `datastream/fold.product_int`.
pub fn product_int(stream: Stream(Int)) -> Int {
  fold.product_int(stream)
}

/// Collect a `Stream(Result(a, e))` into `Result(List(a), e)` with
/// short-circuit semantics on the first `Error(e)`. Re-exported from
/// `datastream/fold.collect_result`.
pub fn collect_result(stream: Stream(Result(a, e))) -> Result(List(a), e) {
  fold.collect_result(stream)
}

/// Partition a `Stream(Result(a, e))` into a `(oks, errs)` pair.
/// Re-exported from `datastream/fold.partition_result`.
pub fn partition_result(stream: Stream(Result(a, e))) -> #(List(a), List(e)) {
  fold.partition_result(stream)
}

/// Map every element to a `Result(b, c)` and partition the outcomes
/// into a `(lefts, rights)` pair. Re-exported from
/// `datastream/fold.partition_map`.
pub fn partition_map(
  over stream: Stream(a),
  with split: fn(a) -> Result(b, c),
) -> #(List(b), List(c)) {
  fold.partition_map(over: stream, with: split)
}
