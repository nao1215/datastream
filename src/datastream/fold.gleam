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
/// Pulls at most one element from upstream, closes the unconsumed
/// continuation, and stops. This makes `first` safe on infinite
/// resource-backed streams. Returns `None` if the stream is empty.
pub fn first(stream: Stream(a)) -> Option(a) {
  case datastream.pull(stream) {
    Next(element, rest) -> {
      datastream.close(rest)
      Some(element)
    }
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

/// Return `True` only when `predicate` holds for every element.
///
/// Returns `True` on an empty stream and short-circuits on the first
/// `False`, so `all` terminates on infinite sources whose first failure
/// is reachable.
pub fn all(over stream: Stream(a), satisfying predicate: fn(a) -> Bool) -> Bool {
  case datastream.pull(stream) {
    Done -> True
    Next(element, rest) ->
      case predicate(element) {
        True -> all(over: rest, satisfying: predicate)
        False -> {
          datastream.close(rest)
          False
        }
      }
  }
}

/// Return `True` as soon as `predicate` holds for some element.
///
/// Returns `False` on an empty stream and short-circuits on the first
/// `True`, so `any` terminates on infinite sources whose first match is
/// reachable.
pub fn any(over stream: Stream(a), satisfying predicate: fn(a) -> Bool) -> Bool {
  case datastream.pull(stream) {
    Done -> False
    Next(element, rest) ->
      case predicate(element) {
        True -> {
          datastream.close(rest)
          True
        }
        False -> any(over: rest, satisfying: predicate)
      }
  }
}

/// Return the first element satisfying `predicate`, or `None`.
///
/// Stops pulling upstream the moment a match is observed.
pub fn find(
  in stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Option(a) {
  case datastream.pull(stream) {
    Done -> None
    Next(element, rest) ->
      case predicate(element) {
        True -> {
          datastream.close(rest)
          Some(element)
        }
        False -> find(in: rest, satisfying: predicate)
      }
  }
}

/// Sum a stream of `Int`s. Returns `0` on the empty stream.
pub fn sum_int(stream: Stream(Int)) -> Int {
  fold(over: stream, from: 0, with: fn(acc, x) { acc + x })
}

/// Sum a stream of `Float`s. Returns `0.0` on the empty stream.
pub fn sum_float(stream: Stream(Float)) -> Float {
  fold(over: stream, from: 0.0, with: fn(acc, x) { acc +. x })
}

/// Multiply a stream of `Int`s. Returns `1` on the empty stream.
pub fn product_int(stream: Stream(Int)) -> Int {
  fold(over: stream, from: 1, with: fn(acc, x) { acc * x })
}

/// Walk a `Stream(Result(a, e))`, returning `Ok(values)` if every
/// element is `Ok`, or short-circuiting with the first `Error(e)`.
///
/// Order is preserved: `Ok` values land in the returned list in source
/// order. Pulling stops the moment the first `Error` is seen, so this
/// is safe to use on infinite streams whose first failure is reachable.
pub fn collect_result(stream: Stream(Result(a, e))) -> Result(List(a), e) {
  do_collect_result(stream, [])
}

fn do_collect_result(
  stream: Stream(Result(a, e)),
  acc: List(a),
) -> Result(List(a), e) {
  case datastream.pull(stream) {
    Done -> Ok(list.reverse(acc))
    Next(Ok(value), rest) -> do_collect_result(rest, [value, ..acc])
    Next(Error(e), rest) -> {
      datastream.close(rest)
      Error(e)
    }
  }
}

/// Split a `Stream(Result(a, e))` into `#(oks, errors)`, both in
/// source order.
///
/// Drives the stream to completion: this is a full-traversal reducer,
/// not a short-circuiting one. Use `collect_result` when you want to
/// stop on the first failure.
pub fn partition_result(stream: Stream(Result(a, e))) -> #(List(a), List(e)) {
  let #(oks, errs) =
    fold(over: stream, from: #([], []), with: fn(acc, value) {
      let #(oks, errs) = acc
      case value {
        Ok(ok) -> #([ok, ..oks], errs)
        Error(err) -> #(oks, [err, ..errs])
      }
    })
  #(list.reverse(oks), list.reverse(errs))
}

/// Route each element through `split` and accumulate the two outputs
/// separately, both in source order.
///
/// `Ok(left)` lands in the first list, `Error(right)` in the second.
/// Drives the stream to completion.
pub fn partition_map(
  over stream: Stream(a),
  with split: fn(a) -> Result(b, c),
) -> #(List(b), List(c)) {
  let #(lefts, rights) =
    fold(over: stream, from: #([], []), with: fn(acc, element) {
      let #(lefts, rights) = acc
      case split(element) {
        Ok(left) -> #([left, ..lefts], rights)
        Error(right) -> #(lefts, [right, ..rights])
      }
    })
  #(list.reverse(lefts), list.reverse(rights))
}
