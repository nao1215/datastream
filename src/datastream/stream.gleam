//// Single-stream middle combinators over `Stream(a)`.
////
//// These are the lazy, order-preserving transforms whose names callers
//// already know from list-style APIs: `map`, `filter`, `take`, `drop`,
//// `take_while`, `drop_while`, and binary `append`. Multi-source
//// composition (`flat_map`, `flatten`, `concat`, `scan`, …) lives in a
//// later issue.
////
//// Building a pipeline with these combinators triggers no user
//// callbacks. Callbacks fire only when a terminal in `fold` or `sink`
//// pulls the result.
////
//// Early-exit combinators (`take`, `take_while`) stop pulling upstream
//// the moment their result is determined, which is what makes them safe
//// on infinite sources.

import datastream.{type Stream, Done, Next}
import gleam/option.{type Option, None, Some}

/// Apply `f` to every element. Cardinality and order are preserved.
pub fn map(over stream: Stream(a), with f: fn(a) -> b) -> Stream(b) {
  datastream.unfold(from: stream, with: fn(current) {
    case datastream.pull(current) {
      Next(element, rest) -> Next(element: f(element), state: rest)
      Done -> Done
    }
  })
}

/// Keep only the elements for which `predicate` returns `True`.
///
/// Relative order of the survivors is preserved.
pub fn filter(
  over stream: Stream(a),
  keeping predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.unfold(from: stream, with: fn(current) {
    do_filter(current, predicate)
  })
}

fn do_filter(stream: Stream(a), predicate: fn(a) -> Bool) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) ->
      case predicate(element) {
        True -> Next(element: element, state: rest)
        False -> do_filter(rest, predicate)
      }
    Done -> Done
  }
}

/// Yield at most the first `n` elements; `n <= 0` yields the empty stream.
///
/// Stops pulling upstream the moment the `n`th element has been emitted,
/// which is what makes `take` safe on infinite sources.
pub fn take(from stream: Stream(a), up_to n: Int) -> Stream(a) {
  datastream.unfold(from: #(n, stream), with: fn(state) {
    let #(remaining, current) = state
    case remaining <= 0 {
      True -> Done
      False ->
        case datastream.pull(current) {
          Next(element, rest) ->
            Next(element: element, state: #(remaining - 1, rest))
          Done -> Done
        }
    }
  })
}

/// Discard the first `n` elements; `n <= 0` is the identity.
pub fn drop(from stream: Stream(a), up_to n: Int) -> Stream(a) {
  datastream.unfold(from: #(n, stream), with: fn(state) {
    let #(remaining, current) = state
    do_drop(remaining, current)
  })
}

fn do_drop(remaining: Int, stream: Stream(a)) -> Step(a, #(Int, Stream(a))) {
  case remaining <= 0 {
    True ->
      case datastream.pull(stream) {
        Next(element, rest) -> Next(element: element, state: #(0, rest))
        Done -> Done
      }
    False ->
      case datastream.pull(stream) {
        Next(_, rest) -> do_drop(remaining - 1, rest)
        Done -> Done
      }
  }
}

/// Yield the longest prefix where `predicate` holds, then stop.
///
/// Stops pulling upstream as soon as `predicate` returns `False`, so
/// `take_while` terminates on infinite sources whose prefix eventually
/// fails the predicate.
pub fn take_while(
  in stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.unfold(from: stream, with: fn(current) {
    case datastream.pull(current) {
      Next(element, rest) ->
        case predicate(element) {
          True -> Next(element: element, state: rest)
          False -> Done
        }
      Done -> Done
    }
  })
}

/// Discard the longest prefix where `predicate` holds, then yield the rest.
pub fn drop_while(
  in stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.unfold(from: DropWhilePending(stream), with: fn(state) {
    drop_while_step(state, predicate)
  })
}

type DropWhile(a) {
  DropWhilePending(stream: Stream(a))
  DropWhileSettled(stream: Stream(a))
}

fn drop_while_step(
  state: DropWhile(a),
  predicate: fn(a) -> Bool,
) -> Step(a, DropWhile(a)) {
  case state {
    DropWhilePending(stream) ->
      case datastream.pull(stream) {
        Next(element, rest) ->
          case predicate(element) {
            True -> drop_while_step(DropWhilePending(rest), predicate)
            False -> Next(element: element, state: DropWhileSettled(rest))
          }
        Done -> Done
      }
    DropWhileSettled(stream) ->
      case datastream.pull(stream) {
        Next(element, rest) ->
          Next(element: element, state: DropWhileSettled(rest))
        Done -> Done
      }
  }
}

/// Yield all of `first`, then all of `second`. If `first` is infinite,
/// `second` is never reached.
pub fn append(first: Stream(a), second: Stream(a)) -> Stream(a) {
  datastream.unfold(from: AppendInFirst(first, second), with: append_step)
}

type Append(a) {
  AppendInFirst(first: Stream(a), second: Stream(a))
  AppendInSecond(stream: Stream(a))
}

fn append_step(state: Append(a)) -> Step(a, Append(a)) {
  case state {
    AppendInFirst(first, second) ->
      case datastream.pull(first) {
        Next(element, rest) ->
          Next(element: element, state: AppendInFirst(rest, second))
        Done -> append_step(AppendInSecond(second))
      }
    AppendInSecond(stream) ->
      case datastream.pull(stream) {
        Next(element, rest) ->
          Next(element: element, state: AppendInSecond(rest))
        Done -> Done
      }
  }
}

/// Apply `f` to each element and keep only the `Some(x)` results.
///
/// Use this rather than `filter` followed by `map` when the predicate
/// and the transform are the same decision: returning `None` discards
/// the element, `Some(x)` emits `x`. Errors should be carried as
/// element-shaped values, not as the discarded path.
pub fn filter_map(
  over stream: Stream(a),
  with f: fn(a) -> Option(b),
) -> Stream(b) {
  datastream.unfold(from: stream, with: fn(current) {
    do_filter_map(current, f)
  })
}

fn do_filter_map(stream: Stream(a), f: fn(a) -> Option(b)) -> Step(b, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) ->
      case f(element) {
        Some(value) -> Next(element: value, state: rest)
        None -> do_filter_map(rest, f)
      }
    Done -> Done
  }
}

/// Apply `f` to each element and concatenate the inner streams it
/// produces.
///
/// Observationally equivalent to `map(f) |> flatten`. Pulls one inner
/// stream at a time; the next inner stream is not constructed until the
/// previous one is exhausted.
pub fn flat_map(over stream: Stream(a), with f: fn(a) -> Stream(b)) -> Stream(b) {
  datastream.unfold(from: FlatMapPullingOuter(stream), with: fn(state) {
    flat_map_step(state, f)
  })
}

type FlatMap(a, b) {
  FlatMapPullingOuter(outer: Stream(a))
  FlatMapPullingInner(inner: Stream(b), outer: Stream(a))
}

fn flat_map_step(
  state: FlatMap(a, b),
  f: fn(a) -> Stream(b),
) -> Step(b, FlatMap(a, b)) {
  case state {
    FlatMapPullingOuter(outer) ->
      case datastream.pull(outer) {
        Next(element, outer_rest) ->
          flat_map_step(FlatMapPullingInner(f(element), outer_rest), f)
        Done -> Done
      }
    FlatMapPullingInner(inner, outer) ->
      case datastream.pull(inner) {
        Next(element, inner_rest) ->
          Next(element: element, state: FlatMapPullingInner(inner_rest, outer))
        Done -> flat_map_step(FlatMapPullingOuter(outer), f)
      }
  }
}

/// Flatten a stream of streams into a single stream.
///
/// `flatten(s)` is observationally equal to `flat_map(s, fn(x) { x })`.
pub fn flatten(streams: Stream(Stream(a))) -> Stream(a) {
  flat_map(over: streams, with: fn(inner) { inner })
}

/// Walk a `List(Stream(a))` in list order, yielding every element of
/// every stream exactly once.
///
/// Prefer this over a long left-associative `append` chain: pull-based
/// `append` chains can drift toward quadratic cost, while `concat`
/// walks the list once and pulls each stream lazily.
pub fn concat(streams: List(Stream(a))) -> Stream(a) {
  datastream.unfold(from: streams, with: concat_step)
}

fn concat_step(remaining: List(Stream(a))) -> Step(a, List(Stream(a))) {
  case remaining {
    [] -> Done
    [first, ..rest] ->
      case datastream.pull(first) {
        Next(element, first_rest) ->
          Next(element: element, state: [first_rest, ..rest])
        Done -> concat_step(rest)
      }
  }
}

/// Left-fold the stream, emitting one output per input.
///
/// The seed itself is NOT emitted, so `scan`'s output cardinality
/// equals its input cardinality. On `[x1, x2, x3]` with seed `s0` and
/// step `f`, the output is `[f(s0,x1), f(f(s0,x1),x2), f(f(f(s0,x1),x2),x3)]`.
/// On empty input the output is empty.
pub fn scan(
  over stream: Stream(a),
  from initial: b,
  with step: fn(b, a) -> b,
) -> Stream(b) {
  datastream.unfold(from: #(initial, stream), with: fn(state) {
    let #(acc, current) = state
    case datastream.pull(current) {
      Next(element, rest) -> {
        let new_acc = step(acc, element)
        Next(element: new_acc, state: #(new_acc, rest))
      }
      Done -> Done
    }
  })
}

/// Thread a state through the stream while emitting elements of a
/// possibly different type.
///
/// `step(acc, x)` returns `#(new_acc, output)`; `new_acc` becomes the
/// state for the next pull and `output` is emitted.
pub fn map_accum(
  over stream: Stream(a),
  from initial: state,
  with step: fn(state, a) -> #(state, b),
) -> Stream(b) {
  datastream.unfold(from: #(initial, stream), with: fn(s) {
    let #(acc, current) = s
    case datastream.pull(current) {
      Next(element, rest) -> {
        let #(new_acc, output) = step(acc, element)
        Next(element: output, state: #(new_acc, rest))
      }
      Done -> Done
    }
  })
}

/// Pair-wise zip two streams; halts the moment either source halts.
pub fn zip(left: Stream(a), right: Stream(b)) -> Stream(#(a, b)) {
  zip_with(left, right, with: fn(l, r) { #(l, r) })
}

/// Combine two streams element-wise with `combiner`; halts the moment
/// either source halts.
pub fn zip_with(
  left: Stream(a),
  right: Stream(b),
  with combiner: fn(a, b) -> c,
) -> Stream(c) {
  datastream.unfold(from: #(left, right), with: fn(state) {
    let #(l, r) = state
    case datastream.pull(l) {
      Done -> Done
      Next(left_element, left_rest) ->
        case datastream.pull(r) {
          Done -> Done
          Next(right_element, right_rest) ->
            Next(element: combiner(left_element, right_element), state: #(
              left_rest,
              right_rest,
            ))
        }
    }
  })
}

/// Insert `separator` between adjacent elements.
///
/// Empty and single-element streams are unchanged: `separator` is only
/// emitted between two real elements, never at the start, end, or
/// around an absent neighbour.
pub fn intersperse(over stream: Stream(a), with separator: a) -> Stream(a) {
  datastream.unfold(from: IntersperseInitial(stream), with: fn(state) {
    intersperse_step(state, separator)
  })
}

type Intersperse(a) {
  IntersperseInitial(stream: Stream(a))
  IntersperseAwaitingSep(stream: Stream(a))
  IntersperseHasPending(pending: a, stream: Stream(a))
}

fn intersperse_step(
  state: Intersperse(a),
  separator: a,
) -> Step(a, Intersperse(a)) {
  case state {
    IntersperseInitial(stream) ->
      case datastream.pull(stream) {
        Done -> Done
        Next(first, rest) ->
          Next(element: first, state: IntersperseAwaitingSep(rest))
      }
    IntersperseAwaitingSep(stream) ->
      case datastream.pull(stream) {
        Done -> Done
        Next(next, rest) ->
          Next(element: separator, state: IntersperseHasPending(next, rest))
      }
    IntersperseHasPending(pending, stream) ->
      Next(element: pending, state: IntersperseAwaitingSep(stream))
  }
}

/// Call `effect` once per element and re-emit the element unchanged.
///
/// Observation only: the element sequence is not altered. Allowed to
/// perform effects (logging, counters, debug breakpoints), but pure
/// pipelines should keep `tap` out of production paths.
pub fn tap(over stream: Stream(a), with effect: fn(a) -> Nil) -> Stream(a) {
  datastream.unfold(from: stream, with: fn(current) {
    case datastream.pull(current) {
      Next(element, rest) -> {
        effect(element)
        Next(element: element, state: rest)
      }
      Done -> Done
    }
  })
}

/// Collapse runs of `==`-equal adjacent values to a single occurrence.
///
/// Local only: values that appear far apart are kept. Full
/// deduplication would require unbounded state, which the pull-based
/// core does not maintain.
pub fn dedupe_adjacent(stream: Stream(a)) -> Stream(a) {
  datastream.unfold(from: #(None, stream), with: dedupe_step)
}

fn dedupe_step(
  state: #(Option(a), Stream(a)),
) -> Step(a, #(Option(a), Stream(a))) {
  let #(last, current) = state
  case datastream.pull(current) {
    Done -> Done
    Next(element, rest) ->
      case last {
        Some(prev) if prev == element -> dedupe_step(#(Some(prev), rest))
        _ -> Next(element: element, state: #(Some(element), rest))
      }
  }
}

type Step(a, state) =
  datastream.Step(a, state)
