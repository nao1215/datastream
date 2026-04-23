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

type Step(a, state) =
  datastream.Step(a, state)
