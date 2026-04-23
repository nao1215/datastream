//// Stream constructors: every public way to *produce* a `Stream(a)`.
////
//// The constructors split into two kinds:
////
//// - **Adapters** wrap an existing data structure: `from_list`,
////   `once`, `empty`.
//// - **Generative** constructors build a stream from a rule: `range`,
////   `repeat`, `iterate`, `unfold`.
////
//// `repeat` and `iterate` are infinite by design; pair them with
//// `stream.take` / `stream.take_while` (or `fold.first` / `fold.find`)
//// to terminate. `unfold`'s step is expected to be pure — effectful
//// generators belong in the resource constructors introduced later.
////
//// Every constructor produces a stream that re-runs from the beginning
//// on every terminal call: there is no implicit caching.

import datastream.{type Step, type Stream, Done, Next}

/// A stream that yields no elements.
pub fn empty() -> Stream(a) {
  datastream.unfold(from: Nil, with: fn(_) { Done })
}

/// A stream that yields exactly one element and then stops.
pub fn once(value: a) -> Stream(a) {
  datastream.unfold(from: True, with: fn(first) {
    case first {
      True -> Next(element: value, state: False)
      False -> Done
    }
  })
}

/// A stream that yields the elements of `list` in order.
pub fn from_list(list: List(a)) -> Stream(a) {
  datastream.unfold(from: list, with: fn(remaining) {
    case remaining {
      [] -> Done
      [head, ..tail] -> Next(element: head, state: tail)
    }
  })
}

/// Stop-exclusive integer range.
///
/// Counts up by 1 when `start < stop`, down by 1 when `start > stop`,
/// and is empty when `start == stop`. Step-by-`n` sequences belong in
/// `iterate` or `unfold`, keeping `range` aligned with the surrounding
/// stdlib's `range`.
pub fn range(from start: Int, to stop: Int) -> Stream(Int) {
  case start == stop {
    True -> empty()
    False -> {
      let direction = case start < stop {
        True -> 1
        False -> -1
      }
      datastream.unfold(from: start, with: fn(current) {
        case current == stop {
          True -> Done
          False -> Next(element: current, state: current + direction)
        }
      })
    }
  }
}

/// A stream that yields `value` infinitely.
///
/// Pair with `stream.take` (or any early-exit terminal) to bound the
/// pull count.
pub fn repeat(value: a) -> Stream(a) {
  datastream.unfold(from: value, with: fn(state) {
    Next(element: state, state: state)
  })
}

/// Iterate `next` from `seed`: yields `seed`, `next(seed)`,
/// `next(next(seed))`, … infinitely.
///
/// Pair with `stream.take` / `stream.take_while` to terminate.
pub fn iterate(from seed: a, with next: fn(a) -> a) -> Stream(a) {
  datastream.unfold(from: seed, with: fn(state) {
    Next(element: state, state: next(state))
  })
}

/// Build a stream from an initial state and a step function.
///
/// `step` is invoked once per pull; returning `Next(element, next)`
/// emits the element and supplies the state for the following pull,
/// returning `Done` ends the stream. The state type is hidden in the
/// produced `Stream(a)` so callers can pick whatever shape suits the
/// generator.
pub fn unfold(
  from initial: state,
  with step: fn(state) -> Step(a, state),
) -> Stream(a) {
  datastream.unfold(from: initial, with: step)
}
