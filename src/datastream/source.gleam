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
import gleam/dict.{type Dict}
import gleam/option.{type Option, None, Some}

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

/// Lift an `Option(a)` to a stream: `Some(x)` becomes a one-element
/// stream, `None` becomes the empty stream.
pub fn from_option(option: Option(a)) -> Stream(a) {
  case option {
    Some(value) -> once(value)
    None -> empty()
  }
}

/// Lift a `Result(a, e)` to a one-element `Stream(Result(a, e))`.
///
/// The `Error(e)` case is preserved as a single element rather than
/// collapsed to the empty stream so the failure information survives
/// through the pipeline. Callers that want to drop the error can chain
/// a later `filter_map` or `collect_result`.
pub fn from_result(result: Result(a, e)) -> Stream(Result(a, e)) {
  once(result)
}

/// Yield each `#(key, value)` entry of a `Dict` exactly once.
///
/// Iteration order is not specified — it follows whatever order
/// `dict.to_list` returns for the underlying target. Callers that need
/// a deterministic order should funnel through `dict.to_list` and sort
/// before constructing the stream.
pub fn from_dict(d: Dict(k, v)) -> Stream(#(k, v)) {
  d |> dict.to_list |> from_list
}

/// Yield each byte of a `BitArray` in order as an `Int` in `0..255`.
///
/// An empty `BitArray` produces the empty stream. This is the natural
/// shape for the upcoming `binary` and `text.utf8_decode` work.
pub fn from_bit_array(bytes: BitArray) -> Stream(Int) {
  unfold(from: bytes, with: fn(remaining) {
    case remaining {
      <<byte:size(8), rest:bits>> -> Next(element: byte, state: rest)
      _ -> Done
    }
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

/// Build a resource-backed stream that opens lazily and closes
/// deterministically.
///
/// `open` runs on the first pull (NOT at construction), so holding a
/// `Stream` value never holds a real-world handle until evaluation
/// begins. Each terminal call re-runs `open`: a `Stream` is a pipeline
/// definition, not a one-shot iterator.
///
/// `close` is honoured on every termination path the library controls:
/// normal end (when `next` returns `Done`), downstream early-exit
/// (`stream.take`, `stream.take_while`), early-exit folds (`fold.first`,
/// `fold.find`, `fold.any`, `fold.all`, `fold.collect_result`), and
/// `sink.try_each` failure. Termination caused by user-code panicking
/// is best-effort.
///
/// `close` returns `Nil`. Errors that happen at close time are not
/// propagated through the terminal's return value; callers that must
/// observe close failures should use a sink that owns the lifecycle.
pub fn resource(
  open open: fn() -> state,
  next next: fn(state) -> Step(a, state),
  close close: fn(state) -> Nil,
) -> Stream(a) {
  datastream.resource(open: open, next: next, close: close)
}
