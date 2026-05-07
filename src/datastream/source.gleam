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
import gleam/bit_array
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

/// Stop-EXCLUSIVE integer range.
///
/// Counts up by 1 when `start < stop`, down by 1 when `start > stop`,
/// and is empty when `start == stop`. Step-by-`n` sequences belong in
/// `iterate` or `unfold`.
///
/// Examples:
///
/// ```gleam
/// source.range(from: 1, to: 5) |> fold.to_list  // [1, 2, 3, 4]
/// source.range(from: 0, to: 0) |> fold.to_list  // []
/// source.range(from: 5, to: 1) |> fold.to_list  // [5, 4, 3, 2]
/// ```
///
/// ## Surprising behaviour: argument order signals direction
///
/// `range` is **direction-implicit**: passing `from > to` produces a
/// **descending** stream, not the empty stream. Callers who think they
/// are guarding against negative iteration with
/// `source.range(from: end, to: start)` get a backwards stream of
/// indices instead.
///
/// Pick the variant that matches your intent:
///
/// - Want **ascending only** and an explicit failure on swapped
///   arguments? Use `range_strict` — it returns
///   `Error(DescendingNotAllowed)` when `from > to`.
/// - Want **descending only** and an explicit failure on swapped
///   arguments? Use `range_descending` — it returns
///   `Error(AscendingNotAllowed)` when `from < to`.
/// - Don't care about direction? Stick with `range`.
///
/// ## Stop-EXCLUSIVE vs. stop-INCLUSIVE in the stdlib
///
/// Stdlib has two `range` shapes with different conventions:
///
/// - `gleam/int.range` (fold form) is **stop-EXCLUSIVE**, matching this
///   function. `int.range` also rejects `from > to` by yielding the
///   empty fold rather than reversing direction.
/// - `gleam/list.range` is **stop-INCLUSIVE** *and* direction-implicit:
///   `list.range(1, 5)` yields `[1, 2, 3, 4, 5]`, `list.range(5, 1)`
///   yields `[5, 4, 3, 2, 1]`.
/// - `gleam/yielder.range` (pull-based stream form) is
///   **stop-INCLUSIVE** — `yielder.range(from: 1, to: 5)` yields
///   `[1, 2, 3, 4, 5]` and `yielder.range(from: 0, to: 0)` yields
///   `[0]`, not the empty list.
///
/// `Stream(a)` is the closer analog of `Yielder(a)`, so be aware of
/// the off-by-one when porting code between the two. Add `+ 1` to
/// `to:` (or use `iterate` / `unfold` for full control) when you want
/// inclusive semantics.
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

/// Reasons a directional range constructor can refuse the inputs.
///
/// `DescendingNotAllowed` is returned by `range_strict` when
/// `from > to`. `AscendingNotAllowed` is returned by `range_descending`
/// when `from < to`. Both keep `from == to` as the empty-stream case
/// (success) so callers do not have to special-case zero-width ranges.
pub type RangeError {
  DescendingNotAllowed
  AscendingNotAllowed
}

/// Stop-EXCLUSIVE **ascending-only** integer range.
///
/// Mirrors `range` for `from <= to` and rejects `from > to` with
/// `Error(DescendingNotAllowed)`. Reach for this when "swapped
/// arguments" should be a hard error rather than a silent backwards
/// stream — the typical case when `from`/`to` are derived from caller
/// state (buffer indices, time bounds, paginated offsets).
///
/// `from == to` produces `Ok(empty())`, matching `range`'s zero-width
/// behaviour.
///
/// Examples:
///
/// ```gleam
/// source.range_strict(from: 1, to: 5)
/// // Ok(<stream of [1, 2, 3, 4]>)
///
/// source.range_strict(from: 3, to: 3)
/// // Ok(<empty stream>)
///
/// source.range_strict(from: 5, to: 1)
/// // Error(DescendingNotAllowed)
/// ```
pub fn range_strict(
  from start: Int,
  to stop: Int,
) -> Result(Stream(Int), RangeError) {
  case start > stop {
    True -> Error(DescendingNotAllowed)
    False -> Ok(range(from: start, to: stop))
  }
}

/// Stop-EXCLUSIVE **descending-only** integer range.
///
/// Mirrors `range` for `from >= to` and rejects `from < to` with
/// `Error(AscendingNotAllowed)`. Use this when descending iteration is
/// the deliberate intent (reverse traversal, countdowns) and an
/// accidentally ascending pair of inputs should fail loudly.
///
/// `from == to` produces `Ok(empty())`, matching `range`'s zero-width
/// behaviour.
///
/// Examples:
///
/// ```gleam
/// source.range_descending(from: 5, to: 1)
/// // Ok(<stream of [5, 4, 3, 2]>)
///
/// source.range_descending(from: 3, to: 3)
/// // Ok(<empty stream>)
///
/// source.range_descending(from: 1, to: 5)
/// // Error(AscendingNotAllowed)
/// ```
pub fn range_descending(
  from start: Int,
  to stop: Int,
) -> Result(Stream(Int), RangeError) {
  case start < stop {
    True -> Error(AscendingNotAllowed)
    False -> Ok(range(from: start, to: stop))
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
///
/// The input MUST be byte-aligned (its bit length MUST be a multiple
/// of 8). A non-byte-aligned input is rejected at construction time
/// with a panic, matching the policy `binary.length_prefixed` and
/// `binary.fixed_size` already use for invalid arguments — silently
/// dropping the trailing sub-byte tail would be data loss the caller
/// could not observe.
pub fn from_bit_array(bytes: BitArray) -> Stream(Int) {
  case bit_array.bit_size(bytes) % 8 {
    0 ->
      unfold(from: bytes, with: fn(remaining) {
        case remaining {
          <<byte:size(8), rest:bits>> -> Next(element: byte, state: rest)
          _ -> Done
        }
      })
    _ -> panic as "datastream/source.from_bit_array: input must be byte-aligned"
  }
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

/// Unified failure shape for `try_resource`.
///
/// `OpenError(e)` carries the error returned by `open`; the stream
/// emits exactly one of these and halts. `NextError(e)` wraps a
/// per-element read error returned by `next`; the stream continues
/// after one of these unless the caller stops it.
///
/// Pair with `fold.collect_result` to stop on the first error, or
/// with `fold.partition_result` to drive the stream to completion
/// and split successes from errors.
pub type ResourceError(open_error, next_error) {
  OpenError(open_error)
  NextError(next_error)
}

/// Build a resource-backed stream that opens lazily and closes
/// deterministically.
///
/// `open` runs on the first pull (NOT at construction), so holding a
/// `Stream` value never holds a real-world handle until evaluation
/// begins. Each terminal call re-runs `open`: a `Stream` is a pipeline
/// definition, not a one-shot iterator.
///
/// `close` is honoured on every termination path the library controls
/// — *provided `open` has run*. With lazy-open semantics, terminals
/// that never pull (`stream.take(up_to: 0)`, an `interrupt_when`
/// signal that fires before the first pull, etc.) skip both `open`
/// and `close`: there is no opened state, so there is nothing to
/// close. A close callback that needs to run unconditionally must be
/// arranged at a higher level than the `Stream`.
///
/// When `open` *has* run, the termination paths covered are: normal
/// end (when `next` returns `Done`), downstream early-exit
/// (`stream.take`, `stream.take_while`), early-exit folds
/// (`fold.first`, `fold.find`, `fold.any`, `fold.all`,
/// `fold.collect_result`), and `sink.try_each` failure. Termination
/// caused by user-code panicking is best-effort.
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

/// Build a resource-backed stream whose `open` and per-element `next`
/// can both fail.
///
/// The two failure shapes are unified into the element type via
/// `ResourceError`:
///
/// - When `open` returns `Error(e)`, the stream emits exactly one
///   element `Error(OpenError(e))` and halts. `close` is NOT called
///   (there is no opened state to close).
/// - When `open` returns `Ok(state)`, behaviour follows the contract
///   of `resource`. `next` returning `Next(Ok(x), state')` yields
///   element `Ok(x)`; `Next(Error(e), state')` yields
///   `Error(NextError(e))` and continues; `Done` halts and triggers
///   `close(state)`.
///
/// Lazy: `open` runs on the first pull, NOT at construction. Each
/// terminal call re-attempts `open`. As with `resource`, terminals
/// that never trigger a pull (`stream.take(up_to: 0)` and similar)
/// skip both `open` and `close`.
///
/// Downstream sees a single `Stream(Result(a, ResourceError(o, n)))`,
/// so `fold.collect_result` and `fold.partition_result` work without
/// further adaptation.
///
/// `open` is responsible for rolling back any partial acquisition
/// itself: when `open` returns `Error(e)`, the library has no state
/// to close.
pub fn try_resource(
  open open: fn() -> Result(state, open_error),
  next next: fn(state) -> Step(Result(a, next_error), state),
  close close: fn(state) -> Nil,
) -> Stream(Result(a, ResourceError(open_error, next_error))) {
  datastream.try_resource(
    open: open,
    next: fn(state) {
      case next(state) {
        Done -> Done
        Next(Ok(value), next_state) -> Next(Ok(value), next_state)
        Next(Error(e), next_state) -> Next(Error(NextError(e)), next_state)
      }
    },
    close: close,
    on_open_error: fn(e) { Error(OpenError(e)) },
  )
}
