//// Process streams of values, text, and bytes one element at a time.
////
//// Build a source with `from_list`, `unfold`, or `resource`, transform it with operations such as `map` and `take`, and consume it with a terminal such as `to_list` or `each`. Construction is lazy unless a function documents otherwise.
////
//// Ordinary pipelines start a new run for each terminal call. Shared consumers from `broadcast`, `broadcast_bounded`, and `unzip` are single-use. On Erlang, pull them in the process that created them.
////
//// ## Resources
////
//// `resource` and `try_resource` open on the first pull and close on completion or early termination. Typed failures such as a binary framing error also close the active upstream. Cleanup after a user callback raises is best-effort; its completion and call count are not guaranteed.
////
//// ## Targets and workers
////
//// The synchronous core runs on Erlang and JavaScript. Parallel, Subject, and time operations require Erlang. The JavaScript async iterator adapter is described in the [JavaScript guide](javascript.html).
////
//// Parallel map and each pull their source in the caller and run callbacks in workers. `merge`, `race`, time operators, and `timeout` pull sources inside workers, so those sources must not depend on the caller's process. Stopping a source worker requests cleanup after any pull callback in progress returns; a callback that never returns can delay cleanup indefinitely. Worker exceptions propagate after cleanup is attempted.

import datastream/internal/ref.{type Ref}
import gleam/bit_array
import gleam/bytes_tree
import gleam/dict.{type Dict}
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/string_tree.{type StringTree}

@target(erlang)
import gleam/dynamic.{type Dynamic}

@target(erlang)
import gleam/erlang/atom.{type Atom}

@target(erlang)
import gleam/erlang/process.{type Monitor, type Pid, type Subject}

/// Describe an invalid argument detected before consuming a stream.
///
/// Construction failure leaves the source untouched. `function` and `argument` identify the public function and argument that rejected `given`.
pub type ArgumentError {
  NotPositive(function: String, argument: String, given: Int)
  InvalidPrefixSize(given: Int)
  NegativeFrameLimit(given: Int)
  BufferLessThanWorkers(max_workers: Int, max_buffer: Int)
  InvalidDuration(function: String, milliseconds: Int)
  EmptyDelimiter
  UnalignedArgument(argument: String, bit_size: Int)
}

/// Describe a bounded broadcast queue overflow.
///
/// `consumer` is the zero-based index of the first overflowing active consumer. The broadcast discards queued values and closes its upstream before reporting the error to each remaining consumer.
pub type BroadcastError {
  QueueLimitExceeded(consumer: Int, max_queue: Int)
}

/// Create a stream with no elements.
pub fn empty() -> Stream(a) {
  make(pull: fn() { Done }, close: noop)
}

/// Describe a lazy stream of elements.
///
/// Ordinary pipelines start a new run for each terminal operation. Consumers returned by `broadcast`, `broadcast_bounded`, and `unzip` share one run and are single-use.
///
/// Construct streams with `unfold`, `resource`, or the other source functions.
pub opaque type Stream(a) {
  Stream(pull: fn() -> Step(a, Stream(a)), close: fn() -> Nil)
}

/// Return one element and the state for the next pull, or finish a source.
///
/// Return `Next(element, state)` or `Done` from an `unfold` or resource callback. After `Done`, the library does not call that source again. Represent fallible elements with `Result` in the element type.
pub type Step(a, state) {
  Next(element: a, state: state)
  Done
}

/// Build a stream from an initial state and a step function.
///
/// Each pull calls `step` with the current state. `Next` emits an element and supplies the next state; `Done` ends the stream. Use `resource` when the state needs cleanup.
pub fn unfold(from state: s, with step: fn(s) -> Step(a, s)) -> Stream(a) {
  Stream(
    pull: fn() {
      case step(state) {
        Next(element, next_state) ->
          Next(element, unfold(from: next_state, with: step))
        Done -> Done
      }
    },
    close: noop,
  )
}

/// Build a stream from explicit `pull` and `close` callbacks.
///
/// Used by combinators that need to forward the upstream's `close` to
/// downstream consumers, and by `resource` to expose a real-world
/// handle as a stream. The `close` callback is called by terminals or
/// combinators that interrupt evaluation before the source returns
/// `Done`; combinators that observe a `Done` already know the upstream
/// has closed itself, so they MUST NOT call `close` again on that path.
///
fn make(
  pull pull: fn() -> Step(a, Stream(a)),
  close close: fn() -> Nil,
) -> Stream(a) {
  Stream(pull: pull, close: close)
}

/// Build a stream with open, next, and close callbacks.
///
/// `open` runs on the first pull. Each terminal operation starts a fresh run. A pipeline that is never pulled, including one consumed through `take(up_to: 0)`, leaves the resource unopened.
///
/// When `next` returns `Done`, `close` receives the current state. Early termination through operations such as `first`, `take`, or `try_each` closes the latest continuation once. User callbacks that panic have best-effort cleanup; neither cleanup completion nor its call count is guaranteed in that case.
///
/// `close` returns `Nil`, so its failures cannot be returned through a terminal's result. Manage the lifecycle outside the stream if the application must receive a typed close error.
pub fn resource(
  open open: fn() -> s,
  next next: fn(s) -> Step(a, s),
  close close: fn(s) -> Nil,
) -> Stream(a) {
  Stream(
    pull: fn() {
      let initial = open()
      pull_resource(initial, next, close)
    },
    close: noop,
  )
}

/// Variant of `resource` whose `open` may fail. On `Error(e)` the
/// stream emits exactly one element produced by `on_open_error(e)`
/// and halts; `close` is NOT called (no opened state exists, so the
/// `open` callback is responsible for rolling back any partial
/// acquisition). On `Ok(state)` behaviour follows `resource`.
///
/// Used by `try_resource` to wrap an opening failure as a stream element.
fn resource_with_open_error(
  open open: fn() -> Result(s, open_error),
  next next: fn(s) -> Step(a, s),
  close close: fn(s) -> Nil,
  on_open_error on_open_error: fn(open_error) -> a,
) -> Stream(a) {
  Stream(
    pull: fn() {
      case open() {
        Ok(state) -> pull_resource(state, next, close)
        Error(e) ->
          Next(on_open_error(e), Stream(pull: fn() { Done }, close: noop))
      }
    },
    close: noop,
  )
}

fn pull_resource(
  state: s,
  next: fn(s) -> Step(a, s),
  close: fn(s) -> Nil,
) -> Step(a, Stream(a)) {
  case next(state) {
    Done -> {
      close(state)
      Done
    }
    Next(element, next_state) ->
      Next(
        element,
        Stream(
          pull: fn() { pull_resource(next_state, next, close) },
          close: fn() { close(next_state) },
        ),
      )
  }
}

/// Pull one element from a stream.
///
/// Returns `Next(element, next_stream)` or `Done`. After `Done` is
/// observed the same stream MUST NOT be pulled again; doing so is a
/// programmer error and the result is unspecified.
///
/// Terminal operations call this helper after taking ownership of a continuation.
fn pull(stream: Stream(a)) -> Step(a, Stream(a)) {
  stream.pull()
}

/// Close a stream's underlying resource, if any.
///
/// Called by combinators / terminals on the *interruption* paths
/// (early-exit, fail-fast). Streams that have already observed `Done`
/// are closed by the source itself, so `close` MUST NOT be called by
/// callers on that path.
///
/// For pure (`unfold`-built) streams `close` is a no-op.
///
fn close(stream: Stream(a)) -> Nil {
  stream.close()
}

fn noop() -> Nil {
  Nil
}

/// Create a stream containing one value.
pub fn once(value: a) -> Stream(a) {
  unfold(from: True, with: fn(first) {
    case first {
      True -> Next(element: value, state: False)
      False -> Done
    }
  })
}

/// Yield the values of a list in order.
pub fn from_list(list: List(a)) -> Stream(a) {
  unfold(from: list, with: fn(remaining) {
    case remaining {
      [] -> Done
      [head, ..tail] -> Next(element: head, state: tail)
    }
  })
}

/// Yield integers from `start` toward `stop`, excluding `stop`.
///
/// The step is one for an ascending range and minus one for a descending range. Equal endpoints produce an empty stream.
///
/// ## Examples
///
/// ```gleam
/// range(from: 1, to: 4) |> to_list
/// // [1, 2, 3]
/// range(from: 4, to: 1) |> to_list
/// // [4, 3, 2]
/// ```
pub fn range(from start: Int, to stop: Int) -> Stream(Int) {
  case start == stop {
    True -> empty()
    False -> {
      let direction = case start < stop {
        True -> 1
        False -> -1
      }
      unfold(from: start, with: fn(current) {
        case current == stop {
          True -> Done
          False -> Next(element: current, state: current + direction)
        }
      })
    }
  }
}

/// Describe a range whose direction does not match the requested constructor.
pub type RangeError {
  DescendingNotAllowed
  AscendingNotAllowed
}

/// Create an ascending range, excluding `stop`.
///
/// Return `Error(DescendingNotAllowed)` when `start > stop`. Equal endpoints produce an empty stream.
pub fn range_strict(
  from start: Int,
  to stop: Int,
) -> Result(Stream(Int), RangeError) {
  case start > stop {
    True -> Error(DescendingNotAllowed)
    False -> Ok(range(from: start, to: stop))
  }
}

/// Create a descending range, excluding `stop`.
///
/// Return `Error(AscendingNotAllowed)` when `start < stop`. Equal endpoints produce an empty stream.
pub fn range_descending(
  from start: Int,
  to stop: Int,
) -> Result(Stream(Int), RangeError) {
  case start < stop {
    True -> Error(AscendingNotAllowed)
    False -> Ok(range(from: start, to: stop))
  }
}

/// Yield the same value indefinitely.
///
/// Use an early-ending operation such as `take` to bound consumption.
pub fn repeat(value: a) -> Stream(a) {
  unfold(from: value, with: fn(state) { Next(element: state, state: state) })
}

/// Yield `seed`, then repeatedly apply `next` to produce the following value.
///
/// The stream is infinite. `next` runs only as elements are requested.
pub fn iterate(from seed: a, with next: fn(a) -> a) -> Stream(a) {
  unfold(from: seed, with: fn(state) {
    Next(element: state, state: next(state))
  })
}

/// Yield the value in `Some`, or no elements for `None`.
pub fn from_option(option: Option(a)) -> Stream(a) {
  case option {
    Some(value) -> once(value)
    None -> empty()
  }
}

/// Yield each dictionary entry as a key-value tuple.
///
/// Entry order is unspecified. Sort `dict.to_list` before calling `from_list` when a particular order is required.
pub fn from_dict(dictionary: Dict(k, v)) -> Stream(#(k, v)) {
  dictionary |> dict.to_list |> from_list
}

/// Yield each byte as an integer from 0 to 255.
///
/// Return `Error(UnalignedArgument(...))` when the input's bit count is not divisible by eight. Empty input creates an empty stream.
pub fn from_bit_array(bytes: BitArray) -> Result(Stream(Int), ArgumentError) {
  case bit_array.bit_size(bytes) % 8 {
    0 ->
      Ok(
        unfold(from: bytes, with: fn(remaining) {
          case remaining {
            <<byte:size(8), rest:bits>> -> Next(element: byte, state: rest)
            _ -> Done
          }
        }),
      )
    _ ->
      Error(UnalignedArgument(
        argument: "bytes",
        bit_size: bit_array.bit_size(bytes),
      ))
  }
}

/// Distinguish an opening failure from a reading failure.
///
/// `OpenFailed` ends the stream after one error without calling `close`. `ReadFailed` wraps one element's error and allows subsequent reads. Use `collect_result` to stop at the first error.
pub type ResourceError(open_error, next_error) {
  OpenFailed(reason: open_error)
  ReadFailed(reason: next_error)
}

/// Build a resource stream whose opening and reading callbacks can fail.
///
/// Opening is lazy. `Error(reason)` from `open` emits one `Error(OpenFailed(reason))` and ends without calling `close`. The opening callback must undo any partial acquisition itself.
///
/// Once opened, `Next(Ok(value), state)` emits a value and `Next(Error(reason), state)` emits `Error(ReadFailed(reason))`. Both continue with the supplied state. `Done` and early termination follow `resource`'s cleanup contract. `collect_result` stops at the first error; `partition_result` consumes all elements.
pub fn try_resource(
  open open: fn() -> Result(state, open_error),
  next next: fn(state) -> Step(Result(a, next_error), state),
  close close: fn(state) -> Nil,
) -> Stream(Result(a, ResourceError(open_error, next_error))) {
  resource_with_open_error(
    open: open,
    next: fn(state) {
      case next(state) {
        Done -> Done
        Next(Ok(value), next_state) -> Next(Ok(value), next_state)
        Next(Error(e), next_state) -> Next(Error(ReadFailed(e)), next_state)
      }
    },
    close: close,
    on_open_error: fn(e) { Error(OpenFailed(e)) },
  )
}

// --- single-stream combinators ----------------------------------------------

/// Apply `transform` to each element, preserving order.
pub fn map(over stream: Stream(a), with transform: fn(a) -> b) -> Stream(b) {
  make(
    pull: fn() {
      case pull(stream) {
        Next(element, rest) ->
          Next(transform(element), map(over: rest, with: transform))
        Done -> Done
      }
    },
    close: fn() { close(stream) },
  )
}

/// Keep elements for which `predicate` returns `True`, preserving order.
pub fn filter(
  over stream: Stream(a),
  keeping predicate: fn(a) -> Bool,
) -> Stream(a) {
  make(pull: fn() { stream_filter_pull(stream, predicate) }, close: fn() {
    close(stream)
  })
}

fn stream_filter_pull(
  stream: Stream(a),
  predicate: fn(a) -> Bool,
) -> Step(a, Stream(a)) {
  case pull(stream) {
    Next(element, rest) ->
      case predicate(element) {
        True -> Next(element, filter(over: rest, keeping: predicate))
        False -> stream_filter_pull(rest, predicate)
      }
    Done -> Done
  }
}

/// Yield at most `count` elements, then close the remaining source.
///
/// Zero or negative counts produce an empty stream. Consuming that stream does not open a fresh lazy resource. For an already active shared broadcast consumer, it releases that consumer when pulled.
pub fn take(over stream: Stream(a), up_to count: Int) -> Stream(a) {
  case count < 0 {
    True -> stream_take_active(stream, 0)
    False -> stream_take_active(stream, count)
  }
}

fn stream_take_active(stream: Stream(a), count: Int) -> Stream(a) {
  make(
    pull: fn() {
      case count {
        0 -> {
          close(stream)
          Done
        }
        _ ->
          case pull(stream) {
            Next(element, rest) ->
              Next(element, stream_take_active(rest, count - 1))
            Done -> Done
          }
      }
    },
    close: fn() { close(stream) },
  )
}

/// Discard the first `count` elements and yield the rest.
///
/// Zero or negative counts return the original stream. Construction does not open or pull the source.
pub fn drop(over stream: Stream(a), up_to count: Int) -> Stream(a) {
  case count < 0 {
    True -> stream_drop_active(stream, 0)
    False -> stream_drop_active(stream, count)
  }
}

fn stream_drop_active(stream: Stream(a), count: Int) -> Stream(a) {
  make(pull: fn() { stream_drop_pull(stream, count) }, close: fn() {
    close(stream)
  })
}

fn stream_drop_pull(stream: Stream(a), count: Int) -> Step(a, Stream(a)) {
  case count {
    0 ->
      case pull(stream) {
        Next(element, rest) -> Next(element, stream_drop_active(rest, 0))
        Done -> Done
      }
    _ ->
      case pull(stream) {
        Next(_, rest) -> stream_drop_pull(rest, count - 1)
        Done -> Done
      }
  }
}

/// Yield elements until `predicate` first returns `False`.
///
/// The failing element is consumed but not emitted. The remaining source is closed immediately.
pub fn take_while(
  over stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  make(
    pull: fn() {
      case pull(stream) {
        Next(element, rest) ->
          case predicate(element) {
            True -> Next(element, take_while(over: rest, satisfying: predicate))
            False -> {
              close(rest)
              Done
            }
          }
        Done -> Done
      }
    },
    close: fn() { close(stream) },
  )
}

/// Discard the initial elements satisfying `predicate`, then yield the rest.
pub fn drop_while(
  over stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  make(pull: fn() { stream_drop_while_pull(stream, predicate) }, close: fn() {
    close(stream)
  })
}

fn stream_drop_while_pull(
  stream: Stream(a),
  predicate: fn(a) -> Bool,
) -> Step(a, Stream(a)) {
  case pull(stream) {
    Next(element, rest) ->
      case predicate(element) {
        True -> stream_drop_while_pull(rest, predicate)
        False -> Next(element, rest)
      }
    Done -> Done
  }
}

/// Yield all elements of `first`, followed by all elements of `second`.
///
/// The second source starts only after the first finishes. If the first is infinite, the second is never opened. Early termination closes the active source.
pub fn append(first: Stream(a), second: Stream(a)) -> Stream(a) {
  make(pull: fn() { stream_append_pull(first, second) }, close: fn() {
    close(first)
  })
}

fn stream_append_pull(
  first: Stream(a),
  second: Stream(a),
) -> Step(a, Stream(a)) {
  case pull(first) {
    Next(element, rest) -> Next(element, append(rest, second))
    Done -> stream_append_pull_second(second)
  }
}

fn stream_append_pull_second(stream: Stream(a)) -> Step(a, Stream(a)) {
  case pull(stream) {
    Next(element, rest) -> Next(element, rest)
    Done -> Done
  }
}

// --- composition combinators ------------------------------------------------

/// Apply `transform` and emit each `Ok` value, skipping errors.
///
/// ## Examples
///
/// ```gleam
/// from_list(["1", "bad", "3"])
/// |> filter_map(with: int.parse)
/// |> to_list
/// // [1, 3]
/// ```
pub fn filter_map(
  over stream: Stream(a),
  with transform: fn(a) -> Result(b, error),
) -> Stream(b) {
  make(pull: fn() { stream_filter_map_pull(stream, transform) }, close: fn() {
    close(stream)
  })
}

fn stream_filter_map_pull(
  stream: Stream(a),
  transform: fn(a) -> Result(b, error),
) -> Step(b, Stream(b)) {
  case pull(stream) {
    Next(element, rest) ->
      case transform(element) {
        Ok(value) -> Next(value, filter_map(over: rest, with: transform))
        Error(_) -> stream_filter_map_pull(rest, transform)
      }
    Done -> Done
  }
}

/// Apply `transform` to each element and concatenate the resulting streams.
///
/// Consume one inner stream completely before constructing the next. On early termination, close the active inner stream before the outer stream.
pub fn flat_map(
  over stream: Stream(a),
  with transform: fn(a) -> Stream(b),
) -> Stream(b) {
  stream_flat_map_active(None, stream, transform)
}

fn stream_flat_map_active(
  inner: Option(Stream(b)),
  outer: Stream(a),
  transform: fn(a) -> Stream(b),
) -> Stream(b) {
  make(
    pull: fn() { stream_flat_map_pull(inner, outer, transform) },
    close: fn() {
      case inner {
        Some(inner) -> close(inner)
        None -> Nil
      }
      close(outer)
    },
  )
}

// One self-recursive loop over both streams, so the JavaScript target
// runs it as a loop instead of growing the stack per empty inner.
fn stream_flat_map_pull(
  inner: Option(Stream(b)),
  outer: Stream(a),
  transform: fn(a) -> Stream(b),
) -> Step(b, Stream(b)) {
  case inner {
    Some(current) ->
      case pull(current) {
        Next(value, rest) ->
          Next(value, stream_flat_map_active(Some(rest), outer, transform))
        Done -> stream_flat_map_pull(None, outer, transform)
      }
    None ->
      case pull(outer) {
        Done -> Done
        Next(element, outer_rest) ->
          stream_flat_map_pull(Some(transform(element)), outer_rest, transform)
      }
  }
}

/// Concatenate a stream of streams in order.
///
/// Use the same consumption and cleanup order as `flat_map`.
pub fn flatten(streams: Stream(Stream(a))) -> Stream(a) {
  flat_map(over: streams, with: fn(inner) { inner })
}

/// Concatenate a list of streams in list order.
///
/// Consume one source completely before opening the next. Early termination closes the active source.
pub fn concat(streams: List(Stream(a))) -> Stream(a) {
  case streams {
    [] -> empty()
    [first, ..rest] -> stream_concat_active(first, rest)
  }
}

fn stream_concat_active(
  active: Stream(a),
  remaining: List(Stream(a)),
) -> Stream(a) {
  make(pull: fn() { stream_concat_pull(active, remaining) }, close: fn() {
    close(active)
  })
}

fn stream_concat_pull(
  active: Stream(a),
  remaining: List(Stream(a)),
) -> Step(a, Stream(a)) {
  case pull(active) {
    Next(element, rest) -> Next(element, stream_concat_active(rest, remaining))
    Done ->
      case remaining {
        [] -> Done
        [next, ..rest] -> stream_concat_pull(next, rest)
      }
  }
}

/// Fold over the stream and emit the updated state for each input element.
///
/// The initial state is not emitted. Empty input produces no elements.
pub fn scan(
  over stream: Stream(a),
  from initial: b,
  with step: fn(b, a) -> b,
) -> Stream(b) {
  make(
    pull: fn() {
      case pull(stream) {
        Next(element, rest) -> {
          let next_state = step(initial, element)
          Next(next_state, scan(over: rest, from: next_state, with: step))
        }
        Done -> Done
      }
    },
    close: fn() { close(stream) },
  )
}

/// Thread a state through the stream and emit each step's output.
///
/// The callback returns `#(next_state, output)`. Emit only the output, including on the final step.
pub fn map_with_state(
  over stream: Stream(a),
  from initial: state,
  with step: fn(state, a) -> #(state, b),
) -> Stream(b) {
  make(
    pull: fn() {
      case pull(stream) {
        Next(element, rest) -> {
          let #(next_state, output) = step(initial, element)
          Next(output, map_with_state(over: rest, from: next_state, with: step))
        }
        Done -> Done
      }
    },
    close: fn() { close(stream) },
  )
}

/// Pair elements from two streams until either source finishes.
///
/// Pull the left element before the right. If the right ends first, the last pulled left element has no pair and is discarded. Early termination closes the right continuation before the left.
pub fn zip(left: Stream(a), right: Stream(b)) -> Stream(#(a, b)) {
  zip_with(left, right, with: fn(left_element, right_element) {
    #(left_element, right_element)
  })
}

/// Combine corresponding elements from two streams until either finishes.
///
/// Use `zip`'s pull order and cleanup order.
pub fn zip_with(
  left: Stream(a),
  right: Stream(b),
  with combiner: fn(a, b) -> c,
) -> Stream(c) {
  make(pull: fn() { stream_zip_pull(left, right, combiner) }, close: fn() {
    close(right)
    close(left)
  })
}

fn stream_zip_pull(
  left: Stream(a),
  right: Stream(b),
  combiner: fn(a, b) -> c,
) -> Step(c, Stream(c)) {
  case pull(left) {
    Done -> {
      close(right)
      Done
    }
    Next(left_element, left_rest) ->
      case pull(right) {
        Done -> {
          close(left_rest)
          Done
        }
        Next(right_element, right_rest) ->
          Next(
            combiner(left_element, right_element),
            zip_with(left_rest, right_rest, with: combiner),
          )
      }
  }
}

/// Insert `separator` between adjacent elements.
///
/// Empty and single-element streams are unchanged.
pub fn intersperse(over stream: Stream(a), with separator: a) -> Stream(a) {
  stream_intersperse_initial(stream, separator)
}

fn stream_intersperse_initial(stream: Stream(a), separator: a) -> Stream(a) {
  make(
    pull: fn() {
      case pull(stream) {
        Done -> Done
        Next(first, rest) ->
          Next(first, stream_intersperse_awaiting_sep(rest, separator))
      }
    },
    close: fn() { close(stream) },
  )
}

fn stream_intersperse_awaiting_sep(
  stream: Stream(a),
  separator: a,
) -> Stream(a) {
  make(
    pull: fn() {
      case pull(stream) {
        Done -> Done
        Next(next, rest) ->
          Next(separator, stream_intersperse_pending(next, rest, separator))
      }
    },
    close: fn() { close(stream) },
  )
}

fn stream_intersperse_pending(
  pending: a,
  stream: Stream(a),
  separator: a,
) -> Stream(a) {
  make(
    pull: fn() {
      Next(pending, stream_intersperse_awaiting_sep(stream, separator))
    },
    close: fn() { close(stream) },
  )
}

/// Run `effect` for each requested element and yield the element unchanged.
pub fn tap(over stream: Stream(a), with effect: fn(a) -> Nil) -> Stream(a) {
  make(
    pull: fn() {
      case pull(stream) {
        Next(element, rest) -> {
          effect(element)
          Next(element, tap(over: rest, with: effect))
        }
        Done -> Done
      }
    },
    close: fn() { close(stream) },
  )
}

/// Collapse each run of equal adjacent elements to one element.
pub fn deduplicate_adjacent(stream: Stream(a)) -> Stream(a) {
  stream_deduplicate_active(stream, None)
}

fn stream_deduplicate_active(stream: Stream(a), last: Option(a)) -> Stream(a) {
  make(pull: fn() { stream_deduplicate_pull(stream, last) }, close: fn() {
    close(stream)
  })
}

fn stream_deduplicate_pull(
  stream: Stream(a),
  last: Option(a),
) -> Step(a, Stream(a)) {
  case pull(stream) {
    Done -> Done
    Next(element, rest) ->
      case last {
        Some(previous) if previous == element ->
          stream_deduplicate_pull(rest, Some(previous))
        Some(_) | None ->
          Next(element, stream_deduplicate_active(rest, Some(element)))
      }
  }
}

/// Read ahead synchronously into a queue of at most `capacity` elements.
///
/// Each consumer pull fills available queue space before returning an element. All pulls happen in the consuming process. Construction does not read the source.
///
/// Return an argument error for `capacity < 1`. After upstream completion, emit the remaining queue. Early termination discards the queue and closes a still-active upstream.
pub fn buffer(
  over stream: Stream(a),
  prefetch capacity: Int,
) -> Result(Stream(a), ArgumentError) {
  use _ <- result.try(check_positive_argument("buffer", "prefetch", capacity))
  Ok(stream_new_buffer(stream, capacity))
}

fn stream_new_buffer(stream: Stream(a), capacity: Int) -> Stream(a) {
  stream_buffer_active(BufferState(
    upstream: Some(stream),
    front: [],
    back: [],
    count: 0,
    capacity:,
  ))
}

// The prefetched elements live in a two-list queue: `front` in pull
// order and `back` in reverse, so pushing and popping are amortised
// O(1). `upstream` becomes `None` once it returns `Done`, after which
// it is neither pulled nor closed again.
type BufferState(a) {
  BufferState(
    upstream: Option(Stream(a)),
    front: List(a),
    back: List(a),
    count: Int,
    capacity: Int,
  )
}

fn stream_buffer_active(state: BufferState(a)) -> Stream(a) {
  make(pull: fn() { stream_buffer_pull(state) }, close: fn() {
    case state.upstream {
      Some(upstream) -> close(upstream)
      None -> Nil
    }
  })
}

fn stream_buffer_pull(state: BufferState(a)) -> Step(a, Stream(a)) {
  let state = stream_buffer_fill(state)
  let #(front, back) = case state.front {
    [] -> #(list.reverse(state.back), [])
    _ -> #(state.front, state.back)
  }
  case front {
    [] -> Done
    [head, ..front] ->
      Next(
        head,
        stream_buffer_active(
          BufferState(..state, front:, back:, count: state.count - 1),
        ),
      )
  }
}

fn stream_buffer_fill(state: BufferState(a)) -> BufferState(a) {
  case state.upstream, state.count >= state.capacity {
    None, _ | _, True -> state
    Some(upstream), False ->
      case pull(upstream) {
        Next(element, rest) ->
          stream_buffer_fill(
            BufferState(
              ..state,
              upstream: Some(rest),
              back: [element, ..state.back],
              count: state.count + 1,
            ),
          )
        Done -> BufferState(..state, upstream: None)
      }
  }
}

/// Stop a stream when the signal stream emits `True`.
///
/// Check the signal before each source pull. `False` permits one source element; `True` closes the signal and source immediately. Once the signal finishes, continue reading the source without checking it again. If the source finishes first, close the remaining signal.
///
/// Early termination closes the signal before the source. Checking a blocking signal also blocks the consumer; this operation does not race two asynchronous sources.
pub fn interrupt_when(
  over stream: Stream(a),
  signal signal: Stream(Bool),
) -> Stream(a) {
  stream_interrupt_active(stream, Some(signal))
}

fn stream_interrupt_active(
  stream: Stream(a),
  signal: Option(Stream(Bool)),
) -> Stream(a) {
  make(pull: fn() { stream_interrupt_pull(stream, signal) }, close: fn() {
    case signal {
      Some(signal) -> close(signal)
      None -> Nil
    }
    close(stream)
  })
}

fn stream_interrupt_pull(
  stream: Stream(a),
  signal: Option(Stream(Bool)),
) -> Step(a, Stream(a)) {
  case signal {
    None -> stream_interrupt_pull_stream(stream, None)
    Some(signal) ->
      case pull(signal) {
        Next(True, signal_rest) -> {
          close(signal_rest)
          close(stream)
          Done
        }
        Next(False, signal_rest) ->
          stream_interrupt_pull_stream(stream, Some(signal_rest))
        Done -> stream_interrupt_pull_stream(stream, None)
      }
  }
}

fn stream_interrupt_pull_stream(
  stream: Stream(a),
  signal: Option(Stream(Bool)),
) -> Step(a, Stream(a)) {
  case pull(stream) {
    Next(element, rest) -> Next(element, stream_interrupt_active(rest, signal))
    Done -> {
      case signal {
        Some(signal) -> close(signal)
        None -> Nil
      }
      Done
    }
  }
}

// --- broadcast and unzip ----------------------------------------------------

// Running state owns the latest upstream and each consumer's queue.
// Failed state keeps only a terminal value and consumers still owed it.
type BroadcastState(a) {
  BroadcastRunning(
    upstream: Option(Stream(a)),
    consumers: List(Consumer(a)),
    policy: QueuePolicy(a),
  )
  BroadcastFailed(terminal: a, pending: List(Int))
}

type QueuePolicy(a) {
  Unlimited
  Limited(max_queue: Int, on_overflow: fn(Int, Int) -> a)
}

// A consumer's queue as two lists, `front` in pull order and `back`
// reversed, so both ends are amortised O(1); `size` counts both.
type Consumer(a) {
  Consumer(front: List(a), back: List(a), size: Int, active: Bool)
}

type ConsumerPull(a) {
  InactiveConsumer
  WaitingConsumer
  QueuedElement(element: a, consumers: List(Consumer(a)))
}

/// Create consumers that share one upstream and receive its elements in order.
///
/// Each consumer is single-use. Its unread values remain queued while other consumers advance. Queues have no size limit; use `broadcast_bounded` to limit them. Merely wrapping an unconsumed consumer in `take` does not release it or bound its queue.
///
/// Close a consumer by consuming it to completion or through an early-ending operation. If the upstream is still active, closing all consumers closes its latest continuation. A non-positive consumer count closes the upstream immediately and returns an empty list; a fresh lazy resource remains unopened.
///
/// ## Panics
///
/// On Erlang, consume each returned stream in the process that called `broadcast`. Reading it in another process panics because its shared state belongs to the creating process.
pub fn broadcast(over stream: Stream(a), into count: Int) -> List(Stream(a)) {
  stream_start_broadcast(stream, count, Unlimited)
}

/// Create shared consumers with a maximum unread queue size.
///
/// Validate `max_queue >= 1` before handling the consumer count. A non-positive consumer count then closes the upstream and returns `Ok([])`.
///
/// Each consumer yields `Ok` values until an upstream element would overflow another active consumer's queue. At that point, discard every queue and the triggering element, close the latest upstream continuation, and emit one `QueueLimitExceeded` error to each active consumer. Each consumer ends after its error. A consumer stopped before notification receives no further elements.
///
/// The error identifies the first overflowing consumer by its zero-based index. All consumers share one run and must follow `broadcast`'s ownership rules.
///
/// ## Panics
///
/// On Erlang, pulling a consumer outside its creating process panics.
pub fn broadcast_bounded(
  over stream: Stream(a),
  into count: Int,
  max_queue max_queue: Int,
) -> Result(List(Stream(Result(a, BroadcastError))), ArgumentError) {
  use _ <- result.try(check_positive_argument(
    "broadcast_bounded",
    "max_queue",
    max_queue,
  ))
  Ok(stream_start_broadcast(
    map(over: stream, with: Ok),
    count,
    Limited(max_queue, fn(consumer, limit) {
      Error(QueueLimitExceeded(consumer:, max_queue: limit))
    }),
  ))
}

fn stream_start_broadcast(
  stream: Stream(a),
  count: Int,
  policy: QueuePolicy(a),
) -> List(Stream(a)) {
  case count < 1 {
    True -> {
      close(stream)
      []
    }
    False -> {
      let state_ref = stream_broadcast_state(stream, count, policy)
      list.repeat(Nil, times: count)
      |> list.index_map(fn(_, consumer_id) {
        stream_broadcast_consumer(state_ref, consumer_id)
      })
    }
  }
}

fn stream_broadcast_state(
  stream: Stream(a),
  count: Int,
  policy: QueuePolicy(a),
) -> Ref(BroadcastState(a)) {
  let empty_queue = Consumer(front: [], back: [], size: 0, active: True)
  ref.new(BroadcastRunning(
    upstream: Some(stream),
    consumers: list.repeat(empty_queue, times: count),
    policy:,
  ))
}

fn stream_broadcast_consumer(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
) -> Stream(a) {
  make(
    pull: fn() { stream_broadcast_pull(state_ref, consumer_id) },
    close: fn() { stream_broadcast_finish(state_ref, consumer_id) },
  )
}

fn stream_broadcast_pull(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
) -> Step(a, Stream(a)) {
  case ref.get(state_ref) {
    Error(Nil) -> Done
    Ok(BroadcastFailed(terminal, pending)) ->
      case list.contains(pending, consumer_id) {
        False -> Done
        True -> {
          stream_broadcast_remove_pending(
            state_ref,
            terminal,
            pending,
            consumer_id,
          )
          Next(terminal, empty())
        }
      }
    Ok(BroadcastRunning(upstream, consumers, policy)) ->
      case stream_pop_queued(consumers, consumer_id) {
        InactiveConsumer -> Done
        QueuedElement(element, consumers) -> {
          ref.set(state_ref, BroadcastRunning(upstream:, consumers:, policy:))
          Next(element, stream_broadcast_consumer(state_ref, consumer_id))
        }
        WaitingConsumer ->
          stream_broadcast_pull_source(
            state_ref,
            consumer_id,
            upstream,
            consumers,
            policy,
          )
      }
  }
}

fn stream_broadcast_pull_source(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
  upstream: Option(Stream(a)),
  consumers: List(Consumer(a)),
  policy: QueuePolicy(a),
) -> Step(a, Stream(a)) {
  case upstream {
    None -> {
      stream_broadcast_finish(state_ref, consumer_id)
      Done
    }
    Some(upstream) ->
      case pull(upstream) {
        Done -> {
          ref.set(
            state_ref,
            BroadcastRunning(upstream: None, consumers:, policy:),
          )
          stream_broadcast_finish(state_ref, consumer_id)
          Done
        }
        Next(element, rest) ->
          case stream_check_queue_bound(consumers, consumer_id, policy) {
            None -> {
              let consumers =
                stream_enqueue_for_others(consumers, consumer_id, element)
              ref.set(
                state_ref,
                BroadcastRunning(upstream: Some(rest), consumers:, policy:),
              )
              Next(element, stream_broadcast_consumer(state_ref, consumer_id))
            }
            Some(terminal) -> {
              let pending = stream_broadcast_active_consumers(consumers)
              // Remove the puller and all upstream references before cleanup.
              stream_broadcast_remove_pending(
                state_ref,
                terminal,
                pending,
                consumer_id,
              )
              close(rest)
              Next(terminal, empty())
            }
          }
      }
  }
}

fn stream_broadcast_active_consumers(
  consumers: List(Consumer(a)),
) -> List(Int) {
  list.index_fold(consumers, [], fn(pending, consumer, index) {
    case consumer.active {
      True -> [index, ..pending]
      False -> pending
    }
  })
}

fn stream_broadcast_remove_pending(
  state_ref: Ref(BroadcastState(a)),
  terminal: a,
  pending: List(Int),
  consumer_id: Int,
) -> Nil {
  case list.filter(pending, fn(index) { index != consumer_id }) {
    [] -> ref.delete(state_ref)
    pending -> ref.set(state_ref, BroadcastFailed(terminal:, pending:))
  }
}

// Marks a consumer as done, by `Done` or by `close`. The last one
// closes the upstream if it is still open and deletes the shared state.
fn stream_broadcast_finish(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
) -> Nil {
  case ref.get(state_ref) {
    Error(Nil) -> Nil
    Ok(BroadcastFailed(terminal, pending)) ->
      stream_broadcast_remove_pending(state_ref, terminal, pending, consumer_id)
    Ok(BroadcastRunning(upstream, consumers, policy)) -> {
      let consumers =
        list.index_map(consumers, fn(consumer, index) {
          case index == consumer_id {
            True -> Consumer(front: [], back: [], size: 0, active: False)
            False -> consumer
          }
        })
      case list.any(consumers, fn(consumer) { consumer.active }), upstream {
        True, _ ->
          ref.set(state_ref, BroadcastRunning(upstream:, consumers:, policy:))
        False, Some(upstream) -> {
          ref.delete(state_ref)
          close(upstream)
        }
        False, None -> ref.delete(state_ref)
      }
    }
  }
}

fn stream_pop_queued(
  consumers: List(Consumer(a)),
  consumer_id: Int,
) -> ConsumerPull(a) {
  case consumers, consumer_id {
    [], _ -> InactiveConsumer
    [Consumer(active: False, ..), ..], 0 -> InactiveConsumer
    [consumer, ..rest], 0 -> {
      let #(front, back) = case consumer.front {
        [] -> #(list.reverse(consumer.back), [])
        front -> #(front, consumer.back)
      }
      case front {
        [element, ..front] -> {
          let consumer =
            Consumer(..consumer, front:, back:, size: consumer.size - 1)
          QueuedElement(element, [consumer, ..rest])
        }
        [] -> WaitingConsumer
      }
    }
    [consumer, ..rest], _ ->
      case stream_pop_queued(rest, consumer_id - 1) {
        QueuedElement(element, rest) ->
          QueuedElement(element, [consumer, ..rest])
        InactiveConsumer -> InactiveConsumer
        WaitingConsumer -> WaitingConsumer
      }
  }
}

fn stream_enqueue_for_others(
  consumers: List(Consumer(a)),
  puller_id: Int,
  element: a,
) -> List(Consumer(a)) {
  list.index_map(consumers, fn(consumer, index) {
    case index != puller_id && consumer.active {
      True ->
        Consumer(
          ..consumer,
          back: [element, ..consumer.back],
          size: consumer.size + 1,
        )
      False -> consumer
    }
  })
}

fn stream_check_queue_bound(
  consumers: List(Consumer(a)),
  puller_id: Int,
  policy: QueuePolicy(a),
) -> Option(a) {
  case policy {
    Unlimited -> None
    Limited(max_queue, on_overflow) ->
      case stream_find_full_queue(consumers, puller_id, max_queue, 0) {
        None -> None
        Some(consumer) -> Some(on_overflow(consumer, max_queue))
      }
  }
}

fn stream_find_full_queue(
  consumers: List(Consumer(a)),
  puller_id: Int,
  max_queue: Int,
  index: Int,
) -> Option(Int) {
  case consumers {
    [] -> None
    [consumer, ..rest] ->
      case consumer.active && index != puller_id && consumer.size >= max_queue {
        True -> Some(index)
        False -> stream_find_full_queue(rest, puller_id, max_queue, index + 1)
      }
  }
}

/// Split a stream of tuples into two shared component streams.
///
/// Each component is single-use and retains unread values in an unbounded queue. Consuming one component to completion before the other stores the entire other component. Cleanup and process ownership follow `broadcast`.
///
/// ## Panics
///
/// On Erlang, consume both components in the process that called `unzip`.
pub fn unzip(stream: Stream(#(a, b))) -> #(Stream(a), Stream(b)) {
  let state_ref = stream_broadcast_state(stream, 2, Unlimited)
  #(
    map(over: stream_broadcast_consumer(state_ref, 0), with: fn(pair) { pair.0 }),
    map(over: stream_broadcast_consumer(state_ref, 1), with: fn(pair) { pair.1 }),
  )
}

/// Group elements into lists of at most `size` elements.
///
/// Return an argument error when `size < 1`. Only the final batch may be shorter than `size`; empty input produces no batches.
pub fn chunks_of(
  over stream: Stream(a),
  into size: Int,
) -> Result(Stream(List(a)), ArgumentError) {
  use _ <- result.try(check_positive_argument("chunks_of", "into", size))
  Ok(stream_chunks_active(stream, [], 0, size))
}

fn stream_chunks_active(
  source: Stream(a),
  buffer: List(a),
  count: Int,
  size: Int,
) -> Stream(List(a)) {
  make(
    pull: fn() { stream_chunks_pull(source, buffer, count, size) },
    close: fn() { close(source) },
  )
}

fn stream_chunks_pull(
  source: Stream(a),
  buffer: List(a),
  count: Int,
  size: Int,
) -> Step(List(a), Stream(List(a))) {
  case pull(source) {
    Done ->
      case buffer {
        [] -> Done
        _ -> Next(list.reverse(buffer), empty())
      }
    Next(element, rest) -> {
      let new_count = count + 1
      let stream_new_buffer = [element, ..buffer]
      case new_count >= size {
        True ->
          Next(
            list.reverse(stream_new_buffer),
            stream_chunks_active(rest, [], 0, size),
          )
        False -> stream_chunks_pull(rest, stream_new_buffer, new_count, size)
      }
    }
  }
}

/// Group consecutive elements sharing a key.
///
/// Emit each key paired with its nonempty list of elements, preserving input order. Equal keys separated by another key form separate groups.
pub fn group_adjacent(
  over stream: Stream(a),
  by key: fn(a) -> k,
) -> Stream(#(k, List(a))) {
  stream_group_active(stream, None, [], key)
}

fn stream_group_active(
  source: Stream(a),
  current_key: Option(k),
  buffer: List(a),
  key: fn(a) -> k,
) -> Stream(#(k, List(a))) {
  make(
    pull: fn() { stream_group_pull(source, current_key, buffer, key) },
    close: fn() { close(source) },
  )
}

fn stream_group_pull(
  source: Stream(a),
  current_key: Option(k),
  buffer: List(a),
  key: fn(a) -> k,
) -> Step(#(k, List(a)), Stream(#(k, List(a)))) {
  case pull(source) {
    Done ->
      case current_key {
        None -> Done
        Some(group_key) -> Next(#(group_key, list.reverse(buffer)), empty())
      }
    Next(element, rest) -> {
      let element_key = key(element)
      case current_key {
        Some(prev_key) if prev_key != element_key ->
          Next(
            #(prev_key, list.reverse(buffer)),
            stream_group_active(rest, Some(element_key), [element], key),
          )
        Some(_) ->
          stream_group_pull(rest, current_key, [element, ..buffer], key)
        None -> stream_group_pull(rest, Some(element_key), [element], key)
      }
    }
  }
}

/// Collect every element into a list, preserving order.
///
/// The input must terminate, and the result holds every element in memory.
pub fn to_list(stream: Stream(a)) -> List(a) {
  stream
  |> fold(from: [], with: fn(accumulator, element) { [element, ..accumulator] })
  |> list.reverse
}

/// Concatenate every string element in order.
///
/// Return `""` for empty input. The input must terminate.
pub fn to_string(stream: Stream(String)) -> String {
  stream
  |> to_string_tree
  |> string_tree.to_string
}

/// Collect strings into a `StringTree`, preserving order.
///
/// Return an empty tree for empty input. The input must terminate.
pub fn to_string_tree(stream: Stream(String)) -> StringTree {
  stream
  |> fold(from: string_tree.new(), with: fn(accumulator, chunk) {
    string_tree.append(accumulator, chunk)
  })
}

/// Join every string element with a separator between adjacent elements.
///
/// Add no leading or trailing separator. Return `""` for empty input. The input must terminate.
pub fn to_string_join(
  stream stream: Stream(String),
  with separator: String,
) -> String {
  stream
  |> to_string_tree_join(with: separator)
  |> string_tree.to_string
}

/// Join string elements into a `StringTree` with a separator between them.
///
/// Use `to_string_join`'s separator rules. Empty input produces an empty tree.
pub fn to_string_tree_join(
  stream stream: Stream(String),
  with separator: String,
) -> StringTree {
  let #(tree, _seen) =
    fold(
      over: stream,
      from: #(string_tree.new(), False),
      with: fn(accumulator, chunk) {
        let #(tree, seen) = accumulator
        case seen {
          False -> #(string_tree.append(tree, chunk), True)
          True -> {
            let with_sep = string_tree.append(tree, separator)
            #(string_tree.append(with_sep, chunk), True)
          }
        }
      },
    )
  tree
}

/// Concatenate every binary element in order.
///
/// Return `<<>>` for empty input. The input must terminate.
pub fn to_bit_array(stream: Stream(BitArray)) -> BitArray {
  stream
  |> fold(from: bytes_tree.new(), with: fn(accumulator, chunk) {
    bytes_tree.append(accumulator, chunk)
  })
  |> bytes_tree.to_bit_array
}

/// Count every element without retaining it.
///
/// Return zero for empty input. The input must terminate.
pub fn count(stream: Stream(a)) -> Int {
  fold(over: stream, from: 0, with: fn(accumulator, _) { accumulator + 1 })
}

/// Return the first element and close the remaining stream.
///
/// Return `Error(Nil)` for empty input.
///
/// ## Examples
///
/// ```gleam
/// from_list([1, 2]) |> first
/// // Ok(1)
/// empty() |> first
/// // Error(Nil)
/// ```
pub fn first(stream: Stream(a)) -> Result(a, Nil) {
  case pull(stream) {
    Next(element, rest) -> {
      close(rest)
      Ok(element)
    }
    Done -> Error(Nil)
  }
}

/// Consume the stream and return its last element.
///
/// Return `Error(Nil)` for empty input. The input must terminate.
pub fn last(stream: Stream(a)) -> Result(a, Nil) {
  fold(over: stream, from: Error(Nil), with: fn(_, element) { Ok(element) })
}

/// Apply `step` to the accumulator and each element in order.
///
/// Return the final accumulator, or `initial` for empty input. The input must terminate.
pub fn fold(
  over stream: Stream(a),
  from initial: b,
  with step: fn(b, a) -> b,
) -> b {
  case pull(stream) {
    Next(element, rest) ->
      fold(over: rest, from: step(initial, element), with: step)
    Done -> initial
  }
}

/// Fold over the stream using its first element as the initial accumulator.
///
/// Return `Error(Nil)` for empty input. The input must terminate.
pub fn reduce(
  over stream: Stream(a),
  with step: fn(a, a) -> a,
) -> Result(a, Nil) {
  case pull(stream) {
    Next(head, rest) -> Ok(fold(over: rest, from: head, with: step))
    Done -> Error(Nil)
  }
}

/// Consume every element and discard the values.
///
/// Run the source's callbacks through completion. The input must terminate.
pub fn drain(stream: Stream(a)) -> Nil {
  case pull(stream) {
    Next(_, rest) -> drain(rest)
    Done -> Nil
  }
}

/// Return whether every element satisfies the predicate.
///
/// Stop and close the remaining stream on the first `False`. Return `True` for empty input.
pub fn all(
  over stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Bool {
  case pull(stream) {
    Done -> True
    Next(element, rest) ->
      case predicate(element) {
        True -> all(over: rest, satisfying: predicate)
        False -> {
          close(rest)
          False
        }
      }
  }
}

/// Return whether any element satisfies the predicate.
///
/// Stop and close the remaining stream on the first `True`. Return `False` for empty input.
pub fn any(
  over stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Bool {
  case pull(stream) {
    Done -> False
    Next(element, rest) ->
      case predicate(element) {
        True -> {
          close(rest)
          True
        }
        False -> any(over: rest, satisfying: predicate)
      }
  }
}

/// Return the first element satisfying the predicate and close the remaining stream.
///
/// Return `Error(Nil)` if the source finishes without a match.
pub fn find(
  over stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Result(a, Nil) {
  case pull(stream) {
    Done -> Error(Nil)
    Next(element, rest) ->
      case predicate(element) {
        True -> {
          close(rest)
          Ok(element)
        }
        False -> find(over: rest, satisfying: predicate)
      }
  }
}

/// Sum all integer elements, returning zero for empty input.
pub fn sum_int(stream: Stream(Int)) -> Int {
  fold(over: stream, from: 0, with: fn(accumulator, value) {
    accumulator + value
  })
}

/// Sum all float elements, returning `0.0` for empty input.
pub fn sum_float(stream: Stream(Float)) -> Float {
  fold(over: stream, from: 0.0, with: fn(accumulator, value) {
    accumulator +. value
  })
}

/// Multiply all integer elements, returning one for empty input.
pub fn product_int(stream: Stream(Int)) -> Int {
  fold(over: stream, from: 1, with: fn(accumulator, value) {
    accumulator * value
  })
}

/// Collect successful values in order, stopping at the first error.
///
/// On an error, close the remaining stream and return that error. Empty input returns `Ok([])`. Use `partition_result` to keep errors and continue.
pub fn collect_result(stream: Stream(Result(a, e))) -> Result(List(a), e) {
  terminal_do_collect_result(stream, [])
}

fn terminal_do_collect_result(
  stream: Stream(Result(a, e)),
  accumulator: List(a),
) -> Result(List(a), e) {
  case pull(stream) {
    Done -> Ok(list.reverse(accumulator))
    Next(Ok(value), rest) ->
      terminal_do_collect_result(rest, [value, ..accumulator])
    Next(Error(error), rest) -> {
      close(rest)
      Error(error)
    }
  }
}

/// Collect successes and errors into separate lists, preserving order in each.
///
/// Consume the whole stream and return `#(successes, errors)`. The input must terminate.
pub fn partition_result(stream: Stream(Result(a, e))) -> #(List(a), List(e)) {
  let #(values, errors) =
    fold(over: stream, from: #([], []), with: fn(accumulator, value) {
      let #(values, errors) = accumulator
      case value {
        Ok(value) -> #([value, ..values], errors)
        Error(error) -> #(values, [error, ..errors])
      }
    })
  #(list.reverse(values), list.reverse(errors))
}

/// Apply `split` to each element and collect its two result branches.
///
/// Return `#(successes, errors)` in source order within each list. Consume the whole input, which must terminate.
pub fn partition_map(
  over stream: Stream(a),
  with split: fn(a) -> Result(b, c),
) -> #(List(b), List(c)) {
  let #(lefts, rights) =
    fold(over: stream, from: #([], []), with: fn(accumulator, element) {
      let #(lefts, rights) = accumulator
      case split(element) {
        Ok(left) -> #([left, ..lefts], rights)
        Error(right) -> #(lefts, [right, ..rights])
      }
    })
  #(list.reverse(lefts), list.reverse(rights))
}

/// Run an effect for every element, sequentially in input order.
///
/// The input must terminate for this call to return.
pub fn each(over stream: Stream(a), with effect: fn(a) -> Nil) -> Nil {
  case pull(stream) {
    Done -> Nil
    Next(element, rest) -> {
      effect(element)
      each(over: rest, with: effect)
    }
  }
}

/// Run an effect for each element until one fails or the source finishes.
///
/// On an effect error, close the remaining stream and return that error. Return `Ok(Nil)` after successful completion, including empty input.
pub fn try_each(
  over stream: Stream(a),
  with effect: fn(a) -> Result(Nil, e),
) -> Result(Nil, e) {
  case pull(stream) {
    Done -> Ok(Nil)
    Next(element, rest) ->
      case effect(element) {
        Ok(Nil) -> try_each(over: rest, with: effect)
        Error(e) -> {
          close(rest)
          Error(e)
        }
      }
  }
}

/// Print each string element followed by a newline.
///
/// Consume the stream sequentially. The input must terminate for this call to return.
pub fn println(stream: Stream(String)) -> Nil {
  each(over: stream, with: io.println)
}

/// Split string chunks into lines at LF, CRLF, or lone CR terminators.
///
/// Exclude terminators from the output. Recognize CRLF across chunk boundaries, preserve blank lines, and emit an unfinished final line. Empty input emits no lines; a trailing terminator adds no extra line.
///
/// Buffer the unfinished line between pulls. Memory use depends on the longest unfinished line.
///
/// ## Examples
///
/// ```gleam
/// from_list(["a\r", "\nb\nc"]) |> lines |> to_list
/// // ["a", "b", "c"]
/// ```
pub fn lines(over stream: Stream(String)) -> Stream(String) {
  text_lines_active(LinesState(
    source: stream,
    ready: [],
    partial: [],
    after_cr: False,
    source_drained: False,
  ))
}

// `ready` holds complete lines not yet emitted, in order. `partial`
// holds the pieces of the current unterminated line in reverse, so a
// line spread over many chunks is joined once. `after_cr` is True when
// the text so far ends with a `\r` that already ended a line, so a
// `\n` at the start of the next chunk belongs to that `\r\n`.
type LinesState {
  LinesState(
    source: Stream(String),
    ready: List(String),
    partial: List(String),
    after_cr: Bool,
    source_drained: Bool,
  )
}

fn text_lines_active(state: LinesState) -> Stream(String) {
  make(pull: fn() { text_lines_pull(state) }, close: fn() {
    core_close_unless_drained(state.source, drained: state.source_drained)
  })
}

fn text_lines_pull(state: LinesState) -> Step(String, Stream(String)) {
  case state.ready, state.source_drained {
    [line, ..ready], _ ->
      Next(line, text_lines_active(LinesState(..state, ready:)))
    [], True ->
      case state.partial {
        [] -> Done
        partial ->
          Next(
            text_reverse_concat(partial),
            text_lines_active(LinesState(..state, partial: [])),
          )
      }
    [], False ->
      case pull(state.source) {
        Done -> text_lines_pull(LinesState(..state, source_drained: True))
        Next("", source) -> text_lines_pull(LinesState(..state, source:))
        Next(chunk, source) ->
          text_lines_pull(text_split_chunk_into_lines(
            LinesState(..state, source:),
            chunk,
          ))
      }
  }
}

// Splits one chunk on `\n`; each piece is then split on `\r`, since a
// lone `\r` also ends a line. Only the text after the last terminator
// is carried over, so every chunk is scanned once.
fn text_split_chunk_into_lines(state: LinesState, chunk: String) -> LinesState {
  let segments = case state.after_cr, string.split(chunk, "\n") {
    True, ["", ..rest] -> rest
    _, segments -> segments
  }
  text_collect_lines(segments, state.partial, [])
  |> fn(collected) {
    let #(reversed_ready, partial, after_cr) = collected
    LinesState(
      ..state,
      ready: list.reverse(reversed_ready),
      partial:,
      after_cr:,
    )
  }
}

fn text_collect_lines(
  segments: List(String),
  partial: List(String),
  reversed_ready: List(String),
) -> #(List(String), List(String), Bool) {
  case segments {
    [] -> #(reversed_ready, partial, False)
    [last] -> {
      // Text after the last `\n`: lines ended by `\r` are complete; the
      // rest starts a line that a later chunk continues.
      case text_split_last(string.split(last, "\r")) {
        #([], tail) -> #(reversed_ready, text_push_piece(partial, tail), False)
        #(complete, tail) -> #(
          text_emit_lines(complete, partial, reversed_ready),
          text_push_piece([], tail),
          tail == "",
        )
      }
    }
    [segment, ..rest] -> {
      // A segment followed by `\n`. A trailing `\r` is the `\r` of a
      // `\r\n`; any other `\r` ends a line of its own.
      let pieces = string.split(segment, "\r")
      let pieces = case text_split_last(pieces) {
        #([_, ..] as complete, "") -> complete
        _ -> pieces
      }
      text_collect_lines(
        rest,
        [],
        text_emit_lines(pieces, partial, reversed_ready),
      )
    }
  }
}

// Prepends `pieces` to `ready_rev` as complete lines, the first one
// joined to the carried-over `partial` line.
fn text_emit_lines(
  pieces: List(String),
  partial: List(String),
  reversed_ready: List(String),
) -> List(String) {
  case pieces {
    [] -> [text_reverse_concat(partial), ..reversed_ready]
    [first, ..others] ->
      list.fold(
        others,
        [text_reverse_concat(text_push_piece(partial, first)), ..reversed_ready],
        fn(accumulator, line) { [line, ..accumulator] },
      )
  }
}

fn text_push_piece(partial: List(String), piece: String) -> List(String) {
  case piece {
    "" -> partial
    _ -> [piece, ..partial]
  }
}

fn text_reverse_concat(reversed_pieces: List(String)) -> String {
  string.concat(list.reverse(reversed_pieces))
}

/// Group lines into records separated by empty strings.
///
/// Emit nonempty lists of lines, preserving order. Consecutive blank lines produce no empty records. Emit an unfinished final record. Use `lines` first when the input consists of arbitrary text chunks.
///
/// Buffer each record until its separator or the end of input.
pub fn records(over stream: Stream(String)) -> Stream(List(String)) {
  text_records_active(stream, [], False)
}

fn text_records_active(
  source: Stream(String),
  reversed_lines: List(String),
  source_drained: Bool,
) -> Stream(List(String)) {
  make(
    pull: fn() { text_records_pull(source, reversed_lines, source_drained) },
    close: fn() { core_close_unless_drained(source, drained: source_drained) },
  )
}

fn text_records_pull(
  source: Stream(String),
  reversed_lines: List(String),
  source_drained: Bool,
) -> Step(List(String), Stream(List(String))) {
  case source_drained {
    True -> text_finish_records(source, reversed_lines)
    False ->
      case pull(source) {
        Done -> text_finish_records(source, reversed_lines)
        Next("", source_rest) ->
          case reversed_lines {
            // Leading blank or back-to-back separator: skip without
            // emitting an empty record and keep pulling. The recursive
            // call only happens after the source has advanced, so this
            // cannot loop on a single value.
            [] -> text_records_pull(source_rest, [], False)
            _ ->
              Next(
                list.reverse(reversed_lines),
                text_records_active(source_rest, [], False),
              )
          }
        Next(line, source_rest) ->
          text_records_pull(source_rest, [line, ..reversed_lines], False)
      }
  }
}

fn text_finish_records(
  source: Stream(String),
  reversed_lines: List(String),
) -> Step(List(String), Stream(List(String))) {
  case reversed_lines {
    [] -> Done
    _ ->
      Next(list.reverse(reversed_lines), text_records_active(source, [], True))
  }
}

/// Split string chunks on a delimiter, joining pieces that cross chunk boundaries.
///
/// For a nonempty delimiter, preserve empty pieces before, between, and after delimiters. A source with no elements emits no pieces; a source containing an empty string emits one empty piece.
///
/// An empty delimiter emits Unicode grapheme clusters. Hold a chunk's final grapheme until the next chunk or end of input establishes its boundary. Memory use depends on the longest unfinished piece or grapheme.
pub fn split(
  over stream: Stream(String),
  on delimiter: String,
) -> Stream(String) {
  case delimiter {
    "" -> text_graphemes_active(stream, [], "", False)
    _ ->
      text_split_active(Split(
        source: stream,
        delimiter:,
        piece: [],
        recent: [],
        unscanned: "",
        has_seen_input: False,
      ))
  }
}

// Same layout as `delimited`: `piece` holds the text that is
// certainly part of the current piece, newest first, `recent` the
// newest scanned chunks covering `byte_size(delimiter) - 1` bytes, and
// `unscanned` the text not searched yet. A piece spread over many
// chunks is therefore joined once, and each chunk is searched about
// once instead of rescanning the whole piece per chunk.
type Split {
  Split(
    source: Stream(String),
    delimiter: String,
    piece: List(String),
    recent: List(String),
    unscanned: String,
    has_seen_input: Bool,
  )
}

fn text_split_active(state: Split) -> Stream(String) {
  make(pull: fn() { text_split_pull(state) }, close: fn() {
    close(state.source)
  })
}

fn text_split_pull(state: Split) -> Step(String, Stream(String)) {
  let window = text_reverse_concat([state.unscanned, ..state.recent])
  case string.split_once(window, on: state.delimiter) {
    Ok(#(before, after)) ->
      Next(
        text_reverse_concat([before, ..state.piece]),
        text_split_active(
          Split(
            ..state,
            piece: [],
            recent: [],
            unscanned: after,
            has_seen_input: True,
          ),
        ),
      )
    Error(_) -> {
      let #(recent, older) =
        core_keep_newest(
          [state.unscanned, ..state.recent],
          holding: string.byte_size(state.delimiter) - 1,
          measured_by: string.byte_size,
        )
      let state =
        Split(
          ..state,
          piece: list.append(older, state.piece),
          recent:,
          unscanned: "",
        )
      case pull(state.source) {
        Next(chunk, source) ->
          text_split_pull(
            Split(..state, source:, unscanned: chunk, has_seen_input: True),
          )
        Done ->
          case state.has_seen_input {
            True ->
              Next(
                text_reverse_concat(list.append(recent, state.piece)),
                empty(),
              )
            False -> Done
          }
      }
    }
  }
}

/// `pending` holds the complete graphemes of the chunks read so far;
/// `tail` is the last grapheme seen, which the next chunk may still
/// extend (a combining mark, the `\n` of `\r\n`, the second regional
/// indicator of a flag), so it is re-segmented together with that chunk.
fn text_graphemes_active(
  source: Stream(String),
  pending: List(String),
  tail: String,
  source_drained: Bool,
) -> Stream(String) {
  make(
    pull: fn() { text_graphemes_pull(source, pending, tail, source_drained) },
    close: fn() { core_close_unless_drained(source, drained: source_drained) },
  )
}

fn text_graphemes_pull(
  source: Stream(String),
  pending: List(String),
  tail: String,
  source_drained: Bool,
) -> Step(String, Stream(String)) {
  case pending {
    [grapheme, ..rest] ->
      Next(grapheme, text_graphemes_active(source, rest, tail, source_drained))
    [] ->
      case source_drained {
        True -> Done
        False ->
          case pull(source) {
            Done ->
              case tail {
                "" -> Done
                _ -> Next(tail, text_graphemes_active(source, [], "", True))
              }
            Next(chunk, source_rest) -> {
              let #(complete, new_tail) =
                text_split_last(string.to_graphemes(tail <> chunk))
              text_graphemes_pull(source_rest, complete, new_tail, False)
            }
          }
      }
  }
}

/// Split off the last element; an empty list yields `#([], "")`.
fn text_split_last(graphemes: List(String)) -> #(List(String), String) {
  case list.reverse(graphemes) {
    [] -> #([], "")
    [last, ..reversed_rest] -> #(list.reverse(reversed_rest), last)
  }
}

// --- UTF-8 decode / encode ------------------------------------------------

/// Decode UTF-8 byte chunks, joining code points split across chunks.
///
/// Emit each buffered run of valid text as `Ok`. The concatenated text is independent of byte chunk boundaries, though output string boundaries may vary.
///
/// Emit `Error(Nil)` for an invalid byte and continue decoding, retaining valid text around it. Invalid sequences include overlong encodings, encoded surrogates, and code points above U+10FFFF. An unfinished trailing sequence produces a final error. Use `collect_result` to stop at the first error.
pub fn utf8_decode(
  over stream: Stream(BitArray),
) -> Stream(Result(String, Nil)) {
  text_utf8_decode_active(stream, <<>>, False)
}

fn text_utf8_decode_active(
  source: Stream(BitArray),
  buffer: BitArray,
  source_drained: Bool,
) -> Stream(Result(String, Nil)) {
  make(
    pull: fn() { text_utf8_decode_pull(source, buffer, source_drained) },
    close: fn() { core_close_unless_drained(source, drained: source_drained) },
  )
}

fn text_utf8_decode_pull(
  source: Stream(BitArray),
  buffer: BitArray,
  source_drained: Bool,
) -> Step(Result(String, Nil), Stream(Result(String, Nil))) {
  case text_extract_run(buffer, source_drained) {
    RunValid(bytes, remaining) ->
      case bit_array.to_string(bytes) {
        Ok(text) ->
          Next(
            Ok(text),
            text_utf8_decode_active(source, remaining, source_drained),
          )
        Error(_) ->
          Next(
            Error(Nil),
            text_utf8_decode_active(source, remaining, source_drained),
          )
      }
    RunInvalid(remaining) ->
      Next(
        Error(Nil),
        text_utf8_decode_active(source, remaining, source_drained),
      )
    RunNeedMore ->
      case pull(source) {
        Next(chunk, source_rest) ->
          text_utf8_decode_pull(
            source_rest,
            bit_array.append(buffer, chunk),
            False,
          )
        Done -> text_utf8_decode_pull(source, buffer, True)
      }
    RunDone -> Done
  }
}

type Run {
  RunValid(bytes: BitArray, remaining: BitArray)
  RunInvalid(remaining: BitArray)
  RunNeedMore
  RunDone
}

fn text_extract_run(buffer: BitArray, source_drained: Bool) -> Run {
  text_do_extract_run(buffer, buffer, 0, source_drained)
}

// Walks the valid prefix of `buffer` counting its bytes in
// `valid_size`, then slices it off once, so a large chunk is not
// rebuilt one code point at a time.
fn text_do_extract_run(
  buffer: BitArray,
  remaining: BitArray,
  valid_size: Int,
  source_drained: Bool,
) -> Run {
  case text_extract_codepoint(remaining, source_drained), valid_size {
    DecodeOk(bytes, rest), _ ->
      text_do_extract_run(
        buffer,
        rest,
        valid_size + bit_array.byte_size(bytes),
        source_drained,
      )
    DecodeInvalid(rest), 0 -> RunInvalid(rest)
    DecodeNeedMore, 0 -> RunNeedMore
    DecodeDone, 0 -> RunDone
    DecodeDone, _ -> text_valid_prefix(buffer, valid_size, <<>>)
    DecodeInvalid(_), _ | DecodeNeedMore, _ ->
      text_valid_prefix(buffer, valid_size, remaining)
  }
}

fn text_valid_prefix(
  buffer: BitArray,
  valid_size: Int,
  remaining: BitArray,
) -> Run {
  case bit_array.slice(buffer, 0, valid_size) {
    Ok(bytes) -> RunValid(bytes, remaining)
    Error(_) -> RunInvalid(remaining)
  }
}

type Decoded {
  DecodeOk(bytes: BitArray, rest: BitArray)
  DecodeInvalid(rest: BitArray)
  DecodeNeedMore
  DecodeDone
}

fn text_extract_codepoint(buffer: BitArray, source_drained: Bool) -> Decoded {
  case buffer {
    <<>> ->
      case source_drained {
        True -> DecodeDone
        False -> DecodeNeedMore
      }
    <<lead:size(8), rest:bits>> ->
      text_classify_and_extract(lead, rest, source_drained)
    _ -> DecodeInvalid(<<>>)
  }
}

fn text_classify_and_extract(
  lead: Int,
  rest: BitArray,
  source_drained: Bool,
) -> Decoded {
  case lead {
    _ if lead < 0x80 -> DecodeOk(<<lead>>, rest)
    _ if lead < 0xC2 -> DecodeInvalid(rest)
    _ if lead < 0xE0 ->
      text_collect_continuation(lead, <<>>, rest, 1, source_drained)
    _ if lead < 0xF0 ->
      text_collect_continuation(lead, <<>>, rest, 2, source_drained)
    _ if lead < 0xF5 ->
      text_collect_continuation(lead, <<>>, rest, 3, source_drained)
    _ -> DecodeInvalid(rest)
  }
}

fn text_collect_continuation(
  lead: Int,
  collected: BitArray,
  rest: BitArray,
  need: Int,
  source_drained: Bool,
) -> Decoded {
  case need {
    0 -> DecodeOk(bit_array.append(<<lead>>, collected), rest)
    _ -> text_consume_continuation(lead, collected, rest, need, source_drained)
  }
}

fn text_consume_continuation(
  lead: Int,
  collected: BitArray,
  rest: BitArray,
  need: Int,
  source_drained: Bool,
) -> Decoded {
  case rest {
    <<>> ->
      case source_drained {
        True -> DecodeInvalid(<<>>)
        False -> DecodeNeedMore
      }
    <<byte:size(8), more:bits>> ->
      case text_is_continuation(lead, bit_array.byte_size(collected), byte) {
        True ->
          text_collect_continuation(
            lead,
            bit_array.append(collected, <<byte>>),
            more,
            need - 1,
            source_drained,
          )
        False -> DecodeInvalid(rest)
      }
    _ -> DecodeInvalid(rest)
  }
}

/// Whether `byte` may follow `lead` after `seen` continuation bytes.
/// Every continuation byte is in 0x80..0xBF, and the byte right after
/// the lead is narrower for four leads (RFC 3629, section 4): 0xE0 and
/// 0xF0 exclude overlong encodings, 0xED excludes the UTF-16 surrogates
/// U+D800..U+DFFF and 0xF4 excludes code points above U+10FFFF.
fn text_is_continuation(lead: Int, seen: Int, byte: Int) -> Bool {
  let #(low, high) = case seen, lead {
    0, 0xE0 -> #(0xA0, 0xBF)
    0, 0xED -> #(0x80, 0x9F)
    0, 0xF0 -> #(0x90, 0xBF)
    0, 0xF4 -> #(0x80, 0x8F)
    _, _ -> #(0x80, 0xBF)
  }
  byte >= low && byte <= high
}

/// Decode UTF-8 and discard decoding errors while retaining valid text.
///
/// Preserve valid segments before and after malformed bytes within the same input chunk.
///
/// ## Examples
///
/// ```gleam
/// once(<<"a", 255, "b">>) |> utf8_decode_lossy |> to_string
/// // "ab"
/// ```
pub fn utf8_decode_lossy(over stream: Stream(BitArray)) -> Stream(String) {
  text_utf8_decode_lossy_active(utf8_decode(over: stream))
}

fn text_utf8_decode_lossy_active(
  source: Stream(Result(String, Nil)),
) -> Stream(String) {
  make(pull: fn() { text_utf8_decode_lossy_pull(source) }, close: fn() {
    close(source)
  })
}

fn text_utf8_decode_lossy_pull(
  source: Stream(Result(String, Nil)),
) -> Step(String, Stream(String)) {
  case pull(source) {
    Done -> Done
    Next(Ok(text), rest) -> Next(text, text_utf8_decode_lossy_active(rest))
    Next(Error(Nil), rest) -> text_utf8_decode_lossy_pull(rest)
  }
}

/// Encode each string as one UTF-8 binary element.
///
/// An empty string produces an empty binary.
pub fn utf8_encode(over stream: Stream(String)) -> Stream(BitArray) {
  make(
    pull: fn() {
      case pull(stream) {
        Next(text, rest) ->
          Next(bit_array.from_string(text), utf8_encode(over: rest))
        Done -> Done
      }
    },
    close: fn() { close(stream) },
  )
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

const binary_nothing_buffered = Buffered(joined: <<>>, later: [], size: 0)

fn binary_buffer_chunk(buffered: Buffered, chunk: BitArray) -> Buffered {
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

// Split off the first `count` bytes after the caller checks the buffered
// size. Refill rejects unaligned chunks before adding them to this buffer.
fn binary_take_bytes(
  buffered: Buffered,
  count: Int,
) -> Result(#(BitArray, Buffered), Nil) {
  let joined = case bit_array.byte_size(buffered.joined) >= count {
    True -> buffered.joined
    False -> bit_array.concat([buffered.joined, ..list.reverse(buffered.later)])
  }
  let later = case joined == buffered.joined {
    True -> buffered.later
    False -> []
  }
  case joined {
    <<taken:bytes-size(count), rest:bits>> ->
      Ok(#(taken, Buffered(joined: rest, later:, size: buffered.size - count)))
    _ -> Error(Nil)
  }
}

// Pull one more chunk into `buffered`, or `None` once `source` is done.
fn binary_refill(
  source: Stream(BitArray),
  buffered: Buffered,
) -> Result(Option(#(Stream(BitArray), Buffered)), BinaryError) {
  case binary_pull(source) {
    Next(Ok(chunk), rest) ->
      Ok(Some(#(rest, binary_buffer_chunk(buffered, chunk))))
    Next(Error(error), _) -> Error(error)
    Done -> Ok(None)
  }
}

// Validate each received chunk before it enters any framing buffer.
// A failed pull owns the latest continuation and closes it exactly once.
fn binary_pull(
  source: Stream(BitArray),
) -> Step(Result(BitArray, BinaryError), Stream(BitArray)) {
  case pull(source) {
    Done -> Done
    Next(chunk, rest) -> {
      let size = bit_array.bit_size(chunk)
      case size % 8 {
        0 -> Next(Ok(chunk), rest)
        _ -> {
          close(rest)
          Next(Error(UnalignedChunk(bit_size: size)), empty())
        }
      }
    }
  }
}

// --- bytes ----------------------------------------------------------------

/// Yield each input byte as `Ok` containing an integer from 0 to 255.
///
/// Reject a non-byte-aligned chunk with one `Error(UnalignedChunk(...))`, close the latest upstream continuation, and end. Reject the whole offending chunk, including any complete bytes within it. Empty chunks produce no elements.
pub fn bytes(
  over stream: Stream(BitArray),
) -> Stream(Result(Int, BinaryError)) {
  binary_bytes_active(stream, <<>>)
}

fn binary_bytes_active(
  source: Stream(BitArray),
  buffer: BitArray,
) -> Stream(Result(Int, BinaryError)) {
  make(pull: fn() { binary_bytes_pull(source, buffer) }, close: fn() {
    close(source)
  })
}

fn binary_bytes_pull(
  source: Stream(BitArray),
  buffer: BitArray,
) -> Step(Result(Int, BinaryError), Stream(Result(Int, BinaryError))) {
  case buffer {
    <<b:size(8), rest:bits>> -> Next(Ok(b), binary_bytes_active(source, rest))
    _ ->
      case binary_pull(source) {
        Next(Ok(chunk), source_rest) ->
          binary_bytes_pull(source_rest, bit_array.append(buffer, chunk))
        Next(Error(error), _) -> Next(Error(error), empty())
        Done -> Done
      }
  }
}

// --- length-prefixed -----------------------------------------------------

/// Describe malformed binary input encountered while consuming a stream.
///
/// `IncompleteFrame` reports a short prefix or payload. `FrameTooLarge` reports a length above the configured limit. `UnalignedChunk` reports an input chunk whose bit count is not divisible by eight. Each error ends the framing stream.
pub type BinaryError {
  IncompleteFrame(expected: Int, got: Int)
  /// The prefix declared a payload length exceeding `max_frame_size`.
  /// The stream ends after this element, since the bytes that follow
  /// can no longer be split into frames.
  FrameTooLarge(claimed: Int, max: Int)
  UnalignedChunk(bit_size: Int)
}

/// Read big-endian unsigned lengths and emit the corresponding binary frames.
///
/// `prefix_size` must be 1, 2, 4, or 8 bytes, and `max_frame_size` must be nonnegative. Reject invalid arguments before pulling the source. A zero frame limit permits empty frames only.
///
/// Emit frames as `Ok` values. A short prefix or payload at EOF produces `IncompleteFrame`. A declared length above the limit produces `FrameTooLarge` as soon as its prefix is available, without pulling further chunks. Payload bytes in the same input chunk may already be buffered. A non-byte-aligned input chunk produces `UnalignedChunk`. Each error ends the stream; an active upstream is closed once.
///
/// ## Examples
///
/// ```gleam
/// let assert Ok(frames) = length_prefixed(
///   once(<<3, "abc">>), prefix_size: 1, max_frame_size: 1024,
/// )
/// collect_result(frames)
/// // Ok([<<"abc">>])
///
/// length_prefixed(empty(), prefix_size: 3, max_frame_size: 1024)
/// // Error(InvalidPrefixSize(3))
/// ```
pub fn length_prefixed(
  over stream: Stream(BitArray),
  prefix_size prefix_size: Int,
  max_frame_size max_frame_size: Int,
) -> Result(Stream(Result(BitArray, BinaryError)), ArgumentError) {
  case prefix_size {
    1 | 2 | 4 | 8 ->
      case max_frame_size < 0 {
        True -> Error(NegativeFrameLimit(given: max_frame_size))
        False ->
          Ok(
            binary_length_prefixed_active(LengthPrefixed(
              source: stream,
              buffered: binary_nothing_buffered,
              prefix_size:,
              max_frame_size:,
              payload_size: None,
            )),
          )
      }
    _ -> Error(InvalidPrefixSize(given: prefix_size))
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

fn binary_length_prefixed_active(
  state: LengthPrefixed,
) -> Stream(Result(BitArray, BinaryError)) {
  make(pull: fn() { binary_length_prefixed_pull(state) }, close: fn() {
    close(state.source)
  })
}

fn binary_length_prefixed_pull(
  state: LengthPrefixed,
) -> Step(Result(BitArray, BinaryError), Stream(Result(BitArray, BinaryError))) {
  let needed = option.unwrap(state.payload_size, state.prefix_size)
  case state.buffered.size >= needed {
    True -> {
      case binary_take_bytes(state.buffered, needed) {
        Ok(#(bytes, buffered)) ->
          binary_length_prefixed_frame(
            LengthPrefixed(..state, buffered:),
            bytes,
          )
        Error(_) -> {
          close(state.source)
          Done
        }
      }
    }
    False ->
      case binary_refill(state.source, state.buffered), state.buffered.size {
        Ok(Some(#(source, buffered))), _ ->
          binary_length_prefixed_pull(
            LengthPrefixed(..state, source:, buffered:),
          )
        Ok(None), 0 if state.payload_size == None -> Done
        Ok(None), got ->
          Next(Error(IncompleteFrame(expected: needed, got:)), empty())
        Error(error), _ -> Next(Error(error), empty())
      }
  }
}

// Handle `bytes` just taken from the buffer: a payload, or a prefix.
fn binary_length_prefixed_frame(
  state: LengthPrefixed,
  bytes: BitArray,
) -> Step(Result(BitArray, BinaryError), Stream(Result(BitArray, BinaryError))) {
  case state.payload_size, binary_read_prefix(bytes, state.prefix_size) {
    Some(_), _ ->
      Next(
        Ok(bytes),
        binary_length_prefixed_active(
          LengthPrefixed(..state, payload_size: None),
        ),
      )
    None, length if length > state.max_frame_size -> {
      // The bytes after an oversized prefix cannot be framed again,
      // so the stream ends here.
      close(state.source)
      Next(
        Error(FrameTooLarge(claimed: length, max: state.max_frame_size)),
        empty(),
      )
    }
    None, length ->
      binary_length_prefixed_pull(
        LengthPrefixed(..state, payload_size: Some(length)),
      )
  }
}

fn binary_read_prefix(buffer: BitArray, size: Int) -> Int {
  case size, buffer {
    1, <<count:size(8), _:bits>> -> count
    2, <<count:size(16), _:bits>> -> count
    4, <<count:size(32), _:bits>> -> count
    // Two 32-bit halves: a single 64-bit integer segment is truncated on
    // the JavaScript target. The result on Erlang is identical.
    8, <<high:size(32), low:size(32), _:bits>> -> high * 4_294_967_296 + low
    _, _ -> 0
  }
}

// --- delimited ------------------------------------------------------------

/// Split binary chunks into delimiter-separated frames.
///
/// Require a nonempty byte-aligned delimiter. Return an argument error before consuming the source if it is invalid.
///
/// Emit `Ok` frames without the delimiter. Preserve empty frames between delimiters and after a trailing delimiter, and emit an unfinished final frame. Memory use depends on the largest unfinished frame.
///
/// A non-byte-aligned input chunk emits one `UnalignedChunk` error, closes the latest upstream continuation, and ends the stream. Discard the offending chunk and unfinished buffered frame.
pub fn delimited(
  over stream: Stream(BitArray),
  on delimiter: BitArray,
) -> Result(Stream(Result(BitArray, BinaryError)), ArgumentError) {
  case delimiter {
    <<>> -> Error(EmptyDelimiter)
    _ -> {
      let size = bit_array.bit_size(delimiter)
      case size % 8 {
        0 ->
          Ok(
            binary_delimited_active(Delimited(
              source: stream,
              delimiter:,
              frame: [],
              recent: [],
              unscanned: <<>>,
              has_seen_input: False,
            )),
          )
        _ -> Error(UnalignedArgument(argument: "on", bit_size: size))
      }
    }
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

fn binary_delimited_active(
  state: Delimited,
) -> Stream(Result(BitArray, BinaryError)) {
  make(pull: fn() { binary_delimited_pull(state) }, close: fn() {
    close(state.source)
  })
}

fn binary_delimited_pull(
  state: Delimited,
) -> Step(Result(BitArray, BinaryError), Stream(Result(BitArray, BinaryError))) {
  let window = bit_array.concat(list.reverse([state.unscanned, ..state.recent]))
  let delimiter_size = bit_array.byte_size(state.delimiter)
  case binary_find_delimiter(window, state.delimiter, 0) {
    Ok(position) ->
      case window {
        <<
          before:bytes-size(position),
          _:bytes-size(delimiter_size),
          after:bytes,
        >> ->
          Next(
            Ok(bit_array.concat(list.reverse([before, ..state.frame]))),
            binary_delimited_active(
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
        core_keep_newest(
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
      case binary_pull(state.source) {
        Next(Ok(chunk), source) ->
          binary_delimited_pull(
            Delimited(
              ..state,
              source:,
              unscanned: chunk,
              has_seen_input: state.has_seen_input
                || bit_array.byte_size(chunk) > 0,
            ),
          )
        Next(Error(error), _) -> Next(Error(error), empty())
        Done ->
          case state.has_seen_input {
            True ->
              Next(
                Ok(
                  bit_array.concat(
                    list.reverse(list.append(recent, state.frame)),
                  ),
                ),
                empty(),
              )
            False -> Done
          }
      }
    }
  }
}

fn binary_find_delimiter(
  buffer: BitArray,
  delimiter: BitArray,
  position: Int,
) -> Result(Int, Nil) {
  let buffer_size = bit_array.byte_size(buffer)
  let delimiter_size = bit_array.byte_size(delimiter)
  case position + delimiter_size > buffer_size {
    True -> Error(Nil)
    False ->
      case bit_array.slice(buffer, position, delimiter_size) {
        Ok(window) ->
          case window == delimiter {
            True -> Ok(position)
            False -> binary_find_delimiter(buffer, delimiter, position + 1)
          }
        Error(_) -> Error(Nil)
      }
  }
}

/// Group binary input into frames of exactly `size` bytes.
///
/// Require `size >= 1`; reject invalid sizes before consuming the source. Emit each complete frame as `Ok`. Discard a byte-aligned final fragment shorter than one frame.
///
/// A non-byte-aligned input chunk emits one `UnalignedChunk` error, closes the latest upstream continuation, and ends the stream. Discard the offending chunk and any unfinished frame.
pub fn fixed_size(
  over stream: Stream(BitArray),
  size size: Int,
) -> Result(Stream(Result(BitArray, BinaryError)), ArgumentError) {
  use _ <- result.try(check_positive_argument("fixed_size", "size", size))
  Ok(binary_fixed_size_active(stream, binary_nothing_buffered, size))
}

fn binary_fixed_size_active(
  source: Stream(BitArray),
  buffered: Buffered,
  size: Int,
) -> Stream(Result(BitArray, BinaryError)) {
  make(
    pull: fn() { binary_fixed_size_pull(source, buffered, size) },
    close: fn() { close(source) },
  )
}

fn binary_fixed_size_pull(
  source: Stream(BitArray),
  buffered: Buffered,
  size: Int,
) -> Step(Result(BitArray, BinaryError), Stream(Result(BitArray, BinaryError))) {
  case buffered.size >= size {
    True -> {
      case binary_take_bytes(buffered, size) {
        Ok(#(frame, buffered)) ->
          Next(Ok(frame), binary_fixed_size_active(source, buffered, size))
        Error(_) -> {
          close(source)
          Done
        }
      }
    }
    False ->
      case binary_refill(source, buffered) {
        Ok(Some(#(source, buffered))) ->
          binary_fixed_size_pull(source, buffered, size)
        // The trailing partial frame, if any, is discarded.
        Ok(None) -> Done
        Error(error) -> Next(Error(error), empty())
      }
  }
}

/// Close `source` unless it already returned `Done`, in which case it
/// closed itself and must not be closed again.
fn core_close_unless_drained(source: Stream(a), drained drained: Bool) -> Nil {
  case drained {
    True -> Nil
    False -> close(source)
  }
}

/// Split newest-first `chunks` into the newest ones that together hold
/// at least `wanted` bytes, as measured by `size`, and the older rest.
/// Used by the delimiter searches to keep only the input a delimiter
/// could still start in.
fn core_keep_newest(
  chunks: List(chunk),
  holding wanted: Int,
  measured_by size: fn(chunk) -> Int,
) -> #(List(chunk), List(chunk)) {
  core_do_keep_newest(chunks, wanted, size, [])
}

fn core_do_keep_newest(
  chunks: List(chunk),
  wanted: Int,
  size: fn(chunk) -> Int,
  reversed_kept: List(chunk),
) -> #(List(chunk), List(chunk)) {
  case chunks, wanted > 0 {
    [chunk, ..older], True ->
      core_do_keep_newest(older, wanted - size(chunk), size, [
        chunk,
        ..reversed_kept
      ])
    _, _ -> #(list.reverse(reversed_kept), chunks)
  }
}

@target(erlang)
/// Read messages from an Erlang subject in its owning process.
///
/// Check `keep_running` before each receive attempt. Poll at intervals of 100 milliseconds while no message is available. The subject remains owned by the caller and is not closed by the stream.
///
/// Available on Erlang only. Parallel map and each pull the source in the caller, so they can consume it in the owning process. `merge`, `race`, time operators, and `timeout` pull inside workers and cannot consume this stream directly.
///
/// ## Panics
///
/// Receiving from a process other than the subject's owner panics.
pub fn from_subject(
  from subject: Subject(a),
  while keep_running: fn() -> Bool,
) -> Stream(a) {
  unfold(from: Nil, with: fn(_) {
    erlang_source_receive_loop(subject, keep_running)
  })
}

@target(erlang)
const subject_poll_milliseconds: Int = 100

@target(erlang)
fn erlang_source_receive_loop(
  subject: Subject(a),
  keep_running: fn() -> Bool,
) -> Step(a, Nil) {
  case keep_running() {
    False -> Done
    True ->
      case process.receive(from: subject, within: subject_poll_milliseconds) {
        Ok(message) -> Next(element: message, state: Nil)
        Error(_) -> erlang_source_receive_loop(subject, keep_running)
      }
  }
}

fn check_positive_argument(
  function: String,
  argument: String,
  given: Int,
) -> Result(Nil, ArgumentError) {
  case given >= 1 {
    True -> Ok(Nil)
    False -> Error(NotPositive(function:, argument:, given:))
  }
}

@target(erlang)
fn time_check_duration(
  function: String,
  milliseconds: Int,
  minimum: Int,
) -> Result(Nil, ArgumentError) {
  case milliseconds >= minimum && milliseconds <= 4_294_967_295 {
    True -> Ok(Nil)
    False -> Error(InvalidDuration(function:, milliseconds:))
  }
}

@target(erlang)
/// Emit a monotonic millisecond timestamp after each requested interval.
///
/// Available on Erlang only. Require `milliseconds` in `1..4_294_967_295`. The first pull waits one interval; subsequent pulls also wait one interval. Construction starts no timer.
pub fn ticks(every milliseconds: Int) -> Result(Stream(Int), ArgumentError) {
  use _ <- result.try(time_check_duration("ticks", milliseconds, 1))
  Ok({
    unfold(from: Nil, with: fn(_) {
      process.sleep(milliseconds)
      Next(element: pump_now_milliseconds(), state: Nil)
    })
  })
}

@target(erlang)
/// Emit `Nil` after each requested interval.
///
/// Available on Erlang only. Require `milliseconds` in `1..4_294_967_295`. Each pull waits one interval, including the first. Construction starts no timer.
pub fn interval(every milliseconds: Int) -> Result(Stream(Nil), ArgumentError) {
  use _ <- result.try(time_check_duration("interval", milliseconds, 1))
  Ok({
    unfold(from: Nil, with: fn(_) {
      process.sleep(milliseconds)
      Next(element: Nil, state: Nil)
    })
  })
}

@target(erlang)
/// Limit how long each upstream element may take to arrive.
///
/// Available on Erlang only. Require `milliseconds` in `0..4_294_967_295`. Zero polls for an already available worker reply and can time out even for a synchronous source.
///
/// Emit `Ok(element)` for each timely result. On a deadline, emit one `Error(Nil)` and end. Pull the source in one worker for the whole run, starting on the first pull. Early termination or timeout asks that worker to stop; cleanup waits for any pull callback in progress to return, without blocking the caller for it.
///
/// The source must support consumption in a worker process. A subject stream or shared broadcast consumer tied to the caller cannot be used directly. Worker failures propagate as exceptions after attempted cleanup.
pub fn timeout(
  over stream: Stream(a),
  within milliseconds: Int,
) -> Result(Stream(Result(a, Nil)), ArgumentError) {
  use _ <- result.try(time_check_duration("timeout", milliseconds, 0))
  Ok({
    use upstream, _now <- pump_on_first_pull(
      stream,
      context: "datastream.timeout",
    )
    erlang_source_timeout_pull(upstream, milliseconds)
  })
}

@target(erlang)
fn erlang_source_timeout_active(
  upstream: Pump(a),
  milliseconds: Int,
) -> Stream(Result(a, Nil)) {
  make(
    pull: fn() { erlang_source_timeout_pull(upstream, milliseconds) },
    close: fn() { pump_stop_pump(upstream) },
  )
}

@target(erlang)
fn erlang_source_timeout_pull(
  upstream: Pump(a),
  milliseconds: Int,
) -> Step(Result(a, Nil), Stream(Result(a, Nil))) {
  case pump_next(upstream, within: milliseconds) {
    #(upstream, Ok(Element(element))) ->
      Next(Ok(element), erlang_source_timeout_active(upstream, milliseconds))
    #(_, Ok(Exhausted)) -> Done
    #(upstream, Error(_)) -> {
      pump_stop_pump(upstream)
      Next(Error(Nil), empty())
    }
  }
}

@target(erlang)
/// Send each element to an Erlang subject, preserving input order.
///
/// Available on Erlang only. Consume the whole source without closing the subject.
pub fn into_subject(over stream: Stream(a), into subject: Subject(a)) -> Nil {
  each(over: stream, with: fn(element) { process.send(subject, element) })
}

@target(erlang)
/// Milliseconds remaining until `deadline`, clamped to a minimum of
/// 0. Used as the `within` argument for `process.receive`.
fn time_milliseconds_until(deadline: Int) -> Int {
  case deadline - pump_now_milliseconds() {
    delta if delta > 0 -> delta
    _ -> 0
  }
}

// --- shared close handler ------------------------------------------------

@target(erlang)
fn time_maybe_stop(upstream: Pump(a), upstream_done: Bool) -> Nil {
  case upstream_done {
    True -> Nil
    False -> pump_stop_pump(upstream)
  }
}

// --- debounce ------------------------------------------------------------

@target(erlang)
type DebounceState(a) {
  DebounceState(
    upstream: Pump(a),
    latest: Option(a),
    deadline: Int,
    upstream_done: Bool,
  )
}

@target(erlang)
/// Emit the latest element after the source stays quiet for the requested duration.
///
/// Available on Erlang only. Require `milliseconds` in `1..4_294_967_295`. A new element restarts the quiet period. If the source finishes, emit a pending value immediately without waiting for the quiet period. Pull the source in a worker, so it must not require the caller's process. See the module's worker cleanup contract.
pub fn debounce(
  over stream: Stream(a),
  quiet_for milliseconds: Int,
) -> Result(Stream(a), ArgumentError) {
  use _ <- result.try(time_check_duration("debounce", milliseconds, 1))
  Ok({
    use upstream, _now <- pump_on_first_pull(
      stream,
      context: "datastream.debounce",
    )
    time_debounce_step(
      DebounceState(upstream:, latest: None, deadline: 0, upstream_done: False),
      milliseconds,
    )
  })
}

@target(erlang)
fn time_build_debounce_stream(
  state: DebounceState(a),
  milliseconds: Int,
) -> Stream(a) {
  make(pull: fn() { time_debounce_step(state, milliseconds) }, close: fn() {
    time_maybe_stop(state.upstream, state.upstream_done)
  })
}

@target(erlang)
fn time_debounce_step(
  state: DebounceState(a),
  milliseconds: Int,
) -> Step(a, Stream(a)) {
  case state.upstream_done, state.latest {
    True, _ -> Done
    False, None ->
      case pump_next_forever(state.upstream) {
        #(upstream, Element(element)) ->
          time_debounce_step(
            time_restart_quiet_period(state, upstream, element, milliseconds),
            milliseconds,
          )
        #(_, Exhausted) -> Done
      }
    False, Some(value) ->
      case
        pump_next(
          state.upstream,
          within: time_milliseconds_until(state.deadline),
        )
      {
        #(upstream, Ok(Element(element))) ->
          time_debounce_step(
            time_restart_quiet_period(state, upstream, element, milliseconds),
            milliseconds,
          )
        #(upstream, Ok(Exhausted)) ->
          Next(
            value,
            time_build_debounce_stream(
              DebounceState(
                ..state,
                upstream:,
                latest: None,
                upstream_done: True,
              ),
              milliseconds,
            ),
          )
        #(upstream, Error(_)) ->
          Next(
            value,
            time_build_debounce_stream(
              DebounceState(..state, upstream:, latest: None),
              milliseconds,
            ),
          )
      }
  }
}

@target(erlang)
/// A new element arrived: it becomes the candidate and the quiet period
/// starts again.
fn time_restart_quiet_period(
  state: DebounceState(a),
  upstream: Pump(a),
  element: a,
  milliseconds: Int,
) -> DebounceState(a) {
  DebounceState(
    ..state,
    upstream:,
    latest: Some(element),
    deadline: pump_now_milliseconds() + milliseconds,
  )
}

// --- throttle ------------------------------------------------------------

@target(erlang)
type ThrottleState(a) {
  ThrottleState(upstream: Pump(a), next_window_start: Option(Int))
}

@target(erlang)
/// Emit the first element of each time window and discard the rest.
///
/// Available on Erlang only. Require `milliseconds` in `1..4_294_967_295`. Start a window when an element is emitted. Pull the source in a worker, so it must not require the caller's process.
pub fn throttle(
  over stream: Stream(a),
  every milliseconds: Int,
) -> Result(Stream(a), ArgumentError) {
  use _ <- result.try(time_check_duration("throttle", milliseconds, 1))
  Ok({
    use upstream, _now <- pump_on_first_pull(
      stream,
      context: "datastream.throttle",
    )
    time_throttle_step(
      ThrottleState(upstream:, next_window_start: None),
      milliseconds,
    )
  })
}

@target(erlang)
fn time_build_throttle_stream(
  state: ThrottleState(a),
  milliseconds: Int,
) -> Stream(a) {
  make(pull: fn() { time_throttle_step(state, milliseconds) }, close: fn() {
    pump_stop_pump(state.upstream)
  })
}

@target(erlang)
fn time_throttle_step(
  state: ThrottleState(a),
  milliseconds: Int,
) -> Step(a, Stream(a)) {
  case pump_next_forever(state.upstream) {
    #(_, Exhausted) -> Done
    #(upstream, Element(element)) -> {
      let state = ThrottleState(..state, upstream:)
      let now = pump_now_milliseconds()
      case state.next_window_start {
        Some(start) if now < start -> time_throttle_step(state, milliseconds)
        Some(_) | None ->
          Next(
            element,
            time_build_throttle_stream(
              ThrottleState(
                ..state,
                next_window_start: Some(now + milliseconds),
              ),
              milliseconds,
            ),
          )
      }
    }
  }
}

// --- sample --------------------------------------------------------------

@target(erlang)
type SampleState(a) {
  SampleState(
    upstream: Pump(a),
    window_start: Int,
    latest: Option(a),
    upstream_done: Bool,
  )
}

@target(erlang)
/// Emit the most recent element at each time-window boundary.
///
/// Available on Erlang only. Require `milliseconds` in `1..4_294_967_295`. A window with no input emits nothing. If the source finishes with a pending value, emit it immediately without waiting for the next boundary. Pull the source in a worker, so it must not require the caller's process.
pub fn sample(
  over stream: Stream(a),
  every milliseconds: Int,
) -> Result(Stream(a), ArgumentError) {
  use _ <- result.try(time_check_duration("sample", milliseconds, 1))
  Ok({
    use upstream, now <- pump_on_first_pull(
      stream,
      context: "datastream.sample",
    )
    time_sample_step(
      SampleState(
        upstream:,
        window_start: now,
        latest: None,
        upstream_done: False,
      ),
      milliseconds,
    )
  })
}

@target(erlang)
fn time_build_sample_stream(
  state: SampleState(a),
  milliseconds: Int,
) -> Stream(a) {
  make(pull: fn() { time_sample_step(state, milliseconds) }, close: fn() {
    time_maybe_stop(state.upstream, state.upstream_done)
  })
}

@target(erlang)
fn time_sample_step(
  state: SampleState(a),
  milliseconds: Int,
) -> Step(a, Stream(a)) {
  case state.upstream_done {
    True -> Done
    False -> time_sample_wait(state, milliseconds)
  }
}

@target(erlang)
fn time_sample_wait(
  state: SampleState(a),
  milliseconds: Int,
) -> Step(a, Stream(a)) {
  let deadline = state.window_start + milliseconds
  case
    pump_next(state.upstream, within: time_milliseconds_until(deadline)),
    state.latest
  {
    #(upstream, Ok(Element(element))), _ ->
      time_sample_wait(
        SampleState(..state, upstream:, latest: Some(element)),
        milliseconds,
      )
    #(_, Ok(Exhausted)), None -> Done
    #(upstream, Ok(Exhausted)), Some(value) ->
      Next(
        value,
        time_build_sample_stream(
          SampleState(..state, upstream:, latest: None, upstream_done: True),
          milliseconds,
        ),
      )
    #(upstream, Error(_)), None ->
      time_sample_wait(
        SampleState(..state, upstream:, window_start: deadline),
        milliseconds,
      )
    #(upstream, Error(_)), Some(value) ->
      Next(
        value,
        time_build_sample_stream(
          SampleState(..state, upstream:, window_start: deadline, latest: None),
          milliseconds,
        ),
      )
  }
}

// --- rate_limit ----------------------------------------------------------

@target(erlang)
type RateState(a) {
  RateState(upstream: Pump(a), window_start: Int, count_in_window: Int)
}

@target(erlang)
/// Delay elements to emit at most `count` per time window.
///
/// Available on Erlang only. Require a positive count and `milliseconds` in `1..4_294_967_295`. Excess elements are delayed rather than discarded. Pull the source in a worker, so it must not require the caller's process.
pub fn rate_limit(
  over stream: Stream(a),
  up_to count: Int,
  every milliseconds: Int,
) -> Result(Stream(a), ArgumentError) {
  use _ <- result.try(check_positive_argument("rate_limit", "up_to", count))
  use _ <- result.try(time_check_duration("rate_limit", milliseconds, 1))
  Ok({
    use upstream, now <- pump_on_first_pull(
      stream,
      context: "datastream.rate_limit",
    )
    time_rate_limit_step(
      RateState(upstream:, window_start: now, count_in_window: 0),
      count,
      milliseconds,
    )
  })
}

@target(erlang)
fn time_build_rate_limit_stream(
  state: RateState(a),
  count: Int,
  milliseconds: Int,
) -> Stream(a) {
  make(
    pull: fn() { time_rate_limit_step(state, count, milliseconds) },
    close: fn() { pump_stop_pump(state.upstream) },
  )
}

@target(erlang)
fn time_rate_limit_step(
  state: RateState(a),
  count: Int,
  milliseconds: Int,
) -> Step(a, Stream(a)) {
  case pump_next_forever(state.upstream) {
    #(_, Exhausted) -> Done
    #(upstream, Element(element)) -> {
      let now = pump_now_milliseconds()
      let state = case now >= state.window_start + milliseconds {
        True -> RateState(upstream:, window_start: now, count_in_window: 0)
        False -> RateState(..state, upstream:)
      }
      let state = case state.count_in_window < count {
        True -> RateState(..state, count_in_window: state.count_in_window + 1)
        False -> {
          // Over the limit: wait for the window to end, then start a new
          // one with this element in it.
          process.sleep(time_milliseconds_until(
            state.window_start + milliseconds,
          ))
          RateState(
            ..state,
            window_start: pump_now_milliseconds(),
            count_in_window: 1,
          )
        }
      }
      Next(element, time_build_rate_limit_stream(state, count, milliseconds))
    }
  }
}

// --- window_time ---------------------------------------------------------

@target(erlang)
type WindowState(a) {
  WindowState(
    upstream: Pump(a),
    buffer: List(a),
    window_start: Int,
    upstream_done: Bool,
  )
}

@target(erlang)
/// Collect each time window's elements into a list.
///
/// Available on Erlang only. Require `milliseconds` in `1..4_294_967_295`. Empty windows emit empty lists. On source completion, immediately emit a nonempty final batch; completion alone does not add an empty batch. Pull the source in a worker, so it must not require the caller's process.
pub fn window_time(
  over stream: Stream(a),
  span milliseconds: Int,
) -> Result(Stream(List(a)), ArgumentError) {
  use _ <- result.try(time_check_duration("window_time", milliseconds, 1))
  Ok({
    use upstream, now <- pump_on_first_pull(
      stream,
      context: "datastream.window_time",
    )
    time_window_step(
      WindowState(
        upstream:,
        buffer: [],
        window_start: now,
        upstream_done: False,
      ),
      milliseconds,
    )
  })
}

@target(erlang)
fn time_build_window_stream(
  state: WindowState(a),
  milliseconds: Int,
) -> Stream(List(a)) {
  make(pull: fn() { time_window_step(state, milliseconds) }, close: fn() {
    time_maybe_stop(state.upstream, state.upstream_done)
  })
}

@target(erlang)
fn time_window_step(
  state: WindowState(a),
  milliseconds: Int,
) -> Step(List(a), Stream(List(a))) {
  case state.upstream_done, state.buffer {
    True, [] -> Done
    True, buffer ->
      Next(
        list.reverse(buffer),
        time_build_window_stream(WindowState(..state, buffer: []), milliseconds),
      )
    False, _ -> {
      let deadline = state.window_start + milliseconds
      case
        pump_next(state.upstream, within: time_milliseconds_until(deadline))
      {
        #(upstream, Ok(Element(element))) ->
          time_window_step(
            WindowState(..state, upstream:, buffer: [element, ..state.buffer]),
            milliseconds,
          )
        #(upstream, Ok(Exhausted)) ->
          time_window_step(
            WindowState(..state, upstream:, upstream_done: True),
            milliseconds,
          )
        #(upstream, Error(_)) ->
          Next(
            list.reverse(state.buffer),
            time_build_window_stream(
              WindowState(
                ..state,
                upstream:,
                buffer: [],
                window_start: deadline,
              ),
              milliseconds,
            ),
          )
      }
    }
  }
}

@target(erlang)
fn parallel_check_arguments(
  function: String,
  max_workers: Int,
  max_buffer: Int,
) -> Result(Nil, ArgumentError) {
  use _ <- result.try(check_positive_argument(
    function,
    "max_workers",
    max_workers,
  ))
  use _ <- result.try(parallel_check_buffer(function, max_buffer))
  case max_buffer >= max_workers {
    True -> Ok(Nil)
    False -> Error(BufferLessThanWorkers(max_workers:, max_buffer:))
  }
}

@target(erlang)
fn parallel_check_buffer(
  function: String,
  max_buffer: Int,
) -> Result(Nil, ArgumentError) {
  check_positive_argument(function, "max_buffer", max_buffer)
}

// --- map_unordered / map_ordered ---------------------------------------------

@target(erlang)
/// Whether `map_*` emits results in input order or as they finish.
type Order {
  InputOrder
  CompletionOrder
}

@target(erlang)
type MapSettings(a, b) {
  MapSettings(
    context: String,
    transform: fn(a) -> b,
    max_workers: Int,
    max_buffer: Int,
    order: Order,
  )
}

@target(erlang)
/// One run of a `map_*` stream. `running` maps each busy worker's
/// dispatch index to its process and monitor, `watching` maps the
/// monitors back to the index, and `finished` holds results that wait
/// for an earlier one under `InputOrder`.
type MapRun(a, b) {
  MapRun(
    settings: MapSettings(a, b),
    source: Stream(a),
    source_drained: Bool,
    mailbox: Mailbox(#(Int, b)),
    reaper: Pid,
    running: Dict(Int, #(Pid, Monitor)),
    watching: Dict(Monitor, Int),
    finished: Dict(Int, b),
    next_dispatch_index: Int,
    next_emit_index: Int,
  )
}

@target(erlang)
/// Map elements concurrently and emit results as they complete.
///
/// Available on Erlang only. Require positive worker and buffer limits, with `max_buffer >= max_workers`. Use `map_ordered`'s source ownership, concurrency bounds, and cleanup contract. Output order may vary between runs.
pub fn map_unordered(
  over stream: Stream(a),
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
  with transform: fn(a) -> b,
) -> Result(Stream(b), ArgumentError) {
  use Nil <- result.map(parallel_check_arguments(
    "map_unordered",
    max_workers,
    max_buffer,
  ))
  parallel_map_stream(
    stream,
    MapSettings(
      context: "datastream.map_unordered",
      transform:,
      max_workers:,
      max_buffer:,
      order: CompletionOrder,
    ),
  )
}

@target(erlang)
/// Map elements concurrently and emit results in input order.
///
/// Available on Erlang only. Require positive worker and buffer limits, with `max_buffer >= max_workers`. Reject invalid arguments before consuming the source. Run at most `max_workers` callbacks concurrently and retain at most `max_buffer` dispatched but unconsumed results, including running work.
///
/// Pull the source in the calling process. Only callbacks run in workers; their effects can occur out of order. Start workers on the first pull. Early termination stops remaining workers and closes the active source. A callback crash propagates after attempted cleanup.
pub fn map_ordered(
  over stream: Stream(a),
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
  with transform: fn(a) -> b,
) -> Result(Stream(b), ArgumentError) {
  use Nil <- result.map(parallel_check_arguments(
    "map_ordered",
    max_workers,
    max_buffer,
  ))
  parallel_map_stream(
    stream,
    MapSettings(
      context: "datastream.map_ordered",
      transform:,
      max_workers:,
      max_buffer:,
      order: InputOrder,
    ),
  )
}

@target(erlang)
/// The mailbox and workers are created on the first pull, in the
/// process that pulls, so every run starts clean.
fn parallel_map_stream(
  stream: Stream(a),
  settings: MapSettings(a, b),
) -> Stream(b) {
  make(
    pull: fn() {
      parallel_map_step(MapRun(
        settings:,
        source: stream,
        source_drained: False,
        mailbox: pump_new_mailbox(),
        reaper: pump_start_reaper(),
        running: dict.new(),
        watching: dict.new(),
        finished: dict.new(),
        next_dispatch_index: 0,
        next_emit_index: 0,
      ))
    },
    close: fn() { close(stream) },
  )
}

@target(erlang)
fn parallel_map_active(run: MapRun(a, b)) -> Stream(b) {
  make(pull: fn() { parallel_map_step(run) }, close: fn() {
    parallel_map_abort(run)
  })
}

@target(erlang)
/// Close the upstream unless it is drained and stop the workers.
fn parallel_map_abort(run: MapRun(a, b)) -> Nil {
  case run.source_drained {
    True -> Nil
    False -> close(run.source)
  }
  parallel_stop_tasks(run)
}

@target(erlang)
/// Kill the busy workers and drop whatever they still send.
fn parallel_stop_tasks(run: MapRun(a, b)) -> Nil {
  let Nil =
    dict.each(run.running, fn(_, worker) {
      let #(pid, monitor) = worker
      process.demonitor_process(monitor)
      process.kill(pid)
    })
  pump_stop_reaper(run.reaper)
  pump_close_mailbox(run.mailbox)
}

@target(erlang)
fn parallel_map_step(run: MapRun(a, b)) -> Step(b, Stream(b)) {
  let run = parallel_map_dispatch(run)
  case dict.get(run.finished, run.next_emit_index), dict.is_empty(run.running) {
    Ok(value), _ ->
      Next(
        value,
        parallel_map_active(
          MapRun(
            ..run,
            finished: dict.delete(run.finished, run.next_emit_index),
            next_emit_index: run.next_emit_index + 1,
          ),
        ),
      )
    Error(_), True -> {
      parallel_stop_tasks(run)
      Done
    }
    Error(_), False ->
      case pump_receive_forever(from: run.mailbox, watching: run.watching) {
        Received(#(index, value)) -> {
          let run = parallel_map_settle(run, index)
          case run.settings.order {
            CompletionOrder -> Next(value, parallel_map_active(run))
            InputOrder ->
              parallel_map_step(
                MapRun(..run, finished: dict.insert(run.finished, index, value)),
              )
          }
        }
        WorkerDown(reason:, ..) -> {
          parallel_map_abort(run)
          pump_raise_worker_crash(run.settings.context, reason)
        }
        TimedOut -> parallel_map_step(run)
      }
  }
}

@target(erlang)
/// Forget the worker for `index` once its result has arrived.
fn parallel_map_settle(run: MapRun(a, b), index: Int) -> MapRun(a, b) {
  case dict.get(run.running, index) {
    Ok(#(_, monitor)) -> {
      process.demonitor_process(monitor)
      MapRun(
        ..run,
        running: dict.delete(run.running, index),
        watching: dict.delete(run.watching, monitor),
      )
    }
    Error(_) -> run
  }
}

@target(erlang)
fn parallel_map_dispatch(run: MapRun(a, b)) -> MapRun(a, b) {
  let busy = dict.size(run.running)
  let in_flight = busy + dict.size(run.finished)
  case
    run.source_drained
    || busy >= run.settings.max_workers
    || in_flight >= run.settings.max_buffer
  {
    True -> run
    False ->
      // A panicking upstream must not leave the busy workers running.
      case
        pump_guard(fn() { pull(run.source) }, on_crash: fn() {
          parallel_stop_tasks(run)
        })
      {
        Done -> MapRun(..run, source_drained: True)
        Next(element, rest) -> {
          let index = run.next_dispatch_index
          let function = run.settings.transform
          let #(pid, monitor) =
            pump_spawn_task(
              into: run.mailbox,
              tagged: index,
              linked_to: run.reaper,
              running: fn() { function(element) },
            )
          parallel_map_dispatch(
            MapRun(
              ..run,
              source: rest,
              running: dict.insert(run.running, index, #(pid, monitor)),
              watching: dict.insert(run.watching, monitor, index),
              next_dispatch_index: index + 1,
            ),
          )
        }
      }
  }
}

@target(erlang)
/// Run effects concurrently and wait for all of them to finish.
///
/// Available on Erlang only. Require positive worker and buffer limits, with `max_buffer >= max_workers`. Invalid arguments leave the source and effects untouched. Pull the source in the caller and run effects in workers, with no order guarantee.
///
/// For effects that must follow input order, use sequential `each`, optionally after `map_ordered` has computed their inputs.
pub fn each_unordered(
  over stream: Stream(a),
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
  with effect: fn(a) -> Nil,
) -> Result(Nil, ArgumentError) {
  use Nil <- result.map(parallel_check_arguments(
    "each_unordered",
    max_workers,
    max_buffer,
  ))
  parallel_map_stream(
    stream,
    MapSettings(
      context: "datastream.each_unordered",
      transform: effect,
      max_workers:,
      max_buffer:,
      order: CompletionOrder,
    ),
  )
  |> drain
}

// --- merge ---------------------------------------------------------------

@target(erlang)
/// One run of a `merge` stream. `workers` holds the pumps whose source
/// has not ended, by index. `waiting_front` / `waiting_back` form a
/// two-list queue of pumps waiting for a request, so that at most
/// `max_buffer` requests are outstanding. `last_emitted` is the pump
/// whose element was handed downstream last; it rejoins the queue on
/// the next pull.
type MergeRun(a) {
  MergeRun(
    mailbox: Mailbox(#(Int, Reply(a))),
    workers: Dict(Int, Worker),
    watching: Dict(Monitor, Int),
    waiting_front: List(Int),
    waiting_back: List(Int),
    last_emitted: Option(Int),
  )
}

@target(erlang)
/// Interleave streams while preserving each source's element order.
///
/// Available on Erlang only. Require `max_buffer >= 1`, limiting pulled elements not yet emitted across all sources. Cross-source order follows worker arrival order and may vary between runs.
///
/// Start a worker per source on the first pull. Sources must support consumption in workers. Early termination asks each worker to stop; cleanup can wait for a pull callback in progress to return.
pub fn merge(
  streams streams: List(Stream(a)),
  max_buffer max_buffer: Int,
) -> Result(Stream(a), ArgumentError) {
  use Nil <- result.map(parallel_check_buffer("merge", max_buffer))
  make(
    pull: fn() {
      parallel_merge_step(parallel_start_merge(streams, max_buffer))
    },
    close: fn() { list.each(streams, close) },
  )
}

@target(erlang)
fn parallel_start_merge(
  streams: List(Stream(a)),
  max_buffer: Int,
) -> MergeRun(a) {
  let mailbox = pump_new_mailbox()
  let workers =
    list.index_map(streams, fn(stream, index) {
      #(index, pump_spawn_pump(over: stream, into: mailbox, tagged: index))
    })
  let #(started, waiting) = list.split(workers, max_buffer)
  list.each(started, fn(entry) { pump_request(entry.1) })
  MergeRun(
    mailbox:,
    workers: dict.from_list(workers),
    watching: parallel_watch_all(workers),
    waiting_front: list.map(waiting, fn(entry) { entry.0 }),
    waiting_back: [],
    last_emitted: None,
  )
}

@target(erlang)
fn parallel_watch_all(workers: List(#(Int, Worker))) -> Dict(Monitor, Int) {
  list.fold(workers, dict.new(), fn(watching, entry) {
    dict.insert(watching, pump_monitor_of(entry.1), entry.0)
  })
}

@target(erlang)
fn parallel_merge_active(run: MergeRun(a)) -> Stream(a) {
  make(pull: fn() { parallel_merge_step(run) }, close: fn() {
    parallel_stop_all(run.workers, run.mailbox)
  })
}

@target(erlang)
fn parallel_stop_all(
  workers: Dict(Int, Worker),
  mailbox: Mailbox(message),
) -> Nil {
  let Nil = dict.each(workers, fn(_, worker) { pump_stop(worker) })
  pump_close_mailbox(mailbox)
}

@target(erlang)
fn parallel_merge_step(run: MergeRun(a)) -> Step(a, Stream(a)) {
  let run = case run.last_emitted {
    Some(index) ->
      parallel_request_next(
        MergeRun(
          ..run,
          waiting_back: [index, ..run.waiting_back],
          last_emitted: None,
        ),
      )
    None -> run
  }
  case dict.is_empty(run.workers) {
    True -> {
      pump_close_mailbox(run.mailbox)
      Done
    }
    False ->
      case pump_receive_forever(from: run.mailbox, watching: run.watching) {
        Received(#(index, Element(element))) ->
          Next(
            element,
            parallel_merge_active(MergeRun(..run, last_emitted: Some(index))),
          )
        Received(#(index, Exhausted)) ->
          parallel_merge_step(
            parallel_request_next(parallel_merge_forget(run, index)),
          )
        WorkerDown(reason:, ..) -> {
          parallel_stop_all(run.workers, run.mailbox)
          pump_raise_worker_crash("datastream.merge", reason)
        }
        TimedOut -> parallel_merge_step(run)
      }
  }
}

@target(erlang)
fn parallel_merge_forget(run: MergeRun(a), index: Int) -> MergeRun(a) {
  case dict.get(run.workers, index) {
    Ok(worker) -> {
      pump_forget(worker)
      MergeRun(
        ..run,
        workers: dict.delete(run.workers, index),
        watching: dict.delete(run.watching, pump_monitor_of(worker)),
      )
    }
    Error(_) -> run
  }
}

@target(erlang)
/// Hand the freed request to the pump that has waited longest.
fn parallel_request_next(run: MergeRun(a)) -> MergeRun(a) {
  let #(front, back) = case run.waiting_front {
    [] -> #(list.reverse(run.waiting_back), [])
    front -> #(front, run.waiting_back)
  }
  case front {
    [index, ..front] -> {
      case dict.get(run.workers, index) {
        Ok(worker) -> pump_request(worker)
        Error(_) -> Nil
      }
      MergeRun(..run, waiting_front: front, waiting_back: back)
    }
    [] -> MergeRun(..run, waiting_front: [], waiting_back: [])
  }
}

// --- race ----------------------------------------------------------------

@target(erlang)
type RaceState(a) {
  /// No source has produced an element yet; `workers` holds the
  /// sources that have not ended.
  RaceWaiting(
    mailbox: Mailbox(#(Int, Reply(a))),
    workers: Dict(Int, Worker),
    watching: Dict(Monitor, Int),
  )
  /// Source `index` won and is the only one still pulled.
  RaceWinning(
    mailbox: Mailbox(#(Int, Reply(a))),
    index: Int,
    winner: Worker,
    watching: Dict(Monitor, Int),
  )
}

@target(erlang)
/// Continue with the first source to produce an element and stop the others.
///
/// Available on Erlang only. Start one worker per source on the first pull. Empty sources do not win; if all sources are empty, emit nothing. The winning worker keeps its source for the remainder of the run.
///
/// Sources must support consumption in workers. Losing workers close their sources once any pull in progress returns. Early termination stops the remaining workers.
pub fn race(streams streams: List(Stream(a))) -> Stream(a) {
  case streams {
    [] -> empty()
    _ ->
      make(
        pull: fn() { parallel_race_step(parallel_start_race(streams)) },
        close: fn() { list.each(streams, close) },
      )
  }
}

@target(erlang)
fn parallel_start_race(streams: List(Stream(a))) -> RaceState(a) {
  let mailbox = pump_new_mailbox()
  let workers =
    list.index_map(streams, fn(stream, index) {
      let worker = pump_spawn_pump(over: stream, into: mailbox, tagged: index)
      pump_request(worker)
      #(index, worker)
    })
  RaceWaiting(
    mailbox:,
    workers: dict.from_list(workers),
    watching: parallel_watch_all(workers),
  )
}

@target(erlang)
fn parallel_race_active(state: RaceState(a)) -> Stream(a) {
  make(pull: fn() { parallel_race_step(state) }, close: fn() {
    case state {
      RaceWaiting(mailbox:, workers:, ..) -> parallel_stop_all(workers, mailbox)
      RaceWinning(mailbox:, winner:, ..) -> {
        pump_stop(winner)
        pump_close_mailbox(mailbox)
      }
    }
  })
}

@target(erlang)
fn parallel_race_step(state: RaceState(a)) -> Step(a, Stream(a)) {
  case state {
    RaceWaiting(mailbox:, workers:, watching:) ->
      case dict.is_empty(workers) {
        True -> {
          pump_close_mailbox(mailbox)
          Done
        }
        False -> parallel_race_wait(mailbox, workers, watching)
      }
    RaceWinning(mailbox:, index:, winner:, watching:) -> {
      pump_request(winner)
      parallel_race_follow(mailbox, index, winner, watching)
    }
  }
}

@target(erlang)
fn parallel_race_wait(
  mailbox: Mailbox(#(Int, Reply(a))),
  workers: Dict(Int, Worker),
  watching: Dict(Monitor, Int),
) -> Step(a, Stream(a)) {
  case pump_receive_forever(from: mailbox, watching:) {
    Received(#(index, Element(element))) -> {
      let #(winner, losers) = case dict.get(workers, index) {
        Ok(winner) -> #(Ok(winner), dict.delete(workers, index))
        Error(_) -> #(Error(Nil), workers)
      }
      let Nil = dict.each(losers, fn(_, loser) { pump_stop(loser) })
      case winner {
        Ok(winner) ->
          Next(
            element,
            parallel_race_active(RaceWinning(
              mailbox:,
              index:,
              winner:,
              watching: dict.from_list([#(pump_monitor_of(winner), index)]),
            )),
          )
        Error(_) ->
          parallel_race_step(RaceWaiting(
            mailbox:,
            workers: dict.new(),
            watching: dict.new(),
          ))
      }
    }
    Received(#(index, Exhausted)) -> {
      case dict.get(workers, index) {
        Ok(worker) -> pump_forget(worker)
        Error(_) -> Nil
      }
      parallel_race_step(RaceWaiting(
        mailbox:,
        workers: dict.delete(workers, index),
        watching:,
      ))
    }
    WorkerDown(reason:, ..) -> {
      parallel_stop_all(workers, mailbox)
      pump_raise_worker_crash("datastream.race", reason)
    }
    TimedOut -> parallel_race_wait(mailbox, workers, watching)
  }
}

@target(erlang)
fn parallel_race_follow(
  mailbox: Mailbox(#(Int, Reply(a))),
  index: Int,
  winner: Worker,
  watching: Dict(Monitor, Int),
) -> Step(a, Stream(a)) {
  case pump_receive_forever(from: mailbox, watching:) {
    // A stopped loser may still deliver the element it was pulling.
    Received(#(from, Element(_)))
      | Received(#(from, Exhausted))
      if from != index
    -> parallel_race_follow(mailbox, index, winner, watching)
    Received(#(_, Element(element))) ->
      Next(
        element,
        parallel_race_active(RaceWinning(mailbox:, index:, winner:, watching:)),
      )
    Received(#(_, Exhausted)) -> {
      pump_forget(winner)
      pump_close_mailbox(mailbox)
      Done
    }
    WorkerDown(reason:, ..) -> {
      pump_close_mailbox(mailbox)
      pump_raise_worker_crash("datastream.race", reason)
    }
    TimedOut -> parallel_race_follow(mailbox, index, winner, watching)
  }
}

// --- mailbox ----------------------------------------------------------------

@target(erlang)
/// An alias of the process that created it, receiving `message`s.
type Mailbox(message)

@target(erlang)
/// What a wait on a mailbox returned.
type Event(message, id) {
  Received(message)
  /// A watched worker exited before its result was received.
  WorkerDown(id: id, reason: Dynamic)
  TimedOut
}

@target(erlang)
@external(erlang, "datastream_mailbox_ffi", "new")
fn pump_new_mailbox() -> Mailbox(message)

@target(erlang)
@external(erlang, "datastream_mailbox_ffi", "send")
fn pump_send(to mailbox: Mailbox(message), message message: message) -> Nil

@target(erlang)
/// Wait up to `ms` milliseconds for a message, or for the exit of a
/// worker whose monitor is a key of `watching`.
@external(erlang, "datastream_mailbox_ffi", "receive_event")
fn pump_receive(
  from mailbox: Mailbox(message),
  watching watching: Dict(Monitor, id),
  within milliseconds: Int,
) -> Event(message, id)

@target(erlang)
@external(erlang, "datastream_mailbox_ffi", "receive_event_forever")
fn pump_receive_forever(
  from mailbox: Mailbox(message),
  watching watching: Dict(Monitor, id),
) -> Event(message, id)

@target(erlang)
/// Deactivate `mailbox` and drop the messages already sent to it.
@external(erlang, "datastream_mailbox_ffi", "close")
fn pump_close_mailbox(mailbox: Mailbox(message)) -> Nil

@target(erlang)
/// Panic in the calling process with a message naming `context` and
/// the crashed worker's exit `reason`.
@external(erlang, "datastream_mailbox_ffi", "raise_worker_crash")
fn pump_raise_worker_crash(context: String, reason: Dynamic) -> a

// --- pump workers -----------------------------------------------------------

@target(erlang)
/// What a pump sends for one request.
type Reply(a) {
  Element(a)
  Exhausted
}

@target(erlang)
type Control {
  Request
  Stop
}

@target(erlang)
/// The consumer's handle on one pump process.
type Worker {
  Worker(monitor: Monitor, control: Subject(Control))
}

@target(erlang)
/// Start a pump for `stream` that answers each request with a
/// `#(id, reply)` message to `mailbox`. The calling process monitors
/// the pump; the pump monitors the calling process.
fn pump_spawn_pump(
  over stream: Stream(a),
  into mailbox: Mailbox(#(id, Reply(a))),
  tagged id: id,
) -> Worker {
  let consumer = process.self()
  let handshake = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      let control = process.new_subject()
      process.send(handshake, control)
      let consumer_monitor = process.monitor(consumer)
      let selector =
        process.new_selector()
        |> process.select(control)
        |> process.select_specific_monitor(consumer_monitor, fn(_) { Stop })
      pump_pump_loop(stream, mailbox, id, selector)
    })
  let monitor = process.monitor(pid)
  Worker(monitor:, control: process.receive_forever(from: handshake))
}

@target(erlang)
fn pump_pump_loop(
  stream: Stream(a),
  mailbox: Mailbox(#(id, Reply(a))),
  id: id,
  selector: process.Selector(Control),
) -> Nil {
  case process.selector_receive_forever(from: selector) {
    Stop -> close(stream)
    Request ->
      case pull(stream) {
        Next(element, rest) -> {
          pump_send(to: mailbox, message: #(id, Element(element)))
          pump_pump_loop(rest, mailbox, id, selector)
        }
        Done -> pump_send(to: mailbox, message: #(id, Exhausted))
      }
  }
}

@target(erlang)
fn pump_monitor_of(worker: Worker) -> Monitor {
  worker.monitor
}

@target(erlang)
/// Ask the pump for one more element.
fn pump_request(worker: Worker) -> Nil {
  process.send(worker.control, Request)
}

@target(erlang)
/// Tell the pump to close its upstream and exit, and stop watching it.
fn pump_stop(worker: Worker) -> Nil {
  process.send(worker.control, Stop)
  process.demonitor_process(worker.monitor)
}

@target(erlang)
/// Stop watching a pump that has replied `Exhausted` and is exiting.
fn pump_forget(worker: Worker) -> Nil {
  process.demonitor_process(worker.monitor)
}

// --- tasks ------------------------------------------------------------------

@target(erlang)
/// Start a process that kills the tasks linked to it when the calling
/// process exits or when `stop_reaper` is called.
@external(erlang, "datastream_mailbox_ffi", "start_reaper")
fn pump_start_reaper() -> Pid

@target(erlang)
@external(erlang, "datastream_mailbox_ffi", "stop_reaper")
fn pump_stop_reaper(reaper: Pid) -> Nil

@target(erlang)
/// Run `body`; if it panics or throws, run `on_crash` first and then let
/// the same error continue.
@external(erlang, "datastream_mailbox_ffi", "guard")
fn pump_guard(body: fn() -> a, on_crash on_crash: fn() -> Nil) -> a

@target(erlang)
/// Run `work` in a new monitored process that sends `#(id, result)` to
/// `mailbox`. The process links to `reaper`, so it is killed when the
/// consumer exits even while `work` is still running.
fn pump_spawn_task(
  into mailbox: Mailbox(#(id, b)),
  tagged id: id,
  linked_to reaper: Pid,
  running work: fn() -> b,
) -> #(Pid, Monitor) {
  let pid =
    process.spawn_unlinked(fn() {
      process.link(reaper)
      pump_send(to: mailbox, message: #(id, work()))
    })
  #(pid, process.monitor(pid))
}

// --- a single pump ------------------------------------------------------------

@target(erlang)
/// One pump together with its mailbox, for combinators that consume a
/// single upstream. `requested` is True while a request is unanswered.
type Pump(a) {
  Pump(
    worker: Worker,
    mailbox: Mailbox(#(Nil, Reply(a))),
    watching: Dict(Monitor, Nil),
    requested: Bool,
    context: String,
  )
}

@target(erlang)
/// Build a stream that starts a pump for `stream` on its first pull and
/// hands it to `begin` together with the current monotonic time.
/// Nothing is spawned or pulled before that first pull, and every run
/// gets its own pump.
fn pump_on_first_pull(
  over stream: Stream(a),
  context context: String,
  begin begin: fn(Pump(a), Int) -> Step(b, Stream(b)),
) -> Stream(b) {
  make(
    pull: fn() {
      let mailbox = pump_new_mailbox()
      let worker = pump_spawn_pump(over: stream, into: mailbox, tagged: Nil)
      let watching = dict.from_list([#(worker.monitor, Nil)])
      begin(
        Pump(worker:, mailbox:, watching:, requested: False, context:),
        pump_now_milliseconds(),
      )
    },
    close: fn() { close(stream) },
  )
}

@target(erlang)
/// Wait up to `ms` milliseconds for the next reply, requesting one
/// first unless a request is still unanswered. After `Exhausted` the
/// pump is finished and must not be used again. A crashed upstream is
/// re-raised as a panic here.
fn pump_next(
  pump: Pump(a),
  within milliseconds: Int,
) -> #(Pump(a), Result(Reply(a), Nil)) {
  let pump = pump_ensure_requested(pump)
  pump_handle_event(
    pump,
    pump_receive(
      from: pump.mailbox,
      watching: pump.watching,
      within: milliseconds,
    ),
  )
}

@target(erlang)
/// Like `next`, without a deadline.
fn pump_next_forever(pump: Pump(a)) -> #(Pump(a), Reply(a)) {
  let pump = pump_ensure_requested(pump)
  case
    pump_handle_event(
      pump,
      pump_receive_forever(from: pump.mailbox, watching: pump.watching),
    )
  {
    #(pump, Ok(reply)) -> #(pump, reply)
    #(pump, Error(_)) -> pump_next_forever(pump)
  }
}

@target(erlang)
fn pump_ensure_requested(pump: Pump(a)) -> Pump(a) {
  case pump.requested {
    True -> pump
    False -> {
      pump_request(pump.worker)
      Pump(..pump, requested: True)
    }
  }
}

@target(erlang)
fn pump_handle_event(
  pump: Pump(a),
  event: Event(#(Nil, Reply(a)), Nil),
) -> #(Pump(a), Result(Reply(a), Nil)) {
  case event {
    Received(#(_, Exhausted)) -> {
      pump_forget(pump.worker)
      pump_close_mailbox(pump.mailbox)
      #(pump, Ok(Exhausted))
    }
    Received(#(_, Element(element))) -> #(
      Pump(..pump, requested: False),
      Ok(Element(element)),
    )
    TimedOut -> #(pump, Error(Nil))
    WorkerDown(reason:, ..) -> {
      pump_close_mailbox(pump.mailbox)
      pump_raise_worker_crash(pump.context, reason)
    }
  }
}

@target(erlang)
/// Stop a pump that has not replied `Exhausted`: it closes its upstream
/// once any pull in progress returns.
fn pump_stop_pump(pump: Pump(a)) -> Nil {
  pump_stop(pump.worker)
  pump_close_mailbox(pump.mailbox)
}

// --- clock --------------------------------------------------------------------

@target(erlang)
@external(erlang, "erlang", "monotonic_time")
fn pump_erlang_monotonic_time(unit: Atom) -> Int

@target(erlang)
/// Monotonic time in milliseconds.
fn pump_now_milliseconds() -> Int {
  pump_erlang_monotonic_time(atom.create("millisecond"))
}

@target(javascript)
/// Expose a synchronous stream through JavaScript's async iterator protocol.
///
/// Available on JavaScript only. The iterator implements `next`, `return`, `throw`, and `Symbol.asyncIterator`. See the [JavaScript guide](javascript.html).
pub type AsyncIterable(a)

@target(javascript)
type PullState(a) {
  Open(Stream(a))
  Closed
}

@target(javascript)
@external(javascript, "./datastream/javascript/async_ffi.mjs", "make_async_iterable")
fn async_make_async_iterable(
  next: fn() -> Result(a, Nil),
  async_stop: fn() -> Nil,
) -> AsyncIterable(a)

@target(javascript)
/// Expose a synchronous stream to a JavaScript async consumer.
///
/// Available on JavaScript only. With callbacks that return normally, each `next()` performs a synchronous pull and returns a promise. The returned iterator is single-use.
///
/// Exhaustion closes a resource through its normal completion path. `return()`, a loop's `break`, and `throw()` close the remaining stream once when callbacks return normally. Cleanup after a callback exception is best-effort. See the [JavaScript guide](javascript.html) for an executable host example.
pub fn to_async_iterable(stream: Stream(a)) -> AsyncIterable(a) {
  let state = ref.new(Open(stream))
  async_make_async_iterable(fn() { async_pull_next(state) }, fn() {
    async_stop(state)
  })
}

@target(javascript)
fn async_pull_next(state: Ref(PullState(a))) -> Result(a, Nil) {
  case ref.get(state) {
    Error(_) | Ok(Closed) -> Error(Nil)
    Ok(Open(stream)) ->
      case pull(stream) {
        Done -> {
          ref.set(state, Closed)
          Error(Nil)
        }
        Next(element, rest) -> {
          ref.set(state, Open(rest))
          Ok(element)
        }
      }
  }
}

@target(javascript)
fn async_stop(state: Ref(PullState(a))) -> Nil {
  case ref.get(state) {
    Error(_) | Ok(Closed) -> Nil
    Ok(Open(stream)) -> {
      ref.set(state, Closed)
      close(stream)
    }
  }
}
