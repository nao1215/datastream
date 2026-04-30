//// Single-stream middle and composition combinators over `Stream(a)`.
////
//// All combinators are lazy and order-preserving (unless documented
//// otherwise). Building a pipeline with these combinators triggers no
//// user callbacks; callbacks fire only when a terminal in `fold` or
//// `sink` pulls the result.
////
//// Early-exit combinators (`take`, `take_while`) stop pulling upstream
//// the moment their result is determined, which is what makes them
//// safe on infinite sources.
////
//// Every combinator here forwards the upstream's `close` callback so
//// resource-backed streams (`source.resource`) are released on every
//// termination path. Combinators that observe a `Done` from the
//// upstream do NOT call `close` again — the source closed itself when
//// it returned `Done`. Combinators that hold multiple upstreams open
//// at once close them in the order specified in the spec
//// (`flat_map`: inner before outer; `zip`: right before left).

import datastream.{type Step, type Stream, Done, Next}
import datastream/chunk.{type Chunk}
import datastream/internal/ref.{type Ref}
import gleam/list
import gleam/option.{type Option, None, Some}

fn noop() -> Nil {
  Nil
}

// --- single-stream combinators ----------------------------------------------

/// Apply `f` to every element. Cardinality and order are preserved.
pub fn map(over stream: Stream(a), with f: fn(a) -> b) -> Stream(b) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) -> Next(f(element), map(over: rest, with: f))
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Keep only the elements for which `predicate` returns `True`.
///
/// Relative order of the survivors is preserved.
pub fn filter(
  over stream: Stream(a),
  keeping predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.make(pull: fn() { filter_pull(stream, predicate) }, close: fn() {
    datastream.close(stream)
  })
}

fn filter_pull(
  stream: Stream(a),
  predicate: fn(a) -> Bool,
) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) ->
      case predicate(element) {
        True -> Next(element, filter(over: rest, keeping: predicate))
        False -> filter_pull(rest, predicate)
      }
    Done -> Done
  }
}

/// Yield at most the first `n` elements; `n == 0` yields the empty
/// stream.
///
/// `n` MUST be `>= 0`. A negative `n` is rejected at construction
/// time with a panic per the `datastream` module-level
/// invalid-argument policy.
///
/// Stops pulling upstream the moment the `n`th element has been
/// emitted, and closes the upstream on that early exit. This is what
/// makes `take` safe on infinite resource-backed sources.
pub fn take(from stream: Stream(a), up_to n: Int) -> Stream(a) {
  case n < 0 {
    True -> panic as "datastream/stream.take: count must be >= 0"
    False -> take_active(stream, n)
  }
}

fn take_active(stream: Stream(a), n: Int) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case n {
        0 -> {
          datastream.close(stream)
          Done
        }
        _ ->
          case datastream.pull(stream) {
            Next(element, rest) -> Next(element, take_active(rest, n - 1))
            Done -> Done
          }
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Discard the first `n` elements; `n == 0` is the identity.
///
/// `n` MUST be `>= 0`. A negative `n` is rejected at construction
/// time with a panic per the `datastream` module-level
/// invalid-argument policy.
pub fn drop(from stream: Stream(a), up_to n: Int) -> Stream(a) {
  case n < 0 {
    True -> panic as "datastream/stream.drop: count must be >= 0"
    False -> drop_active(stream, n)
  }
}

/// Why a checked stream constructor refused its argument.
///
/// Returned by `take_checked`, `drop_checked`, and any future
/// non-panicking variant of a constructor that would otherwise
/// reject its argument with a `panic`. Lets the caller surface
/// argument-validation failures through `Result` instead of
/// crashing the process — useful when the numeric argument comes
/// from CLI flags, config files, or request parameters.
///
/// `function` names the constructor that rejected the value
/// (`"take"`, `"drop"`, ...) so a caller routing many checked
/// constructors through the same handler can produce a meaningful
/// error message.
pub type StreamArgError {
  NegativeCount(function: String, given: Int)
}

/// Like `take`, but returns the argument-validation failure as a
/// `Result` instead of panicking. Use this when `n` comes from
/// dynamic input (CLI, config, request parameters).
///
/// On success the stream behaves identically to
/// `take(from: stream, up_to: n)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error` — no upstream pull happens, but the
/// upstream is otherwise untouched.
///
/// Example:
///   case stream.take_checked(from: src, up_to: configured_limit) {
///     Ok(s) -> ...
///     Error(stream.NegativeCount(function: _, given: g)) -> ...
///   }
pub fn take_checked(
  from stream: Stream(a),
  up_to n: Int,
) -> Result(Stream(a), StreamArgError) {
  case n < 0 {
    True -> Error(NegativeCount(function: "take", given: n))
    False -> Ok(take_active(stream, n))
  }
}

/// Like `drop`, but returns the argument-validation failure as a
/// `Result` instead of panicking. Use this when `n` comes from
/// dynamic input.
///
/// On success the stream behaves identically to
/// `drop(from: stream, up_to: n)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error`.
pub fn drop_checked(
  from stream: Stream(a),
  up_to n: Int,
) -> Result(Stream(a), StreamArgError) {
  case n < 0 {
    True -> Error(NegativeCount(function: "drop", given: n))
    False -> Ok(drop_active(stream, n))
  }
}

fn drop_active(stream: Stream(a), n: Int) -> Stream(a) {
  datastream.make(pull: fn() { drop_pull(stream, n) }, close: fn() {
    datastream.close(stream)
  })
}

fn drop_pull(stream: Stream(a), n: Int) -> Step(a, Stream(a)) {
  case n {
    0 ->
      case datastream.pull(stream) {
        Next(element, rest) -> Next(element, drop_active(rest, 0))
        Done -> Done
      }
    _ ->
      case datastream.pull(stream) {
        Next(_, rest) -> drop_pull(rest, n - 1)
        Done -> Done
      }
  }
}

/// Yield the longest prefix where `predicate` holds, then stop.
///
/// Stops pulling upstream and closes it as soon as `predicate` returns
/// `False`, so `take_while` terminates on infinite resource-backed
/// sources whose prefix eventually fails the predicate.
pub fn take_while(
  in stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) ->
          case predicate(element) {
            True -> Next(element, take_while(in: rest, satisfying: predicate))
            False -> {
              datastream.close(rest)
              Done
            }
          }
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Discard the longest prefix where `predicate` holds, then yield the rest.
pub fn drop_while(
  in stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.make(
    pull: fn() { drop_while_pull(stream, predicate) },
    close: fn() { datastream.close(stream) },
  )
}

fn drop_while_pull(
  stream: Stream(a),
  predicate: fn(a) -> Bool,
) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) ->
      case predicate(element) {
        True -> drop_while_pull(rest, predicate)
        False -> Next(element, rest)
      }
    Done -> Done
  }
}

/// Yield all of `first`, then all of `second`. If `first` is infinite,
/// `second` is never opened.
///
/// Honours the spec close ordering: `second` is opened only after
/// `first` returns `Done` (and so has already closed itself); on early
/// exit before `first` finishes, `second` is never opened.
///
/// **Close contract:** `first` self-closes when it returns `Done`;
/// `append` does not call `close` on it again. After `first` is
/// exhausted, `second`'s elements are yielded directly — when `second`
/// itself returns `Done`, it self-closes in the same way. On early
/// exit mid-`second`, the downstream's `close` call propagates to the
/// active `second` node.
pub fn append(first: Stream(a), second: Stream(a)) -> Stream(a) {
  datastream.make(pull: fn() { append_pull(first, second) }, close: fn() {
    datastream.close(first)
  })
}

fn append_pull(first: Stream(a), second: Stream(a)) -> Step(a, Stream(a)) {
  case datastream.pull(first) {
    Next(element, rest) -> Next(element, append(rest, second))
    Done -> append_pull_second(second)
  }
}

fn append_pull_second(stream: Stream(a)) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) -> Next(element, rest)
    Done -> Done
  }
}

// --- composition combinators ------------------------------------------------

/// Apply `f` to each element and keep only the `Some(x)` results.
pub fn filter_map(
  over stream: Stream(a),
  with f: fn(a) -> Option(b),
) -> Stream(b) {
  datastream.make(pull: fn() { filter_map_pull(stream, f) }, close: fn() {
    datastream.close(stream)
  })
}

fn filter_map_pull(
  stream: Stream(a),
  f: fn(a) -> Option(b),
) -> Step(b, Stream(b)) {
  case datastream.pull(stream) {
    Next(element, rest) ->
      case f(element) {
        Some(value) -> Next(value, filter_map(over: rest, with: f))
        None -> filter_map_pull(rest, f)
      }
    Done -> Done
  }
}

/// Apply `f` to each element and concatenate the inner streams it
/// produces.
///
/// Pulls one inner stream at a time; the next inner stream is not
/// constructed until the previous one is exhausted. On early exit
/// before the outer is `Done`, the active inner is closed first, then
/// the outer.
///
/// **Close contract:** when an inner stream returns `Done`, `flat_map`
/// does NOT call `close` on it — the source is expected to have
/// released its resources when it returned `Done` (the self-close
/// convention followed by `source.resource`). Custom streams built
/// with `make` must release resources inside `next` on the `Done`
/// path if they hold any.
pub fn flat_map(over stream: Stream(a), with f: fn(a) -> Stream(b)) -> Stream(b) {
  flat_map_outer(stream, f)
}

fn flat_map_outer(outer: Stream(a), f: fn(a) -> Stream(b)) -> Stream(b) {
  datastream.make(pull: fn() { flat_map_outer_pull(outer, f) }, close: fn() {
    datastream.close(outer)
  })
}

fn flat_map_outer_pull(
  outer: Stream(a),
  f: fn(a) -> Stream(b),
) -> Step(b, Stream(b)) {
  case datastream.pull(outer) {
    Done -> Done
    Next(element, outer_rest) -> flat_map_inner_pull(f(element), outer_rest, f)
  }
}

fn flat_map_inner(
  inner: Stream(b),
  outer: Stream(a),
  f: fn(a) -> Stream(b),
) -> Stream(b) {
  datastream.make(
    pull: fn() { flat_map_inner_pull(inner, outer, f) },
    close: fn() {
      datastream.close(inner)
      datastream.close(outer)
    },
  )
}

fn flat_map_inner_pull(
  inner: Stream(b),
  outer: Stream(a),
  f: fn(a) -> Stream(b),
) -> Step(b, Stream(b)) {
  case datastream.pull(inner) {
    Next(value, inner_rest) -> Next(value, flat_map_inner(inner_rest, outer, f))
    Done -> flat_map_outer_pull(outer, f)
  }
}

/// Flatten a stream of streams into a single stream.
///
/// Equivalent to `flat_map(s, fn(x) { x })`; the close ordering is the
/// same.
pub fn flatten(streams: Stream(Stream(a))) -> Stream(a) {
  flat_map(over: streams, with: fn(inner) { inner })
}

/// Walk a `List(Stream(a))` in list order, yielding every element of
/// every stream.
///
/// Each inner stream is closed (by reaching its own `Done`) before the
/// next one is opened. On early exit, the active stream is closed.
pub fn concat(streams: List(Stream(a))) -> Stream(a) {
  case streams {
    [] -> empty_stream()
    [first, ..rest] -> concat_active(first, rest)
  }
}

fn concat_active(active: Stream(a), remaining: List(Stream(a))) -> Stream(a) {
  datastream.make(pull: fn() { concat_pull(active, remaining) }, close: fn() {
    datastream.close(active)
  })
}

fn concat_pull(
  active: Stream(a),
  remaining: List(Stream(a)),
) -> Step(a, Stream(a)) {
  case datastream.pull(active) {
    Next(element, rest) -> Next(element, concat_active(rest, remaining))
    Done ->
      case remaining {
        [] -> Done
        [next, ..rest] -> concat_pull(next, rest)
      }
  }
}

fn empty_stream() -> Stream(a) {
  datastream.make(pull: fn() { Done }, close: noop)
}

/// Left-fold the stream, emitting one output per input.
///
/// The seed itself is NOT emitted, so output cardinality equals input
/// cardinality. On empty input the output is empty.
pub fn scan(
  over stream: Stream(a),
  from initial: b,
  with step: fn(b, a) -> b,
) -> Stream(b) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) -> {
          let new_acc = step(initial, element)
          Next(new_acc, scan(over: rest, from: new_acc, with: step))
        }
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Thread a state through the stream while emitting elements of a
/// possibly different type.
pub fn map_accum(
  over stream: Stream(a),
  from initial: state,
  with step: fn(state, a) -> #(state, b),
) -> Stream(b) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) -> {
          let #(new_acc, output) = step(initial, element)
          Next(output, map_accum(over: rest, from: new_acc, with: step))
        }
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Pair-wise zip two streams; halts the moment either source halts.
///
/// Both upstreams may be open at once. On halt, closes `right` first,
/// then `left`, matching the spec.
pub fn zip(left: Stream(a), right: Stream(b)) -> Stream(#(a, b)) {
  zip_with(left, right, with: fn(l, r) { #(l, r) })
}

/// Combine two streams element-wise with `combiner`; halts the moment
/// either source halts.
///
/// Same close ordering as `zip`: right then left.
pub fn zip_with(
  left: Stream(a),
  right: Stream(b),
  with combiner: fn(a, b) -> c,
) -> Stream(c) {
  datastream.make(pull: fn() { zip_pull(left, right, combiner) }, close: fn() {
    datastream.close(right)
    datastream.close(left)
  })
}

fn zip_pull(
  left: Stream(a),
  right: Stream(b),
  combiner: fn(a, b) -> c,
) -> Step(c, Stream(c)) {
  case datastream.pull(left) {
    Done -> {
      datastream.close(right)
      Done
    }
    Next(left_element, left_rest) ->
      case datastream.pull(right) {
        Done -> {
          datastream.close(left_rest)
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
  intersperse_initial(stream, separator)
}

fn intersperse_initial(stream: Stream(a), separator: a) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Done -> Done
        Next(first, rest) ->
          Next(first, intersperse_awaiting_sep(rest, separator))
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

fn intersperse_awaiting_sep(stream: Stream(a), separator: a) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Done -> Done
        Next(next, rest) ->
          Next(separator, intersperse_pending(next, rest, separator))
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

fn intersperse_pending(pending: a, stream: Stream(a), separator: a) -> Stream(a) {
  datastream.make(
    pull: fn() { Next(pending, intersperse_awaiting_sep(stream, separator)) },
    close: fn() { datastream.close(stream) },
  )
}

/// Call `effect` once per element and re-emit the element unchanged.
pub fn tap(over stream: Stream(a), with effect: fn(a) -> Nil) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) -> {
          effect(element)
          Next(element, tap(over: rest, with: effect))
        }
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Collapse runs of `==`-equal adjacent values to a single occurrence.
pub fn dedupe_adjacent(stream: Stream(a)) -> Stream(a) {
  dedupe_active(stream, None)
}

fn dedupe_active(stream: Stream(a), last: Option(a)) -> Stream(a) {
  datastream.make(pull: fn() { dedupe_pull(stream, last) }, close: fn() {
    datastream.close(stream)
  })
}

fn dedupe_pull(stream: Stream(a), last: Option(a)) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Done -> Done
    Next(element, rest) ->
      case last {
        Some(prev) if prev == element -> dedupe_pull(rest, Some(prev))
        _ -> Next(element, dedupe_active(rest, Some(element)))
      }
  }
}

/// Eagerly pull `capacity` elements ahead from `stream` and yield them
/// to the consumer at its pace, refilling the internal queue back to
/// `capacity` after every consumer pull.
///
/// `buffer` is the *prefetch* combinator: it decouples the consumer's
/// pull cadence from upstream latency. For latency-bound upstreams
/// (HTTP body bytes, cold reads from disk) the consumer can do its own
/// per-element work in parallel with the next upstream pull instead of
/// blocking serially on each one.
///
/// `capacity` MUST be `>= 1`. A `capacity < 1` is rejected at
/// construction time with a panic per the `datastream` module-level
/// invalid-argument policy — `capacity == 0` would defeat the point
/// of buffering, and a negative capacity is programmer error.
///
/// Element type, order, and cardinality are preserved. When upstream
/// returns `Done`, any already-buffered elements are still drained
/// before the buffered stream itself emits `Done`. On consumer-side
/// early termination the upstream is closed once and any unconsumed
/// buffered elements are discarded — the upstream produced them in
/// good faith but resource cleanup wins over delivery, matching
/// `take`'s behaviour.
pub fn buffer(over stream: Stream(a), prefetch capacity: Int) -> Stream(a) {
  case capacity < 1 {
    True -> panic as "datastream/stream.buffer: capacity must be >= 1"
    False -> buffer_active(stream, [], 0, capacity)
  }
}

fn buffer_active(
  stream: Stream(a),
  buf: List(a),
  count: Int,
  capacity: Int,
) -> Stream(a) {
  datastream.make(
    pull: fn() { buffer_pull(stream, buf, count, capacity) },
    close: fn() { datastream.close(stream) },
  )
}

fn buffer_pull(
  stream: Stream(a),
  buf: List(a),
  count: Int,
  capacity: Int,
) -> Step(a, Stream(a)) {
  let #(buf, stream, count) = buffer_fill(stream, buf, count, capacity)
  case buf {
    [] -> Done
    [head, ..tail] ->
      Next(head, buffer_active(stream, tail, count - 1, capacity))
  }
}

fn buffer_fill(
  stream: Stream(a),
  buf: List(a),
  count: Int,
  capacity: Int,
) -> #(List(a), Stream(a), Int) {
  case count >= capacity {
    True -> #(buf, stream, count)
    False ->
      case datastream.pull(stream) {
        Next(element, rest) ->
          buffer_fill(rest, list.append(buf, [element]), count + 1, capacity)
        Done -> #(buf, stream, count)
      }
  }
}

/// Terminate `stream` on the first `True` element from `signal`.
///
/// `interrupt_when` is the *external* counterpart of `take_while`:
/// `take_while` ends a stream when a *pulled* element fails a
/// predicate; `interrupt_when` ends it when an *unrelated* stream
/// produces a fire signal — typically a timer expiry, a parent-
/// shutdown probe, or a cancel-token bridge built with
/// `from_subject`.
///
/// On every consumer pull, `signal` is checked first:
///
/// - `signal` yields `True` → both `stream` and the rest of
///   `signal` are closed and the consumer sees `Done` immediately.
/// - `signal` yields `False` → that signal element is consumed; the
///   pull descends into `stream` and yields its next element.
///   Subsequent consumer pulls see the rest of `signal`.
/// - `signal` returns `Done` → the consumer pull descends into
///   `stream`. `signal` is then ignored on subsequent pulls; a
///   `Done` producer is never re-pulled per the `Step` contract.
///
/// The `Stream(Bool)` shape (rather than `Stream(Nil)`) is what
/// makes counted / time-based signals representable in the pull
/// model — `Done` cannot mean "not yet, ask again later" because
/// `Done` is terminal, so a "fire after N consumer pulls" signal
/// is built as `False`-stream-of-length-N followed by `True`.
///
/// **Close ordering:** on consumer-side early termination `signal`
/// is closed first, then `stream` — right-before-left, matching
/// `zip` and `flat_map`.
pub fn interrupt_when(
  in stream: Stream(a),
  signal signal: Stream(Bool),
) -> Stream(a) {
  interrupt_active(stream, Some(signal))
}

fn interrupt_active(
  stream: Stream(a),
  signal: Option(Stream(Bool)),
) -> Stream(a) {
  datastream.make(pull: fn() { interrupt_pull(stream, signal) }, close: fn() {
    case signal {
      Some(sig) -> datastream.close(sig)
      None -> Nil
    }
    datastream.close(stream)
  })
}

fn interrupt_pull(
  stream: Stream(a),
  signal: Option(Stream(Bool)),
) -> Step(a, Stream(a)) {
  case signal {
    None -> interrupt_pull_stream(stream, None)
    Some(sig) ->
      case datastream.pull(sig) {
        Next(True, sig_rest) -> {
          datastream.close(sig_rest)
          datastream.close(stream)
          Done
        }
        Next(False, sig_rest) -> interrupt_pull_stream(stream, Some(sig_rest))
        Done -> interrupt_pull_stream(stream, None)
      }
  }
}

fn interrupt_pull_stream(
  stream: Stream(a),
  signal: Option(Stream(Bool)),
) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) -> Next(element, interrupt_active(rest, signal))
    Done -> Done
  }
}

// --- broadcast and unzip ----------------------------------------------------

/// Internal state shared by every consumer returned from `broadcast`.
///
/// `upstream` is `None` once the source has signalled `Done` (or once
/// every consumer has called `close` and we proactively closed the
/// source). Each `queue` holds elements that the source has produced
/// but the corresponding consumer has not yet pulled. `active` tracks
/// which consumers are still alive — we stop enqueueing into a closed
/// consumer's queue, and once every flag is `False` we close the
/// upstream from the last close call.
type BroadcastState(a) {
  BroadcastState(
    upstream: Option(Stream(a)),
    queues: List(List(a)),
    active: List(Bool),
  )
}

/// Split `stream` into `n` independent consumer streams that share a
/// single underlying source.
///
/// Each consumer pulls at its own pace; any element the source
/// produces is observed by every still-alive consumer in order. A
/// consumer that lags behind sees the same prefix as a fast consumer,
/// just later. This is the fan-out / pub-sub combinator: an
/// observability tap can run in parallel with the main pipeline
/// without forcing the source to be re-evaluated, which matters for
/// resource-backed and otherwise non-replayable sources.
///
/// `n` MUST be `>= 1`. A `n < 1` is rejected at construction time
/// with a panic per the `datastream` module-level invalid-argument
/// policy.
///
/// **Buffer size:** the per-consumer queue is currently unbounded.
/// A consumer that never pulls while another pulls aggressively will
/// accumulate the entire stream in its queue; users with that risk
/// should compose `broadcast(..., n) |> list.map(stream.take(_, k))`
/// or its equivalent. A bounded variant with an explicit drop policy
/// is left as a follow-up — see the Roadmap.
///
/// **Cross-target:** implemented via a small FFI-backed mutable
/// reference (`datastream/internal/ref`). On Erlang the cell is a
/// process-dictionary slot; on JavaScript it is a one-field mutable
/// object. No `gleam_erlang` processes are spawned; no shared global
/// state. The `Ref(_)` is local to each `broadcast` call.
///
/// **Close contract:** every consumer's `close` is recorded; the
/// upstream is closed exactly once, on whichever call brings the
/// active-consumer count to zero. If the upstream returns `Done`
/// first it self-closes, and the close callbacks are no-ops.
pub fn broadcast(over stream: Stream(a), into n: Int) -> List(Stream(a)) {
  case n < 1 {
    True -> panic as "datastream/stream.broadcast: n must be >= 1"
    False -> {
      let initial =
        BroadcastState(
          upstream: Some(stream),
          queues: list.repeat([], times: n),
          active: list.repeat(True, times: n),
        )
      let state_ref = ref.new(initial)
      build_consumers(state_ref, n, [])
    }
  }
}

fn build_consumers(
  state_ref: Ref(BroadcastState(a)),
  remaining: Int,
  acc: List(Stream(a)),
) -> List(Stream(a)) {
  case remaining {
    0 -> acc
    _ -> {
      let consumer_id = remaining - 1
      build_consumers(state_ref, consumer_id, [
        broadcast_consumer(state_ref, consumer_id),
        ..acc
      ])
    }
  }
}

fn broadcast_consumer(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
) -> Stream(a) {
  datastream.make(
    pull: fn() { broadcast_pull(state_ref, consumer_id) },
    close: fn() { broadcast_close(state_ref, consumer_id) },
  )
}

fn broadcast_pull(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
) -> Step(a, Stream(a)) {
  let state = ref.get(state_ref)
  case list_at(state.queues, consumer_id) {
    [head, ..tail] -> {
      let new_queues = list_replace_at(state.queues, consumer_id, tail)
      ref.set(
        state_ref,
        BroadcastState(
          upstream: state.upstream,
          queues: new_queues,
          active: state.active,
        ),
      )
      Next(head, broadcast_consumer(state_ref, consumer_id))
    }
    [] ->
      case state.upstream {
        None -> Done
        Some(upstream) ->
          case datastream.pull(upstream) {
            Done -> {
              ref.set(
                state_ref,
                BroadcastState(
                  upstream: None,
                  queues: state.queues,
                  active: state.active,
                ),
              )
              Done
            }
            Next(element, rest) -> {
              let new_queues =
                fanout_to_others(
                  state.queues,
                  state.active,
                  consumer_id,
                  element,
                )
              ref.set(
                state_ref,
                BroadcastState(
                  upstream: Some(rest),
                  queues: new_queues,
                  active: state.active,
                ),
              )
              Next(element, broadcast_consumer(state_ref, consumer_id))
            }
          }
      }
  }
}

fn broadcast_close(state_ref: Ref(BroadcastState(a)), consumer_id: Int) -> Nil {
  let state = ref.get(state_ref)
  let new_active = list_replace_at(state.active, consumer_id, False)
  let any_active = list.any(new_active, fn(b) { b })
  let cleared_queue = list_replace_at(state.queues, consumer_id, [])
  case any_active, state.upstream {
    True, _ ->
      ref.set(
        state_ref,
        BroadcastState(
          upstream: state.upstream,
          queues: cleared_queue,
          active: new_active,
        ),
      )
    False, Some(upstream) -> {
      datastream.close(upstream)
      ref.set(
        state_ref,
        BroadcastState(
          upstream: None,
          queues: cleared_queue,
          active: new_active,
        ),
      )
    }
    False, None ->
      ref.set(
        state_ref,
        BroadcastState(
          upstream: None,
          queues: cleared_queue,
          active: new_active,
        ),
      )
  }
}

fn fanout_to_others(
  queues: List(List(a)),
  active: List(Bool),
  skip_id: Int,
  element: a,
) -> List(List(a)) {
  list.index_map(queues, fn(queue, index) {
    case index == skip_id, list_at_bool(active, index) {
      True, _ -> queue
      False, False -> queue
      False, True -> list.append(queue, [element])
    }
  })
}

fn list_at(xs: List(List(a)), index: Int) -> List(a) {
  case xs, index {
    [], _ -> []
    [head, ..], 0 -> head
    [_, ..rest], _ -> list_at(rest, index - 1)
  }
}

fn list_at_bool(xs: List(Bool), index: Int) -> Bool {
  case xs, index {
    [], _ -> False
    [head, ..], 0 -> head
    [_, ..rest], _ -> list_at_bool(rest, index - 1)
  }
}

fn list_replace_at(xs: List(b), index: Int, value: b) -> List(b) {
  case xs, index {
    [], _ -> []
    [_, ..rest], 0 -> [value, ..rest]
    [head, ..rest], _ -> [head, ..list_replace_at(rest, index - 1, value)]
  }
}

/// Counterpart of `zip`: split a `Stream(#(a, b))` into a pair of
/// independent component streams.
///
/// Both component streams see the same elements (left projections
/// for the first, right projections for the second) in the same
/// order. Each consumer pulls at its own pace, sharing the upstream
/// via `broadcast` under the hood.
///
/// **Cross-target:** same FFI-backed shared state as `broadcast`,
/// so `unzip` runs on both Erlang and JavaScript.
///
/// **Buffer size:** unbounded per consumer, inherited from
/// `broadcast`. A pipeline that drives one half of the unzipped pair
/// to completion while the other half stays unpulled will accumulate
/// the entire stream in the lagging half's queue.
pub fn unzip(stream: Stream(#(a, b))) -> #(Stream(a), Stream(b)) {
  case broadcast(over: stream, into: 2) {
    [left, right] -> #(
      map(over: left, with: fn(p) { p.0 }),
      map(over: right, with: fn(p) { p.1 }),
    )
    _ ->
      panic as "datastream/stream.unzip: broadcast(_, 2) returned an unexpected shape"
  }
}

// --- chunked operations -----------------------------------------------------

/// Group adjacent elements into fixed-size chunks.
///
/// `size` MUST be `>= 1`. A `size < 1` is rejected at construction
/// time with a panic per the `datastream` module-level
/// invalid-argument policy.
///
/// The trailing chunk may be smaller than `size` when the source
/// length is not divisible.
pub fn chunks_of(over stream: Stream(a), into size: Int) -> Stream(Chunk(a)) {
  case size < 1 {
    True -> panic as "datastream/stream.chunks_of: size must be >= 1"
    False -> chunks_active(stream, [], 0, size)
  }
}

fn chunks_active(
  source: Stream(a),
  buffer: List(a),
  count: Int,
  size: Int,
) -> Stream(Chunk(a)) {
  datastream.make(
    pull: fn() { chunks_pull(source, buffer, count, size) },
    close: fn() { datastream.close(source) },
  )
}

fn chunks_pull(
  source: Stream(a),
  buffer: List(a),
  count: Int,
  size: Int,
) -> Step(Chunk(a), Stream(Chunk(a))) {
  case datastream.pull(source) {
    Done ->
      case buffer {
        [] -> Done
        _ -> Next(chunk.from_list(list.reverse(buffer)), empty_stream())
      }
    Next(element, rest) -> {
      let new_count = count + 1
      let new_buffer = [element, ..buffer]
      case new_count >= size {
        True ->
          Next(
            chunk.from_list(list.reverse(new_buffer)),
            chunks_active(rest, [], 0, size),
          )
        False -> chunks_pull(rest, new_buffer, new_count, size)
      }
    }
  }
}

/// Group consecutive elements that share `key(element)`.
pub fn group_adjacent(
  over stream: Stream(a),
  by key: fn(a) -> k,
) -> Stream(#(k, Chunk(a))) {
  group_active(stream, None, [], key)
}

fn group_active(
  source: Stream(a),
  current_key: Option(k),
  buffer: List(a),
  key: fn(a) -> k,
) -> Stream(#(k, Chunk(a))) {
  datastream.make(
    pull: fn() { group_pull(source, current_key, buffer, key) },
    close: fn() { datastream.close(source) },
  )
}

fn group_pull(
  source: Stream(a),
  current_key: Option(k),
  buffer: List(a),
  key: fn(a) -> k,
) -> Step(#(k, Chunk(a)), Stream(#(k, Chunk(a)))) {
  case datastream.pull(source) {
    Done -> group_flush(current_key, buffer)
    Next(element, rest) ->
      group_advance(current_key, buffer, element, rest, key)
  }
}

fn group_flush(
  current_key: Option(k),
  buffer: List(a),
) -> Step(#(k, Chunk(a)), Stream(#(k, Chunk(a)))) {
  case current_key {
    None -> Done
    Some(k) -> Next(#(k, chunk.from_list(list.reverse(buffer))), empty_stream())
  }
}

fn group_advance(
  current_key: Option(k),
  buffer: List(a),
  element: a,
  rest: Stream(a),
  key: fn(a) -> k,
) -> Step(#(k, Chunk(a)), Stream(#(k, Chunk(a)))) {
  let element_key = key(element)
  case current_key {
    None -> group_pull(rest, Some(element_key), [element], key)
    Some(prev_key) if prev_key == element_key ->
      group_pull(rest, Some(element_key), [element, ..buffer], key)
    Some(prev_key) ->
      Next(
        #(prev_key, chunk.from_list(list.reverse(buffer))),
        group_active(rest, Some(element_key), [element], key),
      )
  }
}
