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
import datastream/internal/core
import datastream/internal/ref.{type Ref}
import gleam/list
import gleam/option.{type Option, None, Some}

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
/// Negative `n` is treated as `0` (the empty stream, with the same
/// eager-close semantics as `take(s, 0)`). This matches
/// `gleam/list.take`'s lenient convention so callers can hand
/// `take` an arithmetic result without first clamping it. If you
/// want the previous "reject and surface the bad value" behaviour,
/// reach for `take_checked`, which returns `Error(NegativeCount(...))`
/// on negative input.
///
/// Stops pulling upstream the moment the `n`th element has been
/// emitted, and closes the upstream on that early exit. This is what
/// makes `take` safe on infinite resource-backed sources.
///
/// **Resource handling at `n == 0`** — early-exit semantics extend to
/// the degenerate case: the first `pull` returns `Done` *and* closes
/// the upstream. This is asymmetric with `drop(s, 0)`, which is the
/// identity (no close, no pull). For dynamic-`n` pipelines built from
/// `take_checked` / `drop_checked` where `n` may end up `0`, see the
/// "Asymmetry of `take(s, 0)` and `drop(s, 0)`" subsection below for
/// guidance.
///
/// ## Asymmetry of `take(s, 0)` and `drop(s, 0)`
///
/// | Constructor   | Pull behaviour at `n == 0`           | Resource handling      |
/// |---------------|---------------------------------------|------------------------|
/// | `take(s, 0)`  | First pull returns `Done`            | Closes `s` eagerly     |
/// | `drop(s, 0)`  | Identity (first pull pulls from `s`) | Does **not** pre-close |
///
/// Both choices follow each constructor's primary semantics
/// (`take` is early-exit; `drop` is identity), but the combination
/// surprises callers who treat the two as duals. Practical
/// consequences:
///
/// - **`take(s, 0)` discarded without consumption** — upstream is
///   auto-closed on the first pull, but if no terminal ever pulls,
///   the resource stays open until the calling process exits.
/// - **`drop(s, 0)` discarded without consumption** — upstream is
///   never touched. Callers that build `drop(s, 0)` and throw away
///   the result must close `s` themselves to avoid leaking
///   resource-backed sources (`source.try_resource`, file handles,
///   sockets).
///
/// When `n` comes from config / CLI / request parameters and may be
/// `0`, prefer driving the resulting stream through a terminal
/// (`fold.to_list`, `sink.each`, …) so the close path runs
/// regardless of which branch the runtime took.
pub fn take(over stream: Stream(a), up_to n: Int) -> Stream(a) {
  case n < 0 {
    True -> take_active(stream, 0)
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
/// Negative `n` is treated as `0` (identity stream, no upstream
/// pulls, no close). This matches `gleam/list.drop`'s lenient
/// convention so callers can hand `drop` an arithmetic result
/// without first clamping it. If you want the previous "reject
/// and surface the bad value" behaviour, reach for `drop_checked`,
/// which returns `Error(NegativeCount(...))` on negative input.
///
/// **Resource handling at `n == 0`** — identity semantics: no
/// upstream pull, no close. Asymmetric with `take(s, 0)` (which
/// closes eagerly). Callers building `drop(s, 0)` for unknown `n`
/// and discarding the result without consuming it are responsible
/// for closing `s` themselves. See `take`'s "Asymmetry of `take(s,
/// 0)` and `drop(s, 0)`" subsection above for the side-by-side table
/// and recommended practice.
pub fn drop(over stream: Stream(a), up_to n: Int) -> Stream(a) {
  case n < 0 {
    True -> drop_active(stream, 0)
    False -> drop_active(stream, n)
  }
}

/// Why a checked stream constructor refused its argument.
///
/// Returned by `take_checked`, `drop_checked`, `buffer_checked`,
/// `chunks_of_checked`, and any future checked variant of a
/// constructor that would otherwise panic on its argument or, for
/// `take` and `drop`, treat a negative count as `0`. Lets the caller
/// surface argument-validation failures through `Result` — useful
/// when the numeric argument comes from CLI flags, config files, or
/// request parameters.
///
/// `function` names the constructor that rejected the value
/// (`"take"`, `"drop"`, `"buffer"`, `"chunks_of"`) so a caller
/// routing many checked constructors through the same handler can
/// produce a meaningful error message.
///
/// - `NegativeCount` covers constructors whose contract is `>= 0`
///   (`take`, `drop`).
/// - `NotPositive` covers constructors whose contract is `>= 1`
///   (`buffer`, `chunks_of`).
pub type StreamArgError {
  NegativeCount(function: String, given: Int)
  NotPositive(function: String, given: Int)
}

/// Like `take`, but returns `Error` for a negative `n` instead of
/// treating it as `0`. Use this when `n` comes from
/// dynamic input (CLI, config, request parameters).
///
/// On success the stream behaves identically to
/// `take(over: stream, up_to: n)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error` — no upstream pull happens, but the
/// upstream is otherwise untouched.
///
/// Example:
///   case stream.take_checked(over: src, up_to: configured_limit) {
///     Ok(s) -> ...
///     Error(stream.NegativeCount(function: _, given: g)) -> ...
///   }
pub fn take_checked(
  over stream: Stream(a),
  up_to n: Int,
) -> Result(Stream(a), StreamArgError) {
  case n < 0 {
    True -> Error(NegativeCount(function: "take", given: n))
    False -> Ok(take_active(stream, n))
  }
}

/// Like `drop`, but returns `Error` for a negative `n` instead of
/// treating it as `0`. Use this when `n` comes from
/// dynamic input.
///
/// On success the stream behaves identically to
/// `drop(over: stream, up_to: n)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error`.
pub fn drop_checked(
  over stream: Stream(a),
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
  over stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) ->
          case predicate(element) {
            True -> Next(element, take_while(over: rest, satisfying: predicate))
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
  over stream: Stream(a),
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
pub fn flat_map(
  over stream: Stream(a),
  with f: fn(a) -> Stream(b),
) -> Stream(b) {
  flat_map_active(None, stream, f)
}

fn flat_map_active(
  inner: Option(Stream(b)),
  outer: Stream(a),
  f: fn(a) -> Stream(b),
) -> Stream(b) {
  datastream.make(pull: fn() { flat_map_pull(inner, outer, f) }, close: fn() {
    case inner {
      Some(inner) -> datastream.close(inner)
      None -> Nil
    }
    datastream.close(outer)
  })
}

// One self-recursive loop over both streams, so the JavaScript target
// runs it as a loop instead of growing the stack per empty inner.
fn flat_map_pull(
  inner: Option(Stream(b)),
  outer: Stream(a),
  f: fn(a) -> Stream(b),
) -> Step(b, Stream(b)) {
  case inner {
    Some(current) ->
      case datastream.pull(current) {
        Next(value, rest) -> Next(value, flat_map_active(Some(rest), outer, f))
        Done -> flat_map_pull(None, outer, f)
      }
    None ->
      case datastream.pull(outer) {
        Done -> Done
        Next(element, outer_rest) ->
          flat_map_pull(Some(f(element)), outer_rest, f)
      }
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
    [] -> core.empty()
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

fn intersperse_pending(
  pending: a,
  stream: Stream(a),
  separator: a,
) -> Stream(a) {
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
  case buffer_checked(over: stream, prefetch: capacity) {
    Ok(buffered) -> buffered
    Error(_) -> panic as "datastream/stream.buffer: capacity must be >= 1"
  }
}

/// Like `buffer`, but returns the argument-validation failure as a
/// `Result` instead of panicking. Use this when `capacity` comes
/// from dynamic input (CLI, config, request parameters); the
/// panicking `buffer` remains the right tool for trusted constants.
///
/// On success the stream behaves identically to
/// `buffer(over: stream, prefetch: capacity)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error` — no upstream pull happens, but the
/// upstream is otherwise untouched.
pub fn buffer_checked(
  over stream: Stream(a),
  prefetch capacity: Int,
) -> Result(Stream(a), StreamArgError) {
  case capacity < 1 {
    True -> Error(NotPositive(function: "buffer", given: capacity))
    False -> Ok(new_buffer(stream, capacity))
  }
}

fn new_buffer(stream: Stream(a), capacity: Int) -> Stream(a) {
  buffer_active(BufferState(
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

fn buffer_active(state: BufferState(a)) -> Stream(a) {
  datastream.make(pull: fn() { buffer_pull(state) }, close: fn() {
    case state.upstream {
      Some(upstream) -> datastream.close(upstream)
      None -> Nil
    }
  })
}

fn buffer_pull(state: BufferState(a)) -> Step(a, Stream(a)) {
  let state = buffer_fill(state)
  let #(front, back) = case state.front {
    [] -> #(list.reverse(state.back), [])
    _ -> #(state.front, state.back)
  }
  case front {
    [] -> Done
    [head, ..front] ->
      Next(
        head,
        buffer_active(
          BufferState(..state, front:, back:, count: state.count - 1),
        ),
      )
  }
}

fn buffer_fill(state: BufferState(a)) -> BufferState(a) {
  case state.upstream, state.count >= state.capacity {
    None, _ | _, True -> state
    Some(upstream), False ->
      case datastream.pull(upstream) {
        Next(element, rest) ->
          buffer_fill(
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
  over stream: Stream(a),
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

// State shared by every consumer returned from `broadcast`.
//
// `upstream` is `None` once the source has returned `Done` or has been
// closed because every consumer closed. Each consumer has its own
// queue of elements the source produced but that consumer has not
// pulled yet. Once no consumer is active the state is deleted from its
// `Ref`, so a finished broadcast holds no memory.
type BroadcastState(a) {
  BroadcastState(
    upstream: Option(Stream(a)),
    consumers: List(Consumer(a)),
    // `None` for `broadcast`; `Some(k)` for `broadcast_bounded`, which
    // panics once a queue would hold more than `k` elements.
    max_queue: Option(Int),
  )
}

// A consumer's queue as two lists, `front` in pull order and `back`
// reversed, so both ends are amortised O(1); `size` counts both.
type Consumer(a) {
  Consumer(front: List(a), back: List(a), size: Int, active: Bool)
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
/// `n <= 0` is treated as "no consumers": the upstream is closed
/// immediately and `broadcast` returns the empty list. The lenient
/// convention matches `stream.take` / `stream.drop` and lets callers
/// hand `broadcast` an arithmetic result without first guarding
/// against an off-by-one underflow.
///
/// **Buffer size — unbounded by design.** Per-consumer queues grow
/// without limit. The worst-case memory footprint is
/// `O(max_pull_distance × n)`, where `max_pull_distance` is the
/// difference between the fastest and slowest consumer's pull count
/// at any point in time, and `n` is the consumer count. Concretely:
/// if consumer A pulls 1 000 000 elements while B is paused, B's
/// queue holds 1 000 000 elements until B catches up.
///
/// For resource-backed sources of bounded cardinality this is fine.
/// For cardinality-unbounded sources (`source.iterate`,
/// `source.repeat`, `source.unfold` of an infinite seed) it is a
/// silent memory hazard. Two safer composition patterns:
///
/// - **Cap each consumer**: `broadcast(s, n) |> list.map(stream.take(_, k))`
///   so the slowest consumer's queue is bounded by `k - 1`.
/// - **Use `broadcast_bounded(s, n, max_queue)`** for an explicit
///   per-consumer queue limit. Exceeding the limit panics with a
///   structured message rather than letting the process OOM in
///   production.
///
/// **Cross-target:** implemented via a small FFI-backed mutable
/// reference (`datastream/internal/ref`). On Erlang the cell is a
/// process-dictionary slot; on JavaScript it is a one-field mutable
/// object. No `gleam_erlang` processes are spawned; no shared global
/// state. The `Ref(_)` is local to each `broadcast` call and is freed
/// once every consumer has returned `Done` or been closed.
///
/// **Limitation:** on Erlang every consumer must be pulled in the
/// process that called `broadcast`. Pulling one from another process,
/// for example by handing it to `par.merge`, a `time` combinator or
/// `source.timeout`, panics.
///
/// **Close contract:** every consumer's `close` is recorded; the
/// upstream is closed exactly once, on whichever call brings the
/// active-consumer count to zero. If the upstream returns `Done`
/// first it self-closes, and the close callbacks are no-ops.
pub fn broadcast(over stream: Stream(a), into n: Int) -> List(Stream(a)) {
  start_broadcast(stream, n, None)
}

/// Like `broadcast`, but each per-consumer queue is capped at
/// `max_queue` elements. If a fanned-out element would push any
/// consumer's queue beyond that bound, the next upstream pull panics
/// with a structured message naming the limit. Use this in
/// production wiring (HTTP fan-out, websocket multicast, Kafka
/// producer tees, …) where a stalled slow consumer must surface as a
/// crash instead of an OOM.
///
/// `n <= 0` is treated as "no consumers" (the upstream is closed
/// immediately and the function returns the empty list), matching
/// `broadcast`'s lenient convention. `max_queue` MUST still be
/// `>= 1` — a non-positive buffer capacity has no meaningful
/// interpretation — and is rejected at construction time with a
/// panic that names the function.
///
/// Behaviour on success is identical to `broadcast(s, n)` until the
/// queue bound is hit. Until then there is no overhead beyond a
/// per-pull comparison of each queue's length counter.
///
/// **What "exceeded" means.** The check fires *after* the producing
/// pull has already enqueued the element to every still-alive
/// consumer that wasn't the puller. A consumer queue that was at
/// `max_queue - 1` and gets one more element is fine; one that was
/// at `max_queue` and would become `max_queue + 1` triggers the
/// panic. The slowest consumer therefore has up to `max_queue`
/// elements in its queue before the next overflowing pull stops the
/// pipeline.
///
/// Pair with `broadcast`'s docstring above for the queue-growth
/// formula and the unbounded baseline.
pub fn broadcast_bounded(
  over stream: Stream(a),
  into n: Int,
  max_queue max_queue: Int,
) -> List(Stream(a)) {
  case n >= 1 && max_queue < 1 {
    True ->
      panic as "datastream/stream.broadcast_bounded: max_queue must be >= 1"
    False -> start_broadcast(stream, n, Some(max_queue))
  }
}

fn start_broadcast(
  stream: Stream(a),
  n: Int,
  max_queue: Option(Int),
) -> List(Stream(a)) {
  case n < 1 {
    True -> {
      datastream.close(stream)
      []
    }
    False -> {
      let empty_queue = Consumer(front: [], back: [], size: 0, active: True)
      let state_ref =
        ref.new(BroadcastState(
          upstream: Some(stream),
          consumers: list.repeat(empty_queue, times: n),
          max_queue:,
        ))
      list.repeat(Nil, times: n)
      |> list.index_map(fn(_, consumer_id) {
        broadcast_consumer(state_ref, consumer_id)
      })
    }
  }
}

fn broadcast_consumer(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
) -> Stream(a) {
  datastream.make(
    pull: fn() { broadcast_pull(state_ref, consumer_id) },
    close: fn() { broadcast_finish(state_ref, consumer_id) },
  )
}

fn broadcast_pull(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
) -> Step(a, Stream(a)) {
  case ref.get(state_ref) {
    // Every consumer already finished; this one has nothing left.
    Error(_) -> Done
    Ok(state) ->
      case pop_queued(state.consumers, consumer_id), state.upstream {
        Ok(#(element, consumers)), _ -> {
          ref.set(state_ref, BroadcastState(..state, consumers:))
          Next(element, broadcast_consumer(state_ref, consumer_id))
        }
        Error(_), None -> {
          broadcast_finish(state_ref, consumer_id)
          Done
        }
        Error(_), Some(upstream) ->
          case datastream.pull(upstream) {
            Done -> {
              ref.set(state_ref, BroadcastState(..state, upstream: None))
              broadcast_finish(state_ref, consumer_id)
              Done
            }
            Next(element, rest) -> {
              let consumers =
                enqueue_for_others(state.consumers, consumer_id, element)
              check_queue_bound(consumers, state.max_queue)
              ref.set(
                state_ref,
                BroadcastState(..state, upstream: Some(rest), consumers:),
              )
              Next(element, broadcast_consumer(state_ref, consumer_id))
            }
          }
      }
  }
}

// Marks a consumer as done, by `Done` or by `close`. The last one
// closes the upstream if it is still open and deletes the shared state.
fn broadcast_finish(
  state_ref: Ref(BroadcastState(a)),
  consumer_id: Int,
) -> Nil {
  case ref.get(state_ref) {
    Error(_) -> Nil
    Ok(state) -> {
      let consumers =
        list.index_map(state.consumers, fn(consumer, index) {
          case index == consumer_id {
            True -> Consumer(front: [], back: [], size: 0, active: False)
            False -> consumer
          }
        })
      case
        list.any(consumers, fn(consumer) { consumer.active }),
        state.upstream
      {
        True, _ -> ref.set(state_ref, BroadcastState(..state, consumers:))
        False, Some(upstream) -> {
          ref.delete(state_ref)
          datastream.close(upstream)
        }
        False, None -> ref.delete(state_ref)
      }
    }
  }
}

fn pop_queued(
  consumers: List(Consumer(a)),
  consumer_id: Int,
) -> Result(#(a, List(Consumer(a))), Nil) {
  case consumers, consumer_id {
    [], _ -> Error(Nil)
    [consumer, ..rest], 0 -> {
      let #(front, back) = case consumer.front {
        [] -> #(list.reverse(consumer.back), [])
        front -> #(front, consumer.back)
      }
      case front {
        [element, ..front] -> {
          let consumer =
            Consumer(..consumer, front:, back:, size: consumer.size - 1)
          Ok(#(element, [consumer, ..rest]))
        }
        [] -> Error(Nil)
      }
    }
    [consumer, ..rest], _ ->
      case pop_queued(rest, consumer_id - 1) {
        Ok(#(element, rest)) -> Ok(#(element, [consumer, ..rest]))
        Error(_) -> Error(Nil)
      }
  }
}

fn enqueue_for_others(
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

fn check_queue_bound(
  consumers: List(Consumer(a)),
  max_queue: Option(Int),
) -> Nil {
  case max_queue {
    Some(limit) ->
      case list.any(consumers, fn(consumer) { consumer.size > limit }) {
        True ->
          panic as "datastream/stream.broadcast_bounded: a consumer's queue exceeded max_queue (slow consumer stalling fast producer)"
        False -> Nil
      }
    None -> Nil
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
/// so `unzip` runs on both Erlang and JavaScript, with the same
/// limitation: on Erlang both halves must be pulled in the process that
/// called `unzip`.
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
  case chunks_of_checked(over: stream, into: size) {
    Ok(chunked) -> chunked
    Error(_) -> panic as "datastream/stream.chunks_of: size must be >= 1"
  }
}

/// Like `chunks_of`, but returns the argument-validation failure as
/// a `Result` instead of panicking. Use this when `size` comes from
/// dynamic input (CLI, config, request parameters); the panicking
/// `chunks_of` remains the right tool for trusted constants.
///
/// On success the stream behaves identically to
/// `chunks_of(over: stream, into: size)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error`.
pub fn chunks_of_checked(
  over stream: Stream(a),
  into size: Int,
) -> Result(Stream(Chunk(a)), StreamArgError) {
  case size < 1 {
    True -> Error(NotPositive(function: "chunks_of", given: size))
    False -> Ok(chunks_active(stream, [], 0, size))
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
        _ -> Next(chunk.from_list(list.reverse(buffer)), core.empty())
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
    Done ->
      case current_key {
        None -> Done
        Some(k) ->
          Next(#(k, chunk.from_list(list.reverse(buffer))), core.empty())
      }
    Next(element, rest) -> {
      let element_key = key(element)
      case current_key {
        Some(prev_key) if prev_key != element_key ->
          Next(
            #(prev_key, chunk.from_list(list.reverse(buffer))),
            group_active(rest, Some(element_key), [element], key),
          )
        Some(_) -> group_pull(rest, current_key, [element, ..buffer], key)
        None -> group_pull(rest, Some(element_key), [element], key)
      }
    }
  }
}
