//// BEAM-only bounded-concurrency combinators.
////
//// This module is the single place where multiple processes get
//// involved, so callers can tell at the import site whether a
//// pipeline runs in parallel.
////
//// Every public function in this module is gated on
//// `@target(erlang)`. On the JavaScript target the module still
//// compiles, so `import datastream/erlang/par` itself does not fail,
//// but calling any function fails at the call site. The
//// `beam_only_marker` constant exists solely to keep the module
//// non-empty on the JavaScript target.
////
//// ## Concurrency knobs
////
//// - `max_workers`: degree of parallelism. The maximum number of BEAM
////   processes that run the user function at once. Only `map_*` and
////   `each_*` accept this knob; `merge` is fixed at one worker per
////   input stream.
//// - `max_buffer`: in-flight ceiling. The maximum number of elements
////   pulled from the upstream(s) but not yet emitted to the downstream.
////   Workers in flight (mid-`f(x)`) count toward this ceiling.
////
//// `max_buffer >= max_workers` is required where both apply; otherwise
//// the buffer would fill before parallelism is reached.
////
//// ## What happens when `max_buffer` is reached
////
//// | function | behaviour |
//// |----------|-----------|
//// | `map_unordered` | bound collapses to `max_workers` (results emit on receive); new dispatch is paused |
//// | `map_ordered`   | new dispatch is paused; pending dict + workers cap |
//// | `merge`         | per-worker `Continue` ack is withheld; workers wait |
////
//// ## Defaults
////
//// `default_max_workers = 4` and `default_max_buffer = 16` are the
//// values used by the no-argument `merge`, `map_unordered`,
//// `map_ordered`, `each_unordered`, `each_ordered`. They aim at the
//// common case (mixed CPU + IO work, modest cardinality). Tune via
//// the `_with` variants when:
////
//// - workload is IO-bound (consider `max_workers` of 16-128 and a
////   correspondingly larger `max_buffer`)
//// - workload is CPU-bound (consider `max_workers` equal to
////   `erlang:system_info(schedulers_online)`)
//// - upstream is rate-limited or memory-pressured (lower
////   `max_buffer` to throttle pull rate)
////
//// ## Notes
////
//// - A `f(x)` that `panic`s leaves the pipeline blocked; this is a
////   known limitation across all `par.*` combinators.
//// - `merge` workers poll for a coordinator-liveness signal every
////   `merge_worker_liveness_check_ms` ms. If the caller of
////   `merge` / `merge_with` drops the returned stream without
////   reaching `close` (e.g. by losing a reference inside a
////   long-running process), the workers stay resident: the caller is
////   still alive, so the liveness probe says "keep going". Always
////   drive the stream to a terminal or call `close` explicitly.
//// - Unbounded concurrency is intentionally not offered.
////
//// ## Limitation: incompatible with `from_subject`
////
//// Every combinator in this module pulls from its upstream inside a
//// spawned worker process. Streams built from
//// `datastream/erlang/source.from_subject` cannot survive that, since
//// an Erlang `Subject` can only be received by its owning process —
//// the worker would `panic` on the first pull. Wrap subject streams
//// outside the parallel layer (e.g. materialise to a list first) or
//// route them through a non-`par.*` pipeline.

@target(erlang)
const merge_worker_liveness_check_ms: Int = 5000

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only.
pub const beam_only_marker: String = "datastream/erlang/par is BEAM-only"

@target(erlang)
/// Default `max_workers` for the no-argument variants of `map_*` and
/// `each_*`.
pub const default_max_workers: Int = 4

@target(erlang)
/// Default `max_buffer` for the no-argument variants of `merge`,
/// `map_*`, and `each_*`.
pub const default_max_buffer: Int = 16

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import datastream/erlang/internal/pump.{type Stop}

@target(erlang)
import datastream/fold

@target(erlang)
import datastream/source

@target(erlang)
import gleam/dict.{type Dict}

@target(erlang)
import gleam/erlang/process.{type Subject}

@target(erlang)
import gleam/list

@target(erlang)
import gleam/option.{type Option, None, Some}

// --- common argument validation --------------------------------------------

@target(erlang)
fn validate_par_args(max_workers: Int, max_buffer: Int) -> Nil {
  case max_workers >= 1 {
    True -> Nil
    False -> panic as "datastream/erlang/par: max_workers must be >= 1"
  }
  case max_buffer >= max_workers {
    True -> Nil
    False -> panic as "datastream/erlang/par: max_buffer must be >= max_workers"
  }
}

@target(erlang)
fn validate_buffer(max_buffer: Int) -> Nil {
  case max_buffer >= 1 {
    True -> Nil
    False -> panic as "datastream/erlang/par.merge: max_buffer must be >= 1"
  }
}

// --- map_unordered --------------------------------------------------------

@target(erlang)
type MapUnordered(a, b) {
  MapUnordered(
    source: Stream(a),
    f: fn(a) -> b,
    max_workers: Int,
    max_buffer: Int,
    workers_busy: Int,
    result_subject: Subject(b),
    source_drained: Bool,
  )
}

@target(erlang)
/// Run `f` in parallel using `default_max_workers` BEAM processes,
/// emitting results in the order they finish.
///
/// See `map_unordered_with` for tunable concurrency.
pub fn map_unordered(over stream: Stream(a), with f: fn(a) -> b) -> Stream(b) {
  map_unordered_with(
    over: stream,
    with: f,
    max_workers: default_max_workers,
    max_buffer: default_max_buffer,
  )
}

@target(erlang)
/// Run `f` in parallel across at most `max_workers` BEAM processes.
/// Results are emitted in the order they finish, NOT input order.
///
/// `max_workers >= 1` and `max_buffer >= max_workers` are required;
/// violations `panic` at construction. The `max_buffer >= max_workers`
/// rule is enforced uniformly across `par.*` so callers can swap
/// between `map_unordered_with`, `map_ordered_with`, and `merge_with`
/// without re-tuning. Note however that `map_unordered` emits each
/// result the moment it arrives, so `in_flight = workers_busy` and
/// `max_buffer` adds no extra ceiling beyond `max_workers` here. See
/// `default_max_workers` and `default_max_buffer` for sensible
/// starting values.
pub fn map_unordered_with(
  over stream: Stream(a),
  with f: fn(a) -> b,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Stream(b) {
  validate_par_args(max_workers, max_buffer)
  let result_subject = process.new_subject()
  build_map_unordered_stream(MapUnordered(
    source: stream,
    f: f,
    max_workers: max_workers,
    max_buffer: max_buffer,
    workers_busy: 0,
    result_subject: result_subject,
    source_drained: False,
  ))
}

@target(erlang)
fn build_map_unordered_stream(state: MapUnordered(a, b)) -> Stream(b) {
  datastream.make(pull: fn() { map_unordered_step(state) }, close: fn() {
    map_unordered_close(state)
  })
}

@target(erlang)
fn map_unordered_close(state: MapUnordered(a, b)) -> Nil {
  case state.source_drained {
    True -> Nil
    False -> datastream.close(state.source)
  }
}

@target(erlang)
fn map_unordered_step(
  state: MapUnordered(a, b),
) -> datastream.Step(b, Stream(b)) {
  let state = dispatch_unordered(state)
  case state.workers_busy {
    0 -> Done
    _ -> {
      let result = process.receive_forever(from: state.result_subject)
      Next(
        result,
        build_map_unordered_stream(
          MapUnordered(..state, workers_busy: state.workers_busy - 1),
        ),
      )
    }
  }
}

@target(erlang)
fn dispatch_unordered(state: MapUnordered(a, b)) -> MapUnordered(a, b) {
  // The `workers_busy >= max_buffer` check is implied by
  // `workers_busy >= max_workers` together with the validated
  // `max_buffer >= max_workers`, so it is intentionally not duplicated
  // here.
  case state.source_drained || state.workers_busy >= state.max_workers {
    True -> state
    False ->
      case datastream.pull(state.source) {
        Done -> MapUnordered(..state, source_drained: True)
        Next(element, rest) -> {
          let subj = state.result_subject
          let func = state.f
          let _pid =
            process.spawn_unlinked(fn() { process.send(subj, func(element)) })
          dispatch_unordered(
            MapUnordered(
              ..state,
              source: rest,
              workers_busy: state.workers_busy + 1,
            ),
          )
        }
      }
  }
}

// --- map_ordered ---------------------------------------------------------

@target(erlang)
type MapOrdered(a, b) {
  MapOrdered(
    source: Stream(a),
    f: fn(a) -> b,
    max_workers: Int,
    max_buffer: Int,
    workers_busy: Int,
    in_flight: Int,
    next_dispatch_index: Int,
    next_emit_index: Int,
    result_subject: Subject(#(Int, b)),
    pending: Dict(Int, b),
    source_drained: Bool,
  )
}

@target(erlang)
/// Run `f` in parallel using `default_max_workers` BEAM processes,
/// emitting results in input order.
///
/// See `map_ordered_with` for tunable concurrency.
pub fn map_ordered(over stream: Stream(a), with f: fn(a) -> b) -> Stream(b) {
  map_ordered_with(
    over: stream,
    with: f,
    max_workers: default_max_workers,
    max_buffer: default_max_buffer,
  )
}

@target(erlang)
/// Run `f` in parallel across at most `max_workers` BEAM processes,
/// emitting results in input order.
///
/// `max_buffer >= max_workers` is REQUIRED: if `max_buffer` were
/// smaller, the buffer would fill while waiting for an early result
/// and ordering could not be enforced without stalling. Violation
/// panics at construction. `in_flight = workers_busy + pending dict
/// size`; dispatch is paused once it reaches `max_buffer`.
pub fn map_ordered_with(
  over stream: Stream(a),
  with f: fn(a) -> b,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Stream(b) {
  validate_par_args(max_workers, max_buffer)
  let result_subject = process.new_subject()
  build_map_ordered_stream(MapOrdered(
    source: stream,
    f: f,
    max_workers: max_workers,
    max_buffer: max_buffer,
    workers_busy: 0,
    in_flight: 0,
    next_dispatch_index: 0,
    next_emit_index: 0,
    result_subject: result_subject,
    pending: dict.new(),
    source_drained: False,
  ))
}

@target(erlang)
fn build_map_ordered_stream(state: MapOrdered(a, b)) -> Stream(b) {
  datastream.make(pull: fn() { map_ordered_step(state) }, close: fn() {
    map_ordered_close(state)
  })
}

@target(erlang)
fn map_ordered_close(state: MapOrdered(a, b)) -> Nil {
  case state.source_drained {
    True -> Nil
    False -> datastream.close(state.source)
  }
}

@target(erlang)
fn map_ordered_step(state: MapOrdered(a, b)) -> datastream.Step(b, Stream(b)) {
  let state = dispatch_ordered(state)
  case dict.get(state.pending, state.next_emit_index) {
    Ok(value) ->
      Next(
        value,
        build_map_ordered_stream(
          MapOrdered(
            ..state,
            pending: dict.delete(state.pending, state.next_emit_index),
            next_emit_index: state.next_emit_index + 1,
            in_flight: state.in_flight - 1,
          ),
        ),
      )
    Error(_) ->
      case state.workers_busy {
        0 -> Done
        _ -> {
          let #(idx, value) =
            process.receive_forever(from: state.result_subject)
          map_ordered_step(
            MapOrdered(
              ..state,
              workers_busy: state.workers_busy - 1,
              pending: dict.insert(state.pending, idx, value),
            ),
          )
        }
      }
  }
}

@target(erlang)
fn dispatch_ordered(state: MapOrdered(a, b)) -> MapOrdered(a, b) {
  case
    state.source_drained
    || state.workers_busy >= state.max_workers
    || state.in_flight >= state.max_buffer
  {
    True -> state
    False ->
      case datastream.pull(state.source) {
        Done -> MapOrdered(..state, source_drained: True)
        Next(element, rest) -> {
          let subj = state.result_subject
          let func = state.f
          let position = state.next_dispatch_index
          let _pid =
            process.spawn_unlinked(fn() {
              process.send(subj, #(position, func(element)))
            })
          dispatch_ordered(
            MapOrdered(
              ..state,
              source: rest,
              workers_busy: state.workers_busy + 1,
              in_flight: state.in_flight + 1,
              next_dispatch_index: position + 1,
            ),
          )
        }
      }
  }
}

// --- each_ordered / each_unordered ----------------------------------------

@target(erlang)
/// Side-effect-only variant of `map_ordered`. Uses
/// `default_max_workers` and `default_max_buffer`.
pub fn each_ordered(over stream: Stream(a), with effect: fn(a) -> Nil) -> Nil {
  each_ordered_with(
    over: stream,
    with: effect,
    max_workers: default_max_workers,
    max_buffer: default_max_buffer,
  )
}

@target(erlang)
/// Side-effect-only variant of `map_ordered_with`.
pub fn each_ordered_with(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Nil {
  map_ordered_with(
    over: stream,
    with: fn(x) {
      effect(x)
      Nil
    },
    max_workers: max_workers,
    max_buffer: max_buffer,
  )
  |> fold.drain
}

@target(erlang)
/// Side-effect-only variant of `map_unordered`. Uses
/// `default_max_workers` and `default_max_buffer`.
pub fn each_unordered(over stream: Stream(a), with effect: fn(a) -> Nil) -> Nil {
  each_unordered_with(
    over: stream,
    with: effect,
    max_workers: default_max_workers,
    max_buffer: default_max_buffer,
  )
}

@target(erlang)
/// Side-effect-only variant of `map_unordered_with`.
pub fn each_unordered_with(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Nil {
  map_unordered_with(
    over: stream,
    with: fn(x) {
      effect(x)
      Nil
    },
    max_workers: max_workers,
    max_buffer: max_buffer,
  )
  |> fold.drain
}

// --- merge ---------------------------------------------------------------

@target(erlang)
type MergeSignal {
  MergeContinue
  MergeStop
}

@target(erlang)
type MergeMsg(a) {
  MergeNext(from: Subject(MergeSignal), element: a)
  MergeDone(from: Subject(MergeSignal))
}

@target(erlang)
type MergeState(a) {
  MergeState(
    result_subj: Subject(MergeMsg(a)),
    all_signals: List(Subject(MergeSignal)),
    // Round-robin queue of workers waiting for a `Continue`. Stored
    // as a Banker's deque (front + reversed back) so enqueue and
    // dequeue are amortised O(1).
    pending_front: List(Subject(MergeSignal)),
    pending_back: List(Subject(MergeSignal)),
    pending_emit: Option(Subject(MergeSignal)),
    total: Int,
    completed: Int,
  )
}

@target(erlang)
/// Interleave elements from every input stream in arrival order. Uses
/// `default_max_buffer` as the in-flight ceiling.
///
/// See `merge_with` for tunable buffering.
pub fn merge(streams streams: List(Stream(a))) -> Stream(a) {
  merge_with(streams: streams, max_buffer: default_max_buffer)
}

@target(erlang)
/// Interleave elements from every input stream in arrival order.
///
/// Order from each individual source is preserved relative to that
/// source; cross-source order follows whichever worker process
/// happens to deliver first.
///
/// `max_buffer` bounds the number of in-flight elements: pulled by
/// any worker but not yet emitted to the downstream. Implementation:
/// at startup at most `max_buffer` workers are issued a `Continue`
/// signal; the rest wait. Each worker pulls one element, sends it,
/// and waits for the next `Continue`. The coordinator hands a
/// `Continue` to the next waiting worker every time it emits an
/// element downstream. If `max_buffer < len(streams)`, the late-
/// starting workers wait until earlier ones drain.
///
/// `max_buffer >= 1` is required.
pub fn merge_with(
  streams streams: List(Stream(a)),
  max_buffer max_buffer: Int,
) -> Stream(a) {
  validate_buffer(max_buffer)
  let result_subj = process.new_subject()
  let coordinator_pid = process.self()
  let all_signals =
    list.map(streams, fn(stream) {
      spawn_merge_pump(stream, result_subj, coordinator_pid)
    })
  let total = list.length(streams)
  let initial_count = case max_buffer < total {
    True -> max_buffer
    False -> total
  }
  let initial = list.take(all_signals, initial_count)
  let deferred = list.drop(all_signals, initial_count)
  list.each(initial, fn(sig) { process.send(sig, MergeContinue) })
  build_merge_stream(MergeState(
    result_subj: result_subj,
    all_signals: all_signals,
    pending_front: deferred,
    pending_back: [],
    pending_emit: None,
    total: total,
    completed: 0,
  ))
}

@target(erlang)
fn spawn_merge_pump(
  stream: Stream(a),
  result_subj: Subject(MergeMsg(a)),
  coordinator_pid: process.Pid,
) -> Subject(MergeSignal) {
  let handshake = process.new_subject()
  let _pid =
    process.spawn_unlinked(fn() {
      let my_signal = process.new_subject()
      process.send(handshake, my_signal)
      merge_pump(stream, result_subj, my_signal, coordinator_pid)
    })
  process.receive_forever(from: handshake)
}

@target(erlang)
fn merge_pump(
  stream: Stream(a),
  result_subj: Subject(MergeMsg(a)),
  my_signal: Subject(MergeSignal),
  coordinator_pid: process.Pid,
) -> Nil {
  case
    process.receive(from: my_signal, within: merge_worker_liveness_check_ms)
  {
    Ok(MergeStop) -> datastream.close(stream)
    Ok(MergeContinue) ->
      case datastream.pull(stream) {
        Next(element, rest) -> {
          process.send(result_subj, MergeNext(my_signal, element))
          merge_pump(rest, result_subj, my_signal, coordinator_pid)
        }
        Done -> process.send(result_subj, MergeDone(my_signal))
      }
    Error(_) ->
      case process.is_alive(coordinator_pid) {
        True -> merge_pump(stream, result_subj, my_signal, coordinator_pid)
        False -> datastream.close(stream)
      }
  }
}

@target(erlang)
fn build_merge_stream(state: MergeState(a)) -> Stream(a) {
  datastream.make(pull: fn() { merge_step(state) }, close: fn() {
    merge_close(state)
  })
}

@target(erlang)
fn merge_close(state: MergeState(a)) -> Nil {
  case state.completed >= state.total {
    True -> Nil
    False ->
      list.each(state.all_signals, fn(sig) { process.send(sig, MergeStop) })
  }
}

@target(erlang)
fn merge_step(state: MergeState(a)) -> datastream.Step(a, Stream(a)) {
  let state = case state.pending_emit {
    Some(prev) -> release_slot(state, prev, True)
    None -> state
  }
  let state = MergeState(..state, pending_emit: None)
  case state.completed >= state.total {
    True -> Done
    False ->
      case process.receive_forever(from: state.result_subj) {
        MergeNext(from, element) ->
          Next(
            element,
            build_merge_stream(MergeState(..state, pending_emit: Some(from))),
          )
        MergeDone(from) -> {
          let state = release_slot(state, from, False)
          merge_step(MergeState(..state, completed: state.completed + 1))
        }
      }
  }
}

@target(erlang)
fn release_slot(
  state: MergeState(a),
  just_finished: Subject(MergeSignal),
  finished_can_continue: Bool,
) -> MergeState(a) {
  // Round-robin: a worker that just emitted joins the back of the
  // queue, then we hand Continue to whoever is at the front. This
  // gives fair scheduling and avoids starving the first workers when
  // streams > max_buffer. The queue is a Banker's deque
  // (front + reversed back); both operations are amortised O(1).
  let back = case finished_can_continue {
    True -> [just_finished, ..state.pending_back]
    False -> state.pending_back
  }
  case state.pending_front {
    [next, ..rest_front] -> {
      process.send(next, MergeContinue)
      MergeState(..state, pending_front: rest_front, pending_back: back)
    }
    [] ->
      case list.reverse(back) {
        [next, ..rest_front] -> {
          process.send(next, MergeContinue)
          MergeState(..state, pending_front: rest_front, pending_back: [])
        }
        [] -> MergeState(..state, pending_front: [], pending_back: [])
      }
  }
}

// --- race ----------------------------------------------------------------

@target(erlang)
type RaceMsg(a) {
  RaceNext(index: Int, element: a, rest: Stream(a))
  RaceDone(index: Int)
}

@target(erlang)
/// Wait for the first source to emit; signal the other workers to
/// stop, then continue with the winning source until it halts.
///
/// Implementation: spawn one worker per source. Each worker checks
/// for a stop signal, then does a single `datastream.pull` and
/// reports back on a shared subject. The first `RaceNext` wins; the
/// coordinator drains any loser messages already in its mailbox and
/// closes the `rest` they carry, then sends a stop signal to every
/// other worker.
///
/// **Best-effort loser cleanup.** A loser worker that was still
/// inside its `datastream.pull` when the stop signal arrived has no
/// way to observe it and will go on to send its `RaceNext` after the
/// coordinator has stopped reading. Such a `RaceNext` is dropped and
/// its `rest` is not closed. BEAM-internal resources held by the
/// abandoned `rest` are reclaimed by the runtime; external resources
/// (file descriptors, sockets) may leak. Pass external-resource
/// streams to `race` only when this trade-off is acceptable.
pub fn race(streams streams: List(Stream(a))) -> Stream(a) {
  case streams {
    [] -> source.empty()
    _ -> race_with(streams)
  }
}

@target(erlang)
fn race_with(streams: List(Stream(a))) -> Stream(a) {
  let result_subj = process.new_subject()
  let total = list.length(streams)
  let stop_subjs =
    list.index_map(streams, fn(stream, idx) {
      pump.spawn_with_stop(fn(stop_subj) {
        race_first_pull(stream, idx, result_subj, stop_subj)
      })
    })
  build_race_stream(RaceWaiting(stop_subjs, total, result_subj))
}

@target(erlang)
fn race_first_pull(
  stream: Stream(a),
  index: Int,
  result_subj: Subject(RaceMsg(a)),
  stop_subj: Subject(Stop),
) -> Nil {
  case process.receive(from: stop_subj, within: 0) {
    Ok(_) -> datastream.close(stream)
    Error(_) ->
      case datastream.pull(stream) {
        Next(element, rest) ->
          process.send(
            result_subj,
            RaceNext(index: index, element: element, rest: rest),
          )
        Done -> process.send(result_subj, RaceDone(index: index))
      }
  }
}

@target(erlang)
type RaceState(a) {
  RaceWaiting(
    stop_subjs: List(Subject(Stop)),
    remaining: Int,
    subj: Subject(RaceMsg(a)),
  )
  RaceWinning(stream: Stream(a))
}

@target(erlang)
fn build_race_stream(state: RaceState(a)) -> Stream(a) {
  datastream.make(pull: fn() { race_step(state) }, close: fn() {
    race_close(state)
  })
}

@target(erlang)
fn race_close(state: RaceState(a)) -> Nil {
  case state {
    RaceWinning(stream) -> datastream.close(stream)
    RaceWaiting(_, 0, _) -> Nil
    RaceWaiting(stop_subjs, _, _) -> list.each(stop_subjs, pump.stop)
  }
}

@target(erlang)
fn race_step(state: RaceState(a)) -> datastream.Step(a, Stream(a)) {
  case state {
    RaceWinning(stream) ->
      case datastream.pull(stream) {
        Next(element, rest) ->
          Next(element, build_race_stream(RaceWinning(rest)))
        Done -> Done
      }
    RaceWaiting(_, 0, _) -> Done
    RaceWaiting(stop_subjs, remaining, subj) ->
      case process.receive_forever(from: subj) {
        RaceNext(index, element, rest) -> {
          stop_losers(stop_subjs, index)
          drain_loser_messages(subj)
          Next(element, build_race_stream(RaceWinning(rest)))
        }
        RaceDone(_) -> race_step(RaceWaiting(stop_subjs, remaining - 1, subj))
      }
  }
}

@target(erlang)
fn stop_losers(stop_subjs: List(Subject(Stop)), winner_index: Int) -> Nil {
  list.index_map(stop_subjs, fn(stop_subj, idx) {
    case idx == winner_index {
      True -> Nil
      False -> pump.stop(stop_subj)
    }
  })
  Nil
}

@target(erlang)
fn drain_loser_messages(subj: Subject(RaceMsg(a))) -> Nil {
  case process.receive(from: subj, within: 0) {
    Ok(RaceNext(_, _, rest)) -> {
      datastream.close(rest)
      drain_loser_messages(subj)
    }
    Ok(RaceDone(_)) -> drain_loser_messages(subj)
    Error(_) -> Nil
  }
}
