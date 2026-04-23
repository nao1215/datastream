//// BEAM-only bounded-concurrency combinators.
////
//// This module is the single place where multiple processes get
//// involved, so callers can tell at the import site whether a
//// pipeline runs in parallel.
////
//// Three knobs cover the design space: `max_workers` (degree of
//// parallelism), `max_buffer` (back-pressure ceiling), and the
//// explicit ordered-vs-unordered choice. Unbounded concurrency is
//// intentionally not offered.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only.
pub const beam_only_marker: String = "datastream/erlang/par is BEAM-only"

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import datastream/erlang/internal/pump.{
  type Pump, type Stop, PumpDone, PumpElement,
}

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

// --- common argument validation --------------------------------------------

@target(erlang)
fn validate_par_args(max_workers: Int, max_buffer: Int) -> Nil {
  case max_workers >= 1 {
    True -> Nil
    False -> panic as "datastream/erlang/par: max_workers must be >= 1"
  }
  case max_buffer >= 1 {
    True -> Nil
    False -> panic as "datastream/erlang/par: max_buffer must be >= 1"
  }
}

// --- map_unordered --------------------------------------------------------

@target(erlang)
type MapUnordered(a, b) {
  MapUnordered(
    source: Stream(a),
    f: fn(a) -> b,
    max_workers: Int,
    workers_busy: Int,
    result_subject: Subject(b),
    source_drained: Bool,
  )
}

@target(erlang)
/// Run `f` in parallel across at most `max_workers` BEAM processes.
/// Results are emitted in the order they finish, NOT input order.
///
/// `max_workers >= 1` and `max_buffer >= 1` are required; violations
/// `panic` at construction. `max_buffer` is the per-instance prefetch
/// ceiling; this minimal implementation does not yet apply
/// back-pressure beyond capping `workers_busy` at `max_workers`.
pub fn map_unordered(
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
    workers_busy: Int,
    next_dispatch_index: Int,
    next_emit_index: Int,
    result_subject: Subject(#(Int, b)),
    pending: Dict(Int, b),
    source_drained: Bool,
  )
}

@target(erlang)
/// Run `f` in parallel across at most `max_workers` BEAM processes,
/// emitting results in input order.
///
/// `max_buffer >= max_workers` is REQUIRED: if `max_buffer` were
/// smaller, the buffer would fill while waiting for an early result
/// and ordering could not be enforced without stalling. Violation
/// panics at construction.
pub fn map_ordered(
  over stream: Stream(a),
  with f: fn(a) -> b,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Stream(b) {
  validate_par_args(max_workers, max_buffer)
  case max_buffer >= max_workers {
    True -> Nil
    False ->
      panic as "datastream/erlang/par.map_ordered: max_buffer must be >= max_workers"
  }
  let result_subject = process.new_subject()
  build_map_ordered_stream(MapOrdered(
    source: stream,
    f: f,
    max_workers: max_workers,
    workers_busy: 0,
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
  case state.source_drained || state.workers_busy >= state.max_workers {
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
              next_dispatch_index: position + 1,
            ),
          )
        }
      }
  }
}

// --- each_ordered / each_unordered ----------------------------------------

@target(erlang)
/// Side-effect-only variant of `map_ordered`. Drives the stream to
/// completion and returns `Nil`.
pub fn each_ordered(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Nil {
  map_ordered(
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
/// Side-effect-only variant of `map_unordered`. Drives the stream to
/// completion and returns `Nil`.
pub fn each_unordered(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Nil {
  map_unordered(
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
type MergeState(a) {
  MergeState(
    result_subj: Subject(Pump(a)),
    stop_subjs: List(Subject(Stop)),
    total: Int,
    completed: Int,
  )
}

@target(erlang)
/// Interleave elements from every input stream in arrival order.
///
/// Order from each individual source is preserved relative to that
/// source; cross-source order follows whichever worker process
/// happens to deliver first. `max_buffer >= 1` is required.
///
/// One worker process is spawned per input stream; the workers pump
/// their source into a shared subject. The resulting `Stream` walks
/// that subject and counts down workers as they signal that they are
/// done. Downstream early-exit signals every still-running worker to
/// close its upstream.
pub fn merge(
  streams streams: List(Stream(a)),
  max_buffer max_buffer: Int,
) -> Stream(a) {
  case max_buffer >= 1 {
    True -> Nil
    False -> panic as "datastream/erlang/par.merge: max_buffer must be >= 1"
  }
  let result_subj = process.new_subject()
  let total = list.length(streams)
  let stop_subjs =
    list.map(streams, fn(s) { pump.spawn_pump(over: s, into: result_subj) })
  build_merge_stream(MergeState(
    result_subj: result_subj,
    stop_subjs: stop_subjs,
    total: total,
    completed: 0,
  ))
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
    False -> list.each(state.stop_subjs, pump.stop)
  }
}

@target(erlang)
fn merge_step(state: MergeState(a)) -> datastream.Step(a, Stream(a)) {
  case state.completed >= state.total {
    True -> Done
    False ->
      case process.receive_forever(from: state.result_subj) {
        PumpElement(element) -> Next(element, build_merge_stream(state))
        PumpDone ->
          merge_step(MergeState(..state, completed: state.completed + 1))
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
