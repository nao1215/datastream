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
  source.unfold(
    from: MapUnordered(
      source: stream,
      f: f,
      max_workers: max_workers,
      workers_busy: 0,
      result_subject: result_subject,
      source_drained: False,
    ),
    with: map_unordered_step,
  )
}

@target(erlang)
fn map_unordered_step(
  state: MapUnordered(a, b),
) -> datastream.Step(b, MapUnordered(a, b)) {
  let state = dispatch_unordered(state)
  case state.workers_busy {
    0 ->
      case state.source_drained {
        True -> Done
        False -> Done
      }
    _ -> {
      let result = process.receive_forever(from: state.result_subject)
      Next(result, MapUnordered(..state, workers_busy: state.workers_busy - 1))
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
  source.unfold(
    from: MapOrdered(
      source: stream,
      f: f,
      max_workers: max_workers,
      workers_busy: 0,
      next_dispatch_index: 0,
      next_emit_index: 0,
      result_subject: result_subject,
      pending: dict.new(),
      source_drained: False,
    ),
    with: map_ordered_step,
  )
}

@target(erlang)
fn map_ordered_step(
  state: MapOrdered(a, b),
) -> datastream.Step(b, MapOrdered(a, b)) {
  let state = dispatch_ordered(state)
  case dict.get(state.pending, state.next_emit_index) {
    Ok(value) ->
      Next(
        value,
        MapOrdered(
          ..state,
          pending: dict.delete(state.pending, state.next_emit_index),
          next_emit_index: state.next_emit_index + 1,
        ),
      )
    Error(_) ->
      case state.workers_busy {
        0 ->
          case state.source_drained {
            True -> Done
            False -> Done
          }
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
type MergeMsg(a) {
  MergeNext(a)
  MergeDone
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
/// that subject and counts down workers as they signal `MergeDone`.
pub fn merge(
  streams streams: List(Stream(a)),
  max_buffer max_buffer: Int,
) -> Stream(a) {
  case max_buffer >= 1 {
    True -> Nil
    False -> panic as "datastream/erlang/par.merge: max_buffer must be >= 1"
  }
  let result_subject = process.new_subject()
  let total = list.length(streams)
  list.each(streams, fn(s) {
    let subj = result_subject
    let _pid = process.spawn_unlinked(fn() { merge_pump(s, subj) })
  })
  source.unfold(from: #(total, 0), with: fn(state) {
    merge_step(state, result_subject)
  })
}

@target(erlang)
fn merge_pump(stream: Stream(a), subj: Subject(MergeMsg(a))) -> Nil {
  case datastream.pull(stream) {
    Next(element, rest) -> {
      process.send(subj, MergeNext(element))
      merge_pump(rest, subj)
    }
    Done -> process.send(subj, MergeDone)
  }
}

@target(erlang)
fn merge_step(
  state: #(Int, Int),
  subj: Subject(MergeMsg(a)),
) -> datastream.Step(a, #(Int, Int)) {
  let #(total, completed) = state
  case completed >= total {
    True -> Done
    False ->
      case process.receive_forever(from: subj) {
        MergeNext(element) -> Next(element, #(total, completed))
        MergeDone -> merge_step(#(total, completed + 1), subj)
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
/// Wait for the first source to emit; cancel and close the others;
/// continue with the winning source until it halts.
///
/// Implementation: spawn one worker per source, each does a single
/// `datastream.pull` and reports back on a shared subject. The first
/// `RaceNext` wins; every other source has its `close` callback
/// invoked. Sources whose first pull returned `Done` are also no-ops
/// to close.
pub fn race(streams streams: List(Stream(a))) -> Stream(a) {
  case streams {
    [] -> source.empty()
    _ -> race_with(streams)
  }
}

@target(erlang)
fn race_with(streams: List(Stream(a))) -> Stream(a) {
  let result_subject = process.new_subject()
  list.index_map(streams, fn(stream, idx) {
    let subj = result_subject
    let _pid =
      process.spawn_unlinked(fn() { race_first_pull(stream, idx, subj) })
    Nil
  })
  source.unfold(from: RaceWaiting(streams, result_subject), with: race_step)
}

@target(erlang)
fn race_first_pull(
  stream: Stream(a),
  index: Int,
  subj: Subject(RaceMsg(a)),
) -> Nil {
  case datastream.pull(stream) {
    Next(element, rest) ->
      process.send(subj, RaceNext(index: index, element: element, rest: rest))
    Done -> process.send(subj, RaceDone(index: index))
  }
}

@target(erlang)
type RaceState(a) {
  RaceWaiting(streams: List(Stream(a)), subj: Subject(RaceMsg(a)))
  RaceWinning(stream: Stream(a))
  RaceFinished
}

@target(erlang)
fn race_step(state: RaceState(a)) -> datastream.Step(a, RaceState(a)) {
  case state {
    RaceFinished -> Done
    RaceWinning(stream) ->
      case datastream.pull(stream) {
        Next(element, rest) -> Next(element, RaceWinning(rest))
        Done -> Done
      }
    RaceWaiting(streams, subj) ->
      case process.receive_forever(from: subj) {
        RaceNext(index, element, rest) -> {
          close_losers(streams, index)
          Next(element, RaceWinning(rest))
        }
        RaceDone(_index) -> race_step(RaceWaiting(streams, subj))
      }
  }
}

@target(erlang)
fn close_losers(streams: List(Stream(a)), winner_index: Int) -> Nil {
  list.index_map(streams, fn(stream, idx) {
    case idx == winner_index {
      True -> Nil
      False -> datastream.close(stream)
    }
  })
  Nil
}
