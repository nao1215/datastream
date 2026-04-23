//// Internal helpers for BEAM extension worker pumps.
////
//// A pump is a process that pulls from an upstream `Stream(a)` and
//// forwards each element to a result subject. Combinators in
//// `datastream/erlang/par` and `datastream/erlang/time` use this to
//// drive an upstream concurrently with their own coordinator loop.
////
//// Each pump is paired with a stop subject. Sending `Stop` to that
//// subject causes the worker to close the upstream and exit at the
//// start of its next iteration. A worker blocked inside
//// `datastream.pull` only stops after that pull returns; pulls that
//// never return leave the worker resident.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only.
pub const beam_only_marker: String = "datastream/erlang/internal/pump is BEAM-only"

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import gleam/erlang/process.{type Subject}

@target(erlang)
@internal
pub type Pump(a) {
  PumpElement(a)
  PumpDone
}

@target(erlang)
@internal
pub type Stop {
  Stop
}

@target(erlang)
/// Spawn a worker that owns its own stop subject and runs `body`.
/// Returns the worker's stop subject so the caller can later signal
/// it to stop.
///
/// The subject is created inside the worker because Erlang subjects
/// can only be received from by the owning process. A short
/// handshake hands it back to the caller before `body` runs.
@internal
pub fn spawn_with_stop(body: fn(Subject(Stop)) -> Nil) -> Subject(Stop) {
  let handshake = process.new_subject()
  let _pid =
    process.spawn_unlinked(fn() {
      let stop_subj = process.new_subject()
      process.send(handshake, stop_subj)
      body(stop_subj)
    })
  process.receive_forever(from: handshake)
}

@target(erlang)
/// Spawn a worker that pumps `stream` into `result_subj` and exits on
/// `Stop`. Returns the worker's stop subject.
@internal
pub fn spawn_pump(
  over stream: Stream(a),
  into result_subj: Subject(Pump(a)),
) -> Subject(Stop) {
  spawn_with_stop(fn(stop_subj) { pump_loop(stream, result_subj, stop_subj) })
}

@target(erlang)
fn pump_loop(
  stream: Stream(a),
  result_subj: Subject(Pump(a)),
  stop_subj: Subject(Stop),
) -> Nil {
  case process.receive(from: stop_subj, within: 0) {
    Ok(_) -> datastream.close(stream)
    Error(_) ->
      case datastream.pull(stream) {
        Next(element, rest) -> {
          process.send(result_subj, PumpElement(element))
          pump_loop(rest, result_subj, stop_subj)
        }
        Done -> process.send(result_subj, PumpDone)
      }
  }
}

@target(erlang)
/// Signal a pump worker to stop. The worker observes the signal at
/// the start of its next iteration and then closes the upstream.
@internal
pub fn stop(stop_subj: Subject(Stop)) -> Nil {
  process.send(stop_subj, Stop)
}
