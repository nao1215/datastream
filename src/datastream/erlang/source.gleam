//// BEAM-only source constructors.
////
//// These bridge between Erlang's process / timer primitives and the
//// cross-target `Stream(a)` value. The cross-target core has no
//// business knowing about `Subject`, monotonic time, or BEAM timers,
//// so they live here in `datastream/erlang/source` and never compile
//// on the JavaScript target.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only. Every
/// function below is gated on `@target(erlang)`; this constant exists
/// so the module is non-empty under the JavaScript target and the
/// compiler does not warn about an empty module.
pub const beam_only_marker: String = "datastream/erlang/source is BEAM-only"

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import datastream/source

@target(erlang)
import gleam/erlang/atom.{type Atom}

@target(erlang)
import gleam/erlang/process.{type Subject}

@target(erlang)
/// Bridge a process `Subject(a)` into a `Stream(a)`.
///
/// Each pull blocks on `process.receive` until either a message
/// arrives — emitted as the next element — or `keep_running()`
/// returns `False` between receives. The receive uses a short
/// internal poll interval so `keep_running` stays responsive.
///
/// The `Subject`'s lifecycle is owned by the caller: this stream
/// only reads, it never closes the subject. The terminal completing
/// is sufficient to release the stream's hold.
pub fn from_subject(
  from subject: Subject(a),
  while keep_running: fn() -> Bool,
) -> Stream(a) {
  source.unfold(from: Nil, with: fn(_) { receive_loop(subject, keep_running) })
}

@target(erlang)
const subject_poll_ms: Int = 100

@target(erlang)
fn receive_loop(
  subject: Subject(a),
  keep_running: fn() -> Bool,
) -> datastream.Step(a, Nil) {
  case keep_running() {
    False -> Done
    True ->
      case process.receive(from: subject, within: subject_poll_ms) {
        Ok(message) -> Next(element: message, state: Nil)
        Error(_) -> receive_loop(subject, keep_running)
      }
  }
}

@target(erlang)
/// Emit a monotonic millisecond timestamp every `period_ms` ms.
///
/// The first element is emitted `period_ms` after the first pull.
/// Pair with `stream.take` (or any early-exit terminal) to bound
/// the run.
pub fn ticks(every period_ms: Int) -> Stream(Int) {
  source.unfold(from: Nil, with: fn(_) {
    process.sleep(period_ms)
    Next(element: monotonic_millis(), state: Nil)
  })
}

@target(erlang)
/// Emit `Nil` every `period_ms` ms. Same first-emission timing as
/// `ticks`; differs only in element type.
pub fn interval(every period_ms: Int) -> Stream(Nil) {
  source.unfold(from: Nil, with: fn(_) {
    process.sleep(period_ms)
    Next(element: Nil, state: Nil)
  })
}

@target(erlang)
/// Wrap an upstream and add a per-element deadline.
///
/// Each element of `stream` that arrives within `ms` ms surfaces as
/// `Ok(element)`. If the upstream produces nothing for longer than
/// `ms` ms, exactly one `Error(Nil)` element is emitted and the
/// stream halts.
///
/// Implementation: each pull spawns an unlinked worker process that
/// performs the `datastream.pull` and forwards the step on a
/// dedicated subject; the parent waits with `process.receive(_,
/// within: ms)`. On a deadline trip the abandoned worker may still
/// complete the `pull` afterwards; its result is silently discarded
/// and any resource it had partially acquired is left to the BEAM
/// runtime to reclaim.
///
/// Downstream early-exit closes the upstream the next pull would
/// have read from; an abandoned worker is not waited on.
///
/// **Limitation:** because the pull happens in a different process
/// than the one that built the upstream, this combinator MUST NOT
/// be composed with `from_subject`: a `Subject` can only be received
/// by its owning process, so the worker would `panic`. Wrap subject
/// streams with their own application-level timeout instead.
pub fn timeout(over stream: Stream(a), within ms: Int) -> Stream(Result(a, Nil)) {
  build_timeout_stream(TimeoutActive(stream), ms)
}

@target(erlang)
type TimeoutState(a) {
  TimeoutActive(stream: Stream(a))
  TimeoutDrained
}

@target(erlang)
type TimeoutPull(a) {
  TimeoutNext(element: a, rest: Stream(a))
  TimeoutDone
}

@target(erlang)
fn build_timeout_stream(
  state: TimeoutState(a),
  ms: Int,
) -> Stream(Result(a, Nil)) {
  datastream.make(pull: fn() { timeout_pull(state, ms) }, close: fn() {
    timeout_close(state)
  })
}

@target(erlang)
fn timeout_close(state: TimeoutState(a)) -> Nil {
  case state {
    TimeoutDrained -> Nil
    TimeoutActive(stream) -> datastream.close(stream)
  }
}

@target(erlang)
fn timeout_pull(
  state: TimeoutState(a),
  ms: Int,
) -> datastream.Step(Result(a, Nil), Stream(Result(a, Nil))) {
  case state {
    TimeoutDrained -> Done
    TimeoutActive(stream) -> {
      let result = pull_with_deadline(stream, ms)
      case result {
        Ok(TimeoutNext(element, rest)) ->
          Next(Ok(element), build_timeout_stream(TimeoutActive(rest), ms))
        Ok(TimeoutDone) -> Done
        Error(_) -> Next(Error(Nil), build_timeout_stream(TimeoutDrained, ms))
      }
    }
  }
}

@target(erlang)
fn pull_with_deadline(stream: Stream(a), ms: Int) -> Result(TimeoutPull(a), Nil) {
  let result_subject = process.new_subject()
  let _pid =
    process.spawn_unlinked(fn() {
      case datastream.pull(stream) {
        Next(element, rest) ->
          process.send(result_subject, TimeoutNext(element, rest))
        Done -> process.send(result_subject, TimeoutDone)
      }
    })
  process.receive(from: result_subject, within: ms)
}

@target(erlang)
@external(erlang, "erlang", "monotonic_time")
fn erlang_monotonic_time(unit: Atom) -> Int

@target(erlang)
fn monotonic_millis() -> Int {
  erlang_monotonic_time(atom.create("millisecond"))
}
