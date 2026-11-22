//// BEAM-only source constructors.
////
//// These bridge between Erlang's process / timer primitives and the
//// cross-target `Stream(a)` value. The cross-target core has no
//// business knowing about `Subject`, monotonic time, or BEAM timers,
//// so they live here in `datastream/erlang/source`.
////
//// Every public function in this module is gated on
//// `@target(erlang)`. On the JavaScript target the module still
//// compiles, so `import datastream/erlang/source` itself does not
//// fail, but calling any function fails at the call site. The
//// `beam_only_marker` constant exists solely to keep the module
//// non-empty on the JavaScript target.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only. Every
/// function below is gated on `@target(erlang)`; this constant exists
/// so the module is non-empty under the JavaScript target and the
/// compiler does not warn about an empty module.
pub const beam_only_marker: String = "datastream/erlang/source is BEAM-only"

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import datastream/fold
@target(erlang)
import datastream/source

@target(erlang)
import datastream/erlang/internal/pump.{type Pump, Element, Exhausted}

@target(erlang)
import datastream/internal/core

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
///
/// **Limitation:** the resulting stream cannot be passed directly
/// to any combinator in `datastream/erlang/par` or
/// `datastream/erlang/time`, nor to
/// `datastream/erlang/source.timeout` — those run the pull in a
/// worker process and the `Subject`'s owner-only receive constraint
/// causes a `panic` in the worker. Use `bridge_subject_stream`
/// below to materialise the subject side in the owning process
/// before handing it off.
pub fn from_subject(
  from subject: Subject(a),
  while keep_running: fn() -> Bool,
) -> Stream(a) {
  source.unfold(from: Nil, with: fn(_) { receive_loop(subject, keep_running) })
}

@target(erlang)
/// Materialise a `from_subject`-backed stream into a process-safe
/// `Stream(a)` so the result can be composed with `par.*` /
/// `time.*` / `source.timeout`.
///
/// The pull happens in the calling process — the same process that
/// owns the `Subject` — so the owner-only receive constraint is
/// respected. Once the upstream returns `Done`, the materialised
/// `List(a)` is reopened as a list-backed stream, which is pure and
/// can safely be pulled from any worker process.
///
/// **Caveat:** materialises the entire upstream into memory before
/// emitting any element. Only use this on bounded subjects (a
/// `keep_running` callback that eventually returns `False`, or a
/// finite source). For an unbounded subject, route the parallel
/// stage outside the subject pipeline instead — there is no
/// process-safe way to lazily forward an Erlang `Subject` across a
/// process boundary.
///
/// This is the official workaround for the
/// `from_subject` × `par.*` / `time.*` incompatibility documented
/// on `from_subject`, `par`, and `source.timeout`.
pub fn bridge_subject_stream(over stream: Stream(a)) -> Stream(a) {
  source.from_list(fold.to_list(stream))
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
    Next(element: pump.now_ms(), state: Nil)
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
/// Implementation: the first pull spawns one worker process that
/// pulls the upstream one element per request for the whole run, so an
/// upstream whose handle belongs to the process that opened it keeps
/// working. The caller waits for each element with
/// `process.receive(_, within: ms)`. On a deadline trip, or on
/// downstream early exit, the worker is told to stop and closes the
/// upstream once its pull in progress returns; the caller does not
/// wait for that.
///
/// **Limitation:** because the pull happens in a different process
/// than the one that built the upstream, this combinator MUST NOT
/// be composed with `from_subject`: a `Subject` can only be received
/// by its owning process, so the worker would `panic`. Wrap subject
/// streams with their own application-level timeout instead.
pub fn timeout(
  over stream: Stream(a),
  within ms: Int,
) -> Stream(Result(a, Nil)) {
  use upstream, _now <- pump.on_first_pull(
    stream,
    context: "datastream/erlang/source.timeout",
  )
  timeout_pull(upstream, ms)
}

@target(erlang)
fn timeout_active(upstream: Pump(a), ms: Int) -> Stream(Result(a, Nil)) {
  datastream.make(pull: fn() { timeout_pull(upstream, ms) }, close: fn() {
    pump.stop_pump(upstream)
  })
}

@target(erlang)
fn timeout_pull(
  upstream: Pump(a),
  ms: Int,
) -> datastream.Step(Result(a, Nil), Stream(Result(a, Nil))) {
  case pump.next(upstream, within: ms) {
    #(upstream, Ok(Element(element))) ->
      Next(Ok(element), timeout_active(upstream, ms))
    #(_, Ok(Exhausted)) -> Done
    #(upstream, Error(_)) -> {
      pump.stop_pump(upstream)
      Next(Error(Nil), core.empty())
    }
  }
}
