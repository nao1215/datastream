//// BEAM-only sink for delivering a stream's elements onto a `Subject(a)`.
////
//// This is the dual of `datastream/erlang/source.from_subject`:
//// together they let two parts of a system talk over a process-based
//// channel without either side having to know the other's stream
//// representation.
////
//// Every public function in this module is gated on
//// `@target(erlang)`. On the JavaScript target the module still
//// compiles, so `import datastream/erlang/sink` itself does not
//// fail, but calling any function fails at the call site. The
//// `beam_only_marker` constant exists solely to keep the module
//// non-empty on the JavaScript target.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only. Every
/// function below is gated on `@target(erlang)`; this constant exists
/// so the module is non-empty under the JavaScript target and the
/// compiler does not warn about an empty module.
pub const beam_only_marker: String = "datastream/erlang/sink is BEAM-only"

@target(erlang)
import datastream.{type Stream}

@target(erlang)
import datastream/sink

@target(erlang)
import gleam/erlang/process.{type Subject}

@target(erlang)
/// Drive `stream` to completion, sending each element to `subject`
/// in source order. Returns `Nil`.
///
/// The function does not own the subject: it never closes or otherwise
/// modifies it. Lifecycle (creation, naming, hand-off) stays with the
/// caller, identical to `from_subject` on the source side.
pub fn into_subject(over stream: Stream(a), into subject: Subject(a)) -> Nil {
  sink.each(over: stream, with: fn(element) { process.send(subject, element) })
}
