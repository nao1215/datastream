//// `datastream` is a compositional, resource-safe stream library for
//// Gleam. The cross-target core (`datastream/source`, `datastream/stream`,
//// `datastream/fold`, `datastream/sink`, `datastream/chunk`,
//// `datastream/text`, `datastream/binary`, `datastream/dataprep`) runs on
//// both the Erlang and JavaScript targets. The `datastream/erlang/*`
//// extension modules are quarantined to the Erlang target.
////
//// This module defines the foundational types every later module builds
//// on: the opaque pipeline value `Stream(a)` and the pull-result enum
//// `Step(a, state)`.
////
//// See `doc/reference/spec.md` for the full specification.

/// A lazy, pull-based pipeline that yields elements of type `a`.
///
/// Values of this type are pipeline *definitions*, not materialised
/// collections. Each terminal operation (in `fold` / `sink`) re-runs the
/// pipeline from its source — there is no implicit caching. Callers that
/// want replay should materialise once with `fold.to_list`.
///
/// The internal representation is intentionally hidden so the library is
/// free to change it (list-backed, function-backed, chunk-batched, …)
/// without a breaking change. `==` / `!=` carry no specified meaning;
/// observational equality is via terminal reduction.
pub opaque type Stream(a) {
  Stream(pull: fn() -> Step(a, Stream(a)))
}

/// The result of pulling one element from a stream-like producer.
///
/// `Next(element, state)` carries the produced element together with the
/// continuation needed to pull the next one. `Done` signals end-of-stream.
/// Once `Done` is observed the producer MUST NOT be pulled again.
///
/// There is intentionally no `Error` variant: the core does not impose a
/// failure model. Callers that need errors carry them as element-shaped
/// values (`Stream(Result(a, e))`, `Stream(Validated(a, e))`, …).
pub type Step(a, state) {
  Next(element: a, state: state)
  Done
}

/// Build a `Stream(a)` from an initial state and a step function.
///
/// `step` is invoked once per pull; the returned `Step` either yields
/// the next element together with the updated state, or signals `Done`.
/// The state type `s` is a private implementation detail of the
/// constructed stream and never appears in the public surface.
///
/// This constructor is internal to the library — it is the substrate the
/// `source`, `stream`, `fold`, and `sink` modules build on. It is not
/// part of the public API.
@internal
pub fn unfold(from state: s, with step: fn(s) -> Step(a, s)) -> Stream(a) {
  Stream(pull: fn() {
    case step(state) {
      Next(element, next_state) ->
        Next(element, unfold(from: next_state, with: step))
      Done -> Done
    }
  })
}

/// Pull one element from a stream.
///
/// Returns `Next(element, next_stream)` or `Done`. After `Done` is
/// observed the same stream MUST NOT be pulled again; doing so is a
/// programmer error and the result is unspecified.
///
/// Internal to the library: terminal operations live in `fold` / `sink`
/// and call this helper. It is not part of the public API.
@internal
pub fn pull(stream: Stream(a)) -> Step(a, Stream(a)) {
  stream.pull()
}
