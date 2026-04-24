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

/// A lazy, pull-based pipeline that yields elements of type `a`.
///
/// Values of this type are pipeline *definitions*, not materialised
/// collections. Each terminal operation (in `fold` / `sink`) re-runs the
/// pipeline from its source — there is no implicit caching. Callers that
/// want replay should materialise once with `fold.to_list`.
///
/// The internal representation is intentionally hidden so the library is
/// free to change it (list-backed, function-batched, chunk-batched, …)
/// without a breaking change. `==` / `!=` carry no specified meaning;
/// observational equality is via terminal reduction.
///
/// The library threads a cleanup callback through every stream value so
/// resource-safe sources from `source.resource` can be released on every
/// termination path (normal end, downstream early-exit, early-exit
/// folds, `try_each` failure). Pure (`unfold`-built) streams use a
/// no-op cleanup.
pub opaque type Stream(a) {
  Stream(pull: fn() -> Step(a, Stream(a)), close: fn() -> Nil)
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
/// The constructed stream has a no-op `close`; resource-safe streams
/// are built via `resource` instead.
///
/// This constructor is internal to the library — it is the substrate the
/// `source`, `stream`, `fold`, and `sink` modules build on. It is not
/// part of the public API.
@internal
pub fn unfold(from state: s, with step: fn(s) -> Step(a, s)) -> Stream(a) {
  Stream(
    pull: fn() {
      case step(state) {
        Next(element, next_state) ->
          Next(element, unfold(from: next_state, with: step))
        Done -> Done
      }
    },
    close: noop,
  )
}

/// Build a stream from explicit `pull` and `close` callbacks.
///
/// Used by combinators that need to forward the upstream's `close` to
/// downstream consumers, and by `resource` to expose a real-world
/// handle as a stream. The `close` callback is called by terminals or
/// combinators that interrupt evaluation before the source returns
/// `Done`; combinators that observe a `Done` already know the upstream
/// has closed itself, so they MUST NOT call `close` again on that path.
///
/// Internal to the library.
@internal
pub fn make(
  pull pull: fn() -> Step(a, Stream(a)),
  close close: fn() -> Nil,
) -> Stream(a) {
  Stream(pull: pull, close: close)
}

/// Build a resource-backed stream.
///
/// `open` runs lazily on the first pull (NOT at construction), keeping
/// stream construction side-effect-free. `next` is called once per
/// pull; when it returns `Done`, `close` is invoked on the most recent
/// state and the stream signals `Done`. When the stream is interrupted
/// before `next` returns `Done` — for example by a `take` early-exit,
/// a `first` terminal, or a `try_each` `Error` — the surrounding
/// combinator / terminal calls the stream's `close` field, which closes
/// the most recent state.
///
/// Each terminal call re-opens: a `Stream(a)` value is a pipeline
/// definition, not a one-shot iterator.
///
/// Internal to the library — exposed publicly via `source.resource`.
@internal
pub fn resource(
  open open: fn() -> s,
  next next: fn(s) -> Step(a, s),
  close close: fn(s) -> Nil,
) -> Stream(a) {
  Stream(
    pull: fn() {
      let initial = open()
      pull_resource(initial, next, close)
    },
    close: noop,
  )
}

/// Variant of `resource` whose `open` may fail. On `Error(e)` the
/// stream emits exactly one element produced by `on_open_error(e)`
/// and halts; `close` is NOT called (no opened state exists). On
/// `Ok(state)` behaviour follows `resource`.
///
/// Internal to the library — exposed publicly via
/// `source.try_resource`.
@internal
pub fn try_resource(
  open open: fn() -> Result(s, eo),
  next next: fn(s) -> Step(a, s),
  close close: fn(s) -> Nil,
  on_open_error on_open_error: fn(eo) -> a,
) -> Stream(a) {
  Stream(
    pull: fn() {
      case open() {
        Ok(state) -> pull_resource(state, next, close)
        Error(e) ->
          Next(on_open_error(e), Stream(pull: fn() { Done }, close: noop))
      }
    },
    close: noop,
  )
}

fn pull_resource(
  state: s,
  next: fn(s) -> Step(a, s),
  close: fn(s) -> Nil,
) -> Step(a, Stream(a)) {
  case next(state) {
    Done -> {
      close(state)
      Done
    }
    Next(element, next_state) ->
      Next(
        element,
        Stream(
          pull: fn() { pull_resource(next_state, next, close) },
          close: fn() { close(next_state) },
        ),
      )
  }
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

/// Close a stream's underlying resource, if any.
///
/// Called by combinators / terminals on the *interruption* paths
/// (early-exit, fail-fast). Streams that have already observed `Done`
/// are closed by the source itself, so `close` MUST NOT be called by
/// callers on that path.
///
/// For pure (`unfold`-built) streams `close` is a no-op.
///
/// Internal to the library.
@internal
pub fn close(stream: Stream(a)) -> Nil {
  stream.close()
}

fn noop() -> Nil {
  Nil
}
