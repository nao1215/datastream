//// JavaScript-only async edge adapters.
////
//// The cross-target `Stream(a)` core is synchronous and pull-based:
//// each pull either yields the next element immediately or reports
//// `Done`. That contract keeps the core small and predictable across
//// Erlang and JavaScript, but it also means a true JavaScript
//// `AsyncIterable(a)` cannot be represented as a `Stream(a)` without
//// buffering or polling.
////
//// The official boundary is therefore:
////
//// - async input stays in host JavaScript until it has been reduced to
////   a bounded value or batch, then enters the core through the normal
////   constructors (`source.once`, `source.from_list`,
////   `source.from_bit_array`, `source.resource`, ...);
//// - synchronous `Stream(a)` pipelines leave the core through
////   `to_async_iterable`, which exposes them to `for await ... of`
////   and other host-side async consumers.
////
//// This keeps the core honest about its synchronous pull contract
//// while still giving JavaScript applications a first-party interop
//// point.

@target(erlang)
/// Sentinel value documenting that this module is JavaScript-only.
pub const javascript_only_marker: String = "datastream/javascript/async is JavaScript-only"

@target(javascript)
import datastream.{type Stream, Done, Next}

@target(javascript)
import datastream/internal/ref.{type Ref}

@target(javascript)
/// Host-side async iterable view of a synchronous `Stream(a)`.
///
/// Consume this value from JavaScript with `for await ... of`. The
/// returned object also implements the async-iterator protocol
/// directly, so host code can call `.next()` / `.return()` when it
/// needs explicit control.
pub type AsyncIterable(a)

@target(javascript)
type PullState(a) {
  Open(Stream(a))
  Closed
}

@target(javascript)
@external(javascript, "./async_ffi.mjs", "make_async_iterable")
fn make_async_iterable(
  next: fn() -> Result(a, Nil),
  stop: fn() -> Nil,
) -> AsyncIterable(a)

@target(javascript)
/// Expose `stream` as a JavaScript `AsyncIterable`.
///
/// This is the official async boundary for data leaving the pure core.
/// Each `next()` call on the host side performs one synchronous pull on
/// the underlying `Stream(a)`. If the host stops early (`break` from a
/// `for await` loop, `iterator.return()`, or an abrupt error), the
/// stream's `close` callback runs exactly once.
pub fn to_async_iterable(stream: Stream(a)) -> AsyncIterable(a) {
  let state = ref.new(Open(stream))
  make_async_iterable(fn() { pull_next(state) }, fn() { stop(state) })
}

@target(javascript)
fn pull_next(state: Ref(PullState(a))) -> Result(a, Nil) {
  case ref.get(state) {
    Closed -> Error(Nil)
    Open(stream) ->
      case datastream.pull(stream) {
        Done -> {
          ref.set(state, Closed)
          Error(Nil)
        }
        Next(element, rest) -> {
          ref.set(state, Open(rest))
          Ok(element)
        }
      }
  }
}

@target(javascript)
fn stop(state: Ref(PullState(a))) -> Nil {
  case ref.get(state) {
    Closed -> Nil
    Open(stream) -> {
      ref.set(state, Closed)
      datastream.close(stream)
    }
  }
}
