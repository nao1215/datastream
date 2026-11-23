@target(javascript)
import datastream.{Next}

@target(javascript)
import datastream/internal/ref

@target(javascript)
import gleam/int

@target(javascript)
import gleam/list

@target(erlang)
pub const javascript_only_marker: String =
  "datastream_javascript_redesign_test is JavaScript-only"

@target(javascript)
@external(javascript, "./datastream_javascript_redesign_test_ffi.mjs", "assert_break")
fn assert_break(
  iterable: datastream.AsyncIterable(Int),
  history: fn() -> List(String),
) -> Nil

@target(javascript)
@external(javascript, "./datastream_javascript_redesign_test_ffi.mjs", "assert_throw")
fn assert_throw(
  iterable: datastream.AsyncIterable(Int),
  history: fn() -> List(String),
) -> Nil

@target(javascript)
@external(javascript, "./datastream_javascript_redesign_test_ffi.mjs", "assert_loop_throw")
fn assert_loop_throw(
  iterable: datastream.AsyncIterable(Int),
  history: fn() -> List(String),
) -> Nil

@target(javascript)
@external(javascript, "./datastream_javascript_redesign_test_ffi.mjs", "assert_unstarted_return")
fn assert_unstarted_return(
  iterable: datastream.AsyncIterable(Int),
  history: fn() -> List(String),
) -> Nil

@target(javascript)
@external(javascript, "./datastream_javascript_redesign_test_ffi.mjs", "assert_callback_raise")
fn assert_callback_raise(
  iterable: datastream.AsyncIterable(Int),
  history: fn() -> List(String),
  at: Int,
  expected: List(String),
) -> Nil

@target(javascript)
@external(javascript, "./datastream_javascript_redesign_test_ffi.mjs", "assert_close_error")
fn assert_close_error(make: fn() -> datastream.AsyncIterable(Int)) -> Nil

@target(javascript)
fn record(events: ref.Ref(List(String)), event: String) -> Nil {
  let assert Ok(previous) = ref.get(events)
  ref.set(events, [event, ..previous])
}

@target(javascript)
fn observed_iterable() -> #(datastream.AsyncIterable(Int), fn() -> List(String)) {
  let #(stream, history) = observed_stream()
  #(datastream.to_async_iterable(stream), history)
}

@target(javascript)
fn observed_stream() -> #(datastream.Stream(Int), fn() -> List(String)) {
  let events = ref.new([])
  let stream =
    datastream.resource(
      open: fn() {
        record(events, "open")
        0
      },
      next: fn(state) {
        record(events, "pull:" <> int.to_string(state))
        Next(state, state + 1)
      },
      close: fn(state) { record(events, "close:" <> int.to_string(state)) },
    )
  #(stream, fn() {
    let assert Ok(reversed) = ref.get(events)
    list.reverse(reversed)
  })
}

@target(javascript)
pub fn for_await_break_closes_the_latest_state_once_test() {
  let #(iterable, history) = observed_iterable()
  assert_break(iterable, history)
}

@target(javascript)
pub fn iterator_throw_closes_the_latest_state_once_test() {
  let #(iterable, history) = observed_iterable()
  assert_throw(iterable, history)
}

@target(javascript)
pub fn for_await_body_throw_closes_the_latest_state_once_test() {
  let #(iterable, history) = observed_iterable()
  assert_loop_throw(iterable, history)
}

@target(javascript)
pub fn return_before_first_next_leaves_the_resource_unopened_test() {
  let #(iterable, history) = observed_iterable()
  assert_unstarted_return(iterable, history)
}

@target(javascript)
fn raising_iterable(
  at: Int,
) -> #(datastream.AsyncIterable(Int), fn() -> List(String)) {
  let #(stream, history) = observed_stream()
  let iterable =
    stream
    |> datastream.map(fn(value) {
      case value == at {
        True -> panic as "callback"
        False -> value
      }
    })
    |> datastream.to_async_iterable
  #(iterable, history)
}

@target(javascript)
pub fn callback_raise_closes_the_latest_state_once_test() {
  let #(iterable, history) = raising_iterable(2)
  assert_callback_raise(iterable, history, 2, [
    "open", "pull:0", "pull:1", "pull:2", "close:3",
  ])
}

@target(javascript)
pub fn callback_raise_on_first_pull_closes_the_resource_test() {
  let #(iterable, history) = raising_iterable(0)
  assert_callback_raise(iterable, history, 0, ["open", "pull:0", "close:1"])
}

@target(javascript)
pub fn close_error_rejects_instead_of_throwing_test() {
  assert_close_error(fn() {
    datastream.resource(
      open: fn() { 0 },
      next: fn(n) { Next(n, n + 1) },
      close: fn(_) { panic as "close failed" },
    )
    |> datastream.to_async_iterable
  })
}
