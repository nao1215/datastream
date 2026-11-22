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
fn record(events: ref.Ref(List(String)), event: String) -> Nil {
  let assert Ok(previous) = ref.get(events)
  ref.set(events, [event, ..previous])
}

@target(javascript)
fn observed_iterable() -> #(datastream.AsyncIterable(Int), fn() -> List(String)) {
  let events = ref.new([])
  let iterable =
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
    |> datastream.to_async_iterable
  #(iterable, fn() {
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
