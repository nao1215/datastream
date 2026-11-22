@target(javascript)
import datastream.{Done, Next}

@target(erlang)
pub const javascript_only_marker = "JavaScript-only"

@target(javascript)
@external(javascript, "./rabbit_async_reentry_test_ffi.mjs", "assert_reentrant_return")
fn assert_reentrant_return(
  factory: fn(fn(Int) -> Nil, fn(Int) -> Nil) -> datastream.AsyncIterable(Int),
  stop_at: Int,
  finishes: Bool,
) -> Nil

@target(javascript)
@external(javascript, "./rabbit_async_reentry_test_ffi.mjs", "assert_between_pulls")
fn assert_between_pulls(
  factory: fn(fn(Int) -> Nil, fn(Int) -> Nil) -> datastream.AsyncIterable(Int),
) -> Nil

@target(javascript)
@external(javascript, "./rabbit_async_reentry_test_ffi.mjs", "assert_nested_next")
fn assert_nested_next(
  factory: fn(fn(Int) -> Nil, fn(Int) -> Nil) -> datastream.AsyncIterable(Int),
) -> Nil

@target(javascript)
fn factory(stop_at: Int, finishes: Bool) {
  fn(on_next, on_close) {
    datastream.resource(
      open: fn() { 0 },
      next: fn(state) {
        on_next(state)
        case finishes && state == stop_at {
          True -> Done
          False -> Next(state, state + 1)
        }
      },
      close: on_close,
    )
    |> datastream.to_async_iterable
  }
}

@target(javascript)
pub fn return_inside_first_next_closes_new_continuation_test() {
  assert_reentrant_return(factory(0, False), 0, False)
}

@target(javascript)
pub fn return_inside_later_next_closes_latest_state_test() {
  assert_reentrant_return(factory(1, False), 1, False)
}

@target(javascript)
pub fn return_inside_first_done_does_not_close_twice_test() {
  assert_reentrant_return(factory(0, True), 0, True)
}

@target(javascript)
pub fn return_inside_later_done_does_not_close_twice_test() {
  assert_reentrant_return(factory(1, True), 1, True)
}

@target(javascript)
pub fn return_between_pulls_still_closes_immediately_test() {
  assert_between_pulls(factory(-1, False))
}

@target(javascript)
pub fn nested_next_rejects_without_repulling_or_stopping_outer_next_test() {
  assert_nested_next(factory(-1, False))
}
