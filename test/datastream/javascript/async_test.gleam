//// JavaScript-only tests for `datastream/javascript/async`.

@target(javascript)
import datastream.{Done, Next}

@target(erlang)
/// Empty placeholder so this module compiles cleanly on Erlang.
pub const javascript_only_marker: String =
  "datastream/javascript/async_test is JavaScript-only"

@target(javascript)
import datastream/internal/ref.{type Ref}

@target(javascript)
@external(javascript, "./async_test_ffi.mjs", "assert_async_iterable_equals")
fn assert_async_iterable_equals(
  iterable: datastream.AsyncIterable(Int),
  expected: List(Int),
) -> Nil

@target(javascript)
@external(javascript, "./async_test_ffi.mjs", "assert_exhaustion_closes_once")
fn assert_exhaustion_closes_once(
  iterable: datastream.AsyncIterable(Int),
  close_count: fn() -> Int,
) -> Nil

@target(javascript)
@external(javascript, "./async_test_ffi.mjs", "assert_return_closes_once")
fn assert_return_closes_once(
  iterable: datastream.AsyncIterable(Int),
  close_count: fn() -> Int,
) -> Nil

@target(javascript)
pub fn to_async_iterable_preserves_element_order_test() {
  datastream.from_list([1, 2, 3, 4])
  |> datastream.map(with: fn(x) { x * 10 })
  |> datastream.to_async_iterable
  |> assert_async_iterable_equals([10, 20, 30, 40])
}

@target(javascript)
pub fn to_async_iterable_closes_resource_on_exhaustion_test() {
  let close_count = ref.new(0)

  datastream.resource(open: fn() { 0 }, next: finite_counter, close: fn(_) {
    bump(close_count)
  })
  |> datastream.to_async_iterable
  |> assert_exhaustion_closes_once(fn() { read(close_count) })
}

@target(javascript)
pub fn to_async_iterable_closes_resource_on_early_return_test() {
  let close_count = ref.new(0)

  datastream.resource(
    open: fn() { 0 },
    next: fn(state) { Next(element: state, state: state + 1) },
    close: fn(_) { bump(close_count) },
  )
  |> datastream.to_async_iterable
  |> assert_return_closes_once(fn() { read(close_count) })
}

@target(javascript)
fn finite_counter(state: Int) -> datastream.Step(Int, Int) {
  case state < 3 {
    True -> Next(element: state, state: state + 1)
    False -> Done
  }
}

@target(javascript)
fn bump(counter: Ref(Int)) -> Nil {
  ref.set(counter, read(counter) + 1)
}

@target(javascript)
fn read(cell: ref.Ref(a)) -> a {
  case ref.get(cell) {
    Ok(value) -> value
    Error(_) -> panic as "test ref was deleted"
  }
}
