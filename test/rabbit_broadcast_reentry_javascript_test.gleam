@target(javascript)
import datastream.{type AsyncIterable, type BroadcastError, Done, Next}
@target(javascript)
import gleam/int

@target(erlang)
pub const javascript_only_marker = "JavaScript-only"

@target(javascript)
@external(javascript, "./rabbit_broadcast_reentry_javascript_test_ffi.mjs", "assert_active_queued_pop")
fn assert_active_queued_pop(
  factory: fn(fn(Int) -> Nil, fn(String) -> Nil) ->
    #(
      AsyncIterable(Result(Int, BroadcastError)),
      AsyncIterable(Result(Int, BroadcastError)),
    ),
  read_value: fn(Result(Int, BroadcastError)) -> Int,
) -> Nil

@target(javascript)
pub fn queued_pop_during_pull_preserves_activity_and_frees_queue_space_test() {
  assert_active_queued_pop(
    fn(on_next, record) {
      let stream =
        datastream.resource(
          open: fn() {
            record("open")
            0
          },
          next: fn(index) {
            record("pull:" <> int.to_string(index))
            on_next(index)
            case index < 3 {
              True -> Next(index, index + 1)
              False -> Done
            }
          },
          close: fn(index) { record("close:" <> int.to_string(index)) },
        )
      let assert Ok([first, second]) =
        datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
      #(
        datastream.to_async_iterable(first),
        datastream.to_async_iterable(second),
      )
    },
    fn(value) {
      let assert Ok(value) = value
      value
    },
  )
}
