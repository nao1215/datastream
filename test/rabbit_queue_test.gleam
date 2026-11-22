import datastream
import gleam/list

pub fn last_of_many_waiting_consumers_is_stack_safe_test() {
  let consumers = datastream.once(7) |> datastream.broadcast(into: 50_000)
  let assert Ok(last) = list.last(consumers)
  assert datastream.to_list(last) == [7]
}

pub fn last_of_many_queued_consumers_keeps_values_in_order_test() {
  let consumers =
    datastream.from_list([7, 8]) |> datastream.broadcast(into: 50_000)
  let assert [first, ..] = consumers
  let assert Ok(last) = list.last(consumers)
  assert datastream.to_list(first) == [7, 8]
  assert datastream.to_list(last) == [7, 8]
}

pub fn closed_last_consumer_does_not_restart_at_large_index_test() {
  let consumers = datastream.once(7) |> datastream.broadcast(into: 50_000)
  let assert Ok(last) = list.last(consumers)
  last |> datastream.take(up_to: 0) |> datastream.drain
  assert datastream.to_list(last) == []
}
