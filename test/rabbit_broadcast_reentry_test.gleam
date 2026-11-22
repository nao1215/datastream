import datastream.{type Stream, Done, Next, QueueLimitExceeded}
import datastream/internal/ref.{type Ref}
import gleam/list
import gleeunit/should

type Event {
  Opened
  Pulled(Int)
  CloseStarted(Int)
  CloseFinished(Int)
  Notified(Int)
}

fn get(reference: Ref(a)) -> a {
  let assert Ok(value) = ref.get(reference)
  value
}

fn record(events: Ref(List(Event)), event: Event) {
  ref.set(events, [event, ..get(events)])
}

fn consumer(consumers: Ref(List(Stream(a))), index: Int) -> Stream(a) {
  let assert Ok(stream) = get(consumers) |> list.drop(index) |> list.first
  stream
}

fn cancel(stream: Stream(a)) {
  stream |> datastream.take(up_to: 0) |> datastream.drain
}

fn source(
  events: Ref(List(Event)),
  on_next: fn(Int) -> Nil,
  on_close: fn(Int) -> Nil,
) -> Stream(Int) {
  source_with_open(events, fn() { Nil }, on_next, on_close)
}

fn source_with_open(
  events: Ref(List(Event)),
  on_open: fn() -> Nil,
  on_next: fn(Int) -> Nil,
  on_close: fn(Int) -> Nil,
) -> Stream(Int) {
  datastream.resource(
    open: fn() {
      record(events, Opened)
      on_open()
      0
    },
    next: fn(index) {
      record(events, Pulled(index))
      on_next(index)
      case index < 3 {
        True -> Next(index, index + 1)
        False -> Done
      }
    },
    close: fn(index) {
      record(events, CloseStarted(index))
      on_close(index)
      record(events, CloseFinished(index))
    },
  )
}

fn closed_states(events: Ref(List(Event))) -> List(Int) {
  get(events)
  |> list.reverse
  |> list.filter_map(fn(event) {
    case event {
      CloseFinished(index) -> Ok(index)
      _ -> Error(Nil)
    }
  })
}

pub fn next_callback_cancellation_does_not_resurrect_a_sibling_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(
      events,
      fn(index) {
        case index {
          0 -> cancel(consumer(consumers, 1))
          _ -> Nil
        }
      },
      fn(_) { Nil },
    )
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0)
  |> datastream.to_list
  |> should.equal([Ok(0), Ok(1), Ok(2)])
  consumer(consumers, 1) |> datastream.to_list |> should.equal([])
  get(events)
  |> list.reverse
  |> should.equal([
    Opened,
    Pulled(0),
    Pulled(1),
    Pulled(2),
    Pulled(3),
    CloseStarted(3),
    CloseFinished(3),
  ])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn cancelling_all_during_next_closes_the_returned_continuation_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(events, fn(_) { list.each(get(consumers), cancel) }, fn(_) { Nil })
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0) |> datastream.first |> should.equal(Error(Nil))
  list.each(streams, fn(stream) {
    stream |> datastream.to_list |> should.equal([])
  })
  get(events)
  |> list.reverse
  |> should.equal([Opened, Pulled(0), CloseStarted(1), CloseFinished(1)])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn cancelling_current_during_next_preserves_in_flight_value_for_sibling_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(events, fn(_) { cancel(consumer(consumers, 0)) }, fn(_) { Nil })
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0) |> datastream.first |> should.equal(Error(Nil))
  consumer(consumers, 1) |> datastream.first |> should.equal(Ok(Ok(0)))
  closed_states(events) |> should.equal([1])
  consumer(consumers, 0) |> datastream.to_list |> should.equal([])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn nested_empty_read_cancels_without_repulling_the_source_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let entered = ref.new(False)
  let nested = ref.new(Ok(Ok(-1)))
  let stream =
    source(
      events,
      fn(_) {
        case get(entered) {
          True -> Nil
          False -> {
            ref.set(entered, True)
            ref.set(nested, datastream.first(consumer(consumers, 1)))
          }
        }
      },
      fn(_) { Nil },
    )
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0)
  |> datastream.to_list
  |> should.equal([Ok(0), Ok(1), Ok(2)])
  get(nested) |> should.equal(Error(Nil))
  consumer(consumers, 1) |> datastream.to_list |> should.equal([])
  get(events)
  |> list.reverse
  |> should.equal([
    Opened,
    Pulled(0),
    Pulled(1),
    Pulled(2),
    Pulled(3),
    CloseStarted(3),
    CloseFinished(3),
  ])
  ref.delete(consumers)
  ref.delete(events)
  ref.delete(entered)
  ref.delete(nested)
}

pub fn nested_read_can_consume_a_queued_value_and_cancel_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let nested = ref.new(Error(Nil))
  let stream =
    source(
      events,
      fn(index) {
        case index {
          1 -> ref.set(nested, datastream.first(consumer(consumers, 1)))
          _ -> Nil
        }
      },
      fn(_) { Nil },
    )
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0)
  |> datastream.to_list
  |> should.equal([Ok(0), Ok(1), Ok(2)])
  get(nested) |> should.equal(Ok(Ok(0)))
  consumer(consumers, 1) |> datastream.to_list |> should.equal([])
  closed_states(events) |> should.equal([3])
  ref.delete(consumers)
  ref.delete(events)
  ref.delete(nested)
}

pub fn overflow_is_observable_only_after_cleanup_returns_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let nested = ref.new(Ok(Ok(-1)))
  let stream =
    source(events, fn(_) { Nil }, fn(_) {
      ref.set(nested, datastream.first(consumer(consumers, 1)))
    })
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 3, max_queue: 1)
  ref.set(consumers, streams)
  let failure = Error(QueueLimitExceeded(consumer: 1, max_queue: 1))
  consumer(consumers, 0)
  |> datastream.tap(with: fn(value) {
    case value {
      Error(_) -> {
        closed_states(events) |> should.equal([2])
        record(events, Notified(0))
      }
      Ok(_) -> Nil
    }
  })
  |> datastream.to_list
  |> should.equal([Ok(0), failure])
  get(nested) |> should.equal(Error(Nil))
  consumer(consumers, 1) |> datastream.to_list |> should.equal([])
  consumer(consumers, 2)
  |> datastream.tap(with: fn(_) {
    closed_states(events) |> should.equal([2])
    record(events, Notified(2))
  })
  |> datastream.to_list
  |> should.equal([failure])
  get(events)
  |> list.reverse
  |> should.equal([
    Opened,
    Pulled(0),
    Pulled(1),
    CloseStarted(2),
    CloseFinished(2),
    Notified(0),
    Notified(2),
  ])
  ref.delete(consumers)
  ref.delete(events)
  ref.delete(nested)
}

pub fn overflow_cleanup_can_cancel_the_outer_puller_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(events, fn(_) { Nil }, fn(_) { cancel(consumer(consumers, 0)) })
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0) |> datastream.to_list |> should.equal([Ok(0)])
  consumer(consumers, 1)
  |> datastream.to_list
  |> should.equal([Error(QueueLimitExceeded(consumer: 1, max_queue: 1))])
  closed_states(events) |> should.equal([2])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn natural_completion_keeps_close_callback_cancellations_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(events, fn(_) { Nil }, fn(_) { list.each(get(consumers), cancel) })
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 3)
  ref.set(consumers, streams)
  consumer(consumers, 0)
  |> datastream.to_list
  |> should.equal([Ok(0), Ok(1), Ok(2)])
  consumer(consumers, 1) |> datastream.to_list |> should.equal([])
  closed_states(events) |> should.equal([3])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn cancelling_all_inside_open_closes_the_first_returned_state_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source_with_open(
      events,
      fn() { list.each(get(consumers), cancel) },
      fn(_) { Nil },
      fn(_) { Nil },
    )
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0) |> datastream.first |> should.equal(Error(Nil))
  list.each(streams, fn(stream) {
    stream |> datastream.to_list |> should.equal([])
  })
  get(events)
  |> list.reverse
  |> should.equal([Opened, Pulled(0), CloseStarted(1), CloseFinished(1)])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn nested_read_of_current_consumer_cancels_the_outer_read_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let entered = ref.new(False)
  let nested = ref.new(Ok(Ok(-1)))
  let stream =
    source(
      events,
      fn(_) {
        case get(entered) {
          True -> Nil
          False -> {
            ref.set(entered, True)
            ref.set(nested, datastream.first(consumer(consumers, 0)))
          }
        }
      },
      fn(_) { Nil },
    )
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0) |> datastream.first |> should.equal(Error(Nil))
  get(nested) |> should.equal(Error(Nil))
  consumer(consumers, 1) |> datastream.first |> should.equal(Ok(Ok(0)))
  list.each(streams, fn(stream) {
    stream |> datastream.to_list |> should.equal([])
  })
  get(events)
  |> list.reverse
  |> should.equal([Opened, Pulled(0), CloseStarted(1), CloseFinished(1)])
  ref.delete(consumers)
  ref.delete(events)
  ref.delete(entered)
  ref.delete(nested)
}

pub fn cancelling_all_inside_overflow_cleanup_suppresses_every_notification_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(events, fn(_) { Nil }, fn(_) { list.each(get(consumers), cancel) })
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 3, max_queue: 1)
  ref.set(consumers, streams)
  consumer(consumers, 0) |> datastream.to_list |> should.equal([Ok(0)])
  list.each(streams, fn(stream) {
    stream |> datastream.to_list |> should.equal([])
  })
  get(events)
  |> list.reverse
  |> should.equal([
    Opened,
    Pulled(0),
    Pulled(1),
    CloseStarted(2),
    CloseFinished(2),
  ])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn cancellation_inside_next_returning_done_keeps_natural_close_once_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(
      events,
      fn(index) {
        case index {
          3 -> list.each(get(consumers), cancel)
          _ -> Nil
        }
      },
      fn(_) { Nil },
    )
  let assert Ok(streams) =
    datastream.broadcast_bounded(stream, into: 2, max_queue: 3)
  ref.set(consumers, streams)
  consumer(consumers, 0)
  |> datastream.to_list
  |> should.equal([Ok(0), Ok(1), Ok(2)])
  list.each(streams, fn(stream) {
    stream |> datastream.to_list |> should.equal([])
  })
  get(events)
  |> list.reverse
  |> should.equal([
    Opened,
    Pulled(0),
    Pulled(1),
    Pulled(2),
    Pulled(3),
    CloseStarted(3),
    CloseFinished(3),
  ])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn unbounded_broadcast_preserves_sibling_cancellation_and_value_order_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(
      events,
      fn(index) {
        case index {
          0 -> cancel(consumer(consumers, 1))
          _ -> Nil
        }
      },
      fn(_) { Nil },
    )
  let streams = datastream.broadcast(stream, into: 2)
  ref.set(consumers, streams)
  consumer(consumers, 0) |> datastream.to_list |> should.equal([0, 1, 2])
  consumer(consumers, 1) |> datastream.to_list |> should.equal([])
  get(events)
  |> list.reverse
  |> should.equal([
    Opened,
    Pulled(0),
    Pulled(1),
    Pulled(2),
    Pulled(3),
    CloseStarted(3),
    CloseFinished(3),
  ])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn unbounded_nested_empty_read_permanently_cancels_without_reopening_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let entered = ref.new(False)
  let nested = ref.new(Ok(-1))
  let stream =
    source(
      events,
      fn(_) {
        case get(entered) {
          True -> Nil
          False -> {
            ref.set(entered, True)
            ref.set(nested, datastream.first(consumer(consumers, 1)))
          }
        }
      },
      fn(_) { Nil },
    )
  let streams = datastream.broadcast(stream, into: 2)
  ref.set(consumers, streams)
  consumer(consumers, 0) |> datastream.to_list |> should.equal([0, 1, 2])
  get(nested) |> should.equal(Error(Nil))
  consumer(consumers, 1) |> datastream.to_list |> should.equal([])
  get(events)
  |> list.reverse
  |> should.equal([
    Opened,
    Pulled(0),
    Pulled(1),
    Pulled(2),
    Pulled(3),
    CloseStarted(3),
    CloseFinished(3),
  ])
  ref.delete(consumers)
  ref.delete(events)
  ref.delete(entered)
  ref.delete(nested)
}

pub fn unzip_cancelling_both_inside_next_closes_latest_state_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(events, fn(_) { list.each(get(consumers), cancel) }, fn(_) { Nil })
  let #(left, right) =
    stream
    |> datastream.map(with: fn(value) { #(value, value + 10) })
    |> datastream.unzip
  ref.set(consumers, [left, right])
  left |> datastream.first |> should.equal(Error(Nil))
  left |> datastream.to_list |> should.equal([])
  right |> datastream.to_list |> should.equal([])
  get(events)
  |> list.reverse
  |> should.equal([Opened, Pulled(0), CloseStarted(1), CloseFinished(1)])
  ref.delete(consumers)
  ref.delete(events)
}

pub fn unzip_cancelling_current_preserves_the_other_component_test() {
  let consumers = ref.new([])
  let events = ref.new([])
  let stream =
    source(events, fn(_) { cancel(consumer(consumers, 0)) }, fn(_) { Nil })
  let #(left, right) =
    stream
    |> datastream.map(with: fn(value) { #(value, value + 10) })
    |> datastream.unzip
  ref.set(consumers, [left, right])
  left |> datastream.first |> should.equal(Error(Nil))
  right |> datastream.first |> should.equal(Ok(10))
  left |> datastream.to_list |> should.equal([])
  right |> datastream.to_list |> should.equal([])
  get(events)
  |> list.reverse
  |> should.equal([Opened, Pulled(0), CloseStarted(1), CloseFinished(1)])
  ref.delete(consumers)
  ref.delete(events)
}
