import datastream.{type BinaryError, type Stream, Done, Next}
import datastream/internal/ref
import gleam/bit_array
import gleam/list
import gleam/result
import gleeunit/should

type Event {
  Opened
  Pulled(Int)
  Closed(Int)
}

type Output {
  Byte(Int)
  Frame(BitArray)
}

type BinaryCase {
  BinaryCase(
    name: String,
    run: fn(Stream(BitArray)) -> Stream(Result(Output, BinaryError)),
    complete: BitArray,
    expected: Output,
    pending: BitArray,
  )
}

fn recorded(values: List(a), events: ref.Ref(List(Event))) -> Stream(a) {
  datastream.resource(
    open: fn() {
      record(events, Opened)
      #(0, values)
    },
    next: fn(state) {
      let #(index, remaining) = state
      record(events, Pulled(index))
      case remaining {
        [] -> Done
        [value, ..rest] -> Next(value, #(index + 1, rest))
      }
    },
    close: fn(state) { record(events, Closed(state.0)) },
  )
}

fn record(events: ref.Ref(List(Event)), event: Event) -> Nil {
  let assert Ok(previous) = ref.get(events)
  ref.set(events, [event, ..previous])
}

fn history(events: ref.Ref(List(Event))) -> List(Event) {
  let assert Ok(reversed) = ref.get(events)
  list.reverse(reversed)
}

fn binary_cases() -> List(BinaryCase) {
  [
    BinaryCase(
      "bytes",
      fn(source) {
        source
        |> datastream.bytes
        |> datastream.map(with: fn(value) { result.map(value, Byte) })
      },
      <<7>>,
      Byte(7),
      <<>>,
    ),
    BinaryCase(
      "fixed_size",
      fn(source) {
        let assert Ok(frames) = datastream.fixed_size(source, size: 2)
        datastream.map(frames, with: fn(value) { result.map(value, Frame) })
      },
      <<7, 8>>,
      Frame(<<7, 8>>),
      <<7>>,
    ),
    BinaryCase(
      "delimited",
      fn(source) {
        let assert Ok(frames) = datastream.delimited(source, on: <<0>>)
        datastream.map(frames, with: fn(value) { result.map(value, Frame) })
      },
      <<7, 0>>,
      Frame(<<7>>),
      <<7>>,
    ),
    BinaryCase(
      "length_prefixed",
      fn(source) {
        let assert Ok(frames) =
          datastream.length_prefixed(source, prefix_size: 1, max_frame_size: 16)
        datastream.map(frames, with: fn(value) { result.map(value, Frame) })
      },
      <<1, 7>>,
      Frame(<<7>>),
      <<2, 7>>,
    ),
  ]
}

pub fn binary_alignment_errors_stop_at_the_offending_chunk_test() {
  use operation <- list.each(binary_cases())
  let full_bytes_with_tail = <<operation.complete:bits, 1:4>>
  let scenarios = [
    #("first", [<<1:4>>, operation.complete], [], 4, 1),
    #(
      "after_complete",
      [operation.complete, <<1:4>>, operation.complete],
      [Ok(operation.expected)],
      4,
      2,
    ),
    #(
      "whole_chunk_rejected",
      [full_bytes_with_tail, operation.complete],
      [],
      bit_array.bit_size(full_bytes_with_tail),
      1,
    ),
    #("separate_partial_chunks", [<<1:4>>, <<2:4>>], [], 4, 1),
    #(
      "pending_bytes_discarded",
      [operation.pending, <<1:4>>, operation.complete],
      [],
      4,
      2,
    ),
  ]
  use scenario <- list.each(scenarios)
  let #(name, chunks, prefix, bits, close_state) = scenario
  let events = ref.new([])
  let actual = recorded(chunks, events) |> operation.run |> datastream.to_list
  #(operation.name, name, actual)
  |> should.equal(#(
    operation.name,
    name,
    list.append(prefix, [Error(datastream.UnalignedChunk(bits))]),
  ))
  let pulls = case close_state {
    1 -> [Opened, Pulled(0), Closed(1)]
    _ -> [Opened, Pulled(0), Pulled(1), Closed(2)]
  }
  #(operation.name, name, history(events))
  |> should.equal(#(operation.name, name, pulls))
  ref.delete(events)
}

pub fn binary_alignment_error_continuations_are_safe_to_close_test() {
  use operation <- list.each(binary_cases())
  let events = ref.new([])
  recorded([operation.complete, <<1:4>>, operation.complete], events)
  |> operation.run
  |> datastream.collect_result
  |> should.equal(Error(datastream.UnalignedChunk(4)))
  #(operation.name, history(events))
  |> should.equal(#(operation.name, [Opened, Pulled(0), Pulled(1), Closed(2)]))
  ref.delete(events)
}

pub fn binary_size_validation_never_evaluates_the_source_test() {
  let constructors = [
    #("buffer", "prefetch", fn(source, size) {
      datastream.buffer(source, prefetch: size) |> result.map(fn(_) { Nil })
    }),
    #("chunks_of", "into", fn(source, size) {
      datastream.chunks_of(source, into: size) |> result.map(fn(_) { Nil })
    }),
    #("fixed_size", "size", fn(source, size) {
      datastream.fixed_size(source, size: size) |> result.map(fn(_) { Nil })
    }),
  ]
  use constructor <- list.each(constructors)
  let #(name, argument, construct) = constructor
  use size <- list.each([-1, 0, 1])
  let events = ref.new([])
  let expected = case size > 0 {
    True -> Ok(Nil)
    False -> Error(datastream.NotPositive(name, argument, size))
  }
  construct(recorded([<<7>>], events), size) |> should.equal(expected)
  history(events) |> should.equal([])
  ref.delete(events)
}

pub fn prefix_validation_and_frame_limit_validation_are_lazy_test() {
  use prefix <- list.each([-1, 0, 1, 2, 3, 4, 8, 16])
  use maximum <- list.each([-1, 0, 1])
  let events = ref.new([])
  let expected = case list.contains([1, 2, 4, 8], prefix), maximum < 0 {
    False, _ -> Error(datastream.InvalidPrefixSize(prefix))
    True, True -> Error(datastream.NegativeFrameLimit(maximum))
    True, False -> Ok(Nil)
  }
  recorded([<<0>>], events)
  |> datastream.length_prefixed(prefix_size: prefix, max_frame_size: maximum)
  |> result.map(fn(_) { Nil })
  |> should.equal(expected)
  history(events) |> should.equal([])
  ref.delete(events)
}

pub fn direct_bit_array_arguments_report_their_full_bit_size_test() {
  use bytes <- list.each([<<1:3>>, <<7, 1:3>>])
  let bits = bit_array.bit_size(bytes)
  datastream.from_bit_array(bytes)
  |> result.map(fn(_) { Nil })
  |> should.equal(Error(datastream.UnalignedArgument("bytes", bits)))
  let events = ref.new([])
  datastream.delimited(recorded([<<7>>], events), on: bytes)
  |> result.map(fn(_) { Nil })
  |> should.equal(Error(datastream.UnalignedArgument("on", bits)))
  history(events) |> should.equal([])
  ref.delete(events)
}

pub fn empty_delimiter_is_an_argument_error_without_source_effects_test() {
  let events = ref.new([])
  datastream.delimited(recorded([<<7>>], events), on: <<>>)
  |> result.map(fn(_) { Nil })
  |> should.equal(Error(datastream.EmptyDelimiter))
  history(events) |> should.equal([])
  ref.delete(events)
}

pub fn zero_frame_limit_accepts_empty_frames_then_closes_on_nonempty_test() {
  let events = ref.new([])
  let assert Ok(frames) =
    recorded([<<0, 1, 99>>, <<0>>], events)
    |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 0)
  datastream.to_list(frames)
  |> should.equal([
    Ok(<<>>),
    Error(datastream.FrameTooLarge(claimed: #(0, 1), max: 0)),
  ])
  history(events) |> should.equal([Opened, Pulled(0), Closed(1)])
  ref.delete(events)
}

pub fn bounded_overflow_discards_queues_and_notifies_each_active_consumer_test() {
  let events = ref.new([])
  let assert Ok([fast, slow, slower]) =
    datastream.broadcast_bounded(
      recorded([1, 2, 3], events),
      into: 3,
      max_queue: 1,
    )
  let failure = datastream.QueueLimitExceeded(consumer: 1, max_queue: 1)
  datastream.to_list(fast) |> should.equal([Ok(1), Error(failure)])
  datastream.to_list(fast) |> should.equal([])
  datastream.to_list(slower) |> should.equal([Error(failure)])
  datastream.to_list(slower) |> should.equal([])
  datastream.to_list(slow) |> should.equal([Error(failure)])
  datastream.to_list(slow) |> should.equal([])
  history(events) |> should.equal([Opened, Pulled(0), Pulled(1), Closed(2)])
  ref.delete(events)
}

pub fn bounded_overflow_allows_cancelling_an_unnotified_consumer_test() {
  let events = ref.new([])
  let assert Ok([fast, cancelled, slow]) =
    datastream.broadcast_bounded(
      recorded([1, 2, 3], events),
      into: 3,
      max_queue: 1,
    )
  let failure = datastream.QueueLimitExceeded(consumer: 1, max_queue: 1)
  datastream.to_list(fast) |> should.equal([Ok(1), Error(failure)])
  cancelled |> datastream.take(up_to: 0) |> datastream.drain
  datastream.to_list(cancelled) |> should.equal([])
  datastream.to_list(slow) |> should.equal([Error(failure)])
  datastream.to_list(slow) |> should.equal([])
  datastream.to_list(fast) |> should.equal([])
  history(events) |> should.equal([Opened, Pulled(0), Pulled(1), Closed(2)])
  ref.delete(events)
}

pub fn bounded_overflow_continuations_are_safe_to_close_test() {
  let events = ref.new([])
  let assert Ok([fast, slow]) =
    datastream.broadcast_bounded(
      recorded([1, 2, 3], events),
      into: 2,
      max_queue: 1,
    )
  let failure = datastream.QueueLimitExceeded(consumer: 1, max_queue: 1)
  datastream.collect_result(fast) |> should.equal(Error(failure))
  history(events) |> should.equal([Opened, Pulled(0), Pulled(1), Closed(2)])
  datastream.collect_result(slow) |> should.equal(Error(failure))
  datastream.to_list(fast) |> should.equal([])
  datastream.to_list(slow) |> should.equal([])
  history(events) |> should.equal([Opened, Pulled(0), Pulled(1), Closed(2)])
  ref.delete(events)
}

pub fn bounded_last_consumer_early_exit_closes_the_latest_source_once_test() {
  let events = ref.new([])
  let assert Ok([fast, slow]) =
    datastream.broadcast_bounded(
      recorded([1, 2, 3], events),
      into: 2,
      max_queue: 1,
    )
  datastream.first(fast) |> should.equal(Ok(Ok(1)))
  history(events) |> should.equal([Opened, Pulled(0)])
  datastream.to_list(fast) |> should.equal([])
  datastream.first(slow) |> should.equal(Ok(Ok(1)))
  history(events) |> should.equal([Opened, Pulled(0), Closed(1)])
  datastream.to_list(fast) |> should.equal([])
  datastream.to_list(slow) |> should.equal([])
  history(events) |> should.equal([Opened, Pulled(0), Closed(1)])
  ref.delete(events)
}

pub fn bounded_closed_consumer_neither_overflows_nor_advances_upstream_test() {
  let events = ref.new([])
  let assert Ok([fast, cancelled, slow]) =
    datastream.broadcast_bounded(
      recorded([1, 2, 3], events),
      into: 3,
      max_queue: 1,
    )
  cancelled |> datastream.take(up_to: 0) |> datastream.drain
  datastream.to_list(cancelled) |> should.equal([])
  history(events) |> should.equal([])
  let failure = datastream.QueueLimitExceeded(consumer: 2, max_queue: 1)
  datastream.to_list(fast) |> should.equal([Ok(1), Error(failure)])
  datastream.to_list(cancelled) |> should.equal([])
  datastream.to_list(slow) |> should.equal([Error(failure)])
  history(events) |> should.equal([Opened, Pulled(0), Pulled(1), Closed(2)])
  ref.delete(events)
}

pub fn bounded_naturally_ended_consumers_cannot_restart_the_resource_test() {
  let events = ref.new([])
  let assert Ok([left, right]) =
    datastream.broadcast_bounded(
      recorded([1, 2], events),
      into: 2,
      max_queue: 1,
    )
  datastream.zip(left, right)
  |> datastream.to_list
  |> should.equal([#(Ok(1), Ok(1)), #(Ok(2), Ok(2))])
  datastream.to_list(left) |> should.equal([])
  datastream.to_list(right) |> should.equal([])
  history(events)
  |> should.equal([Opened, Pulled(0), Pulled(1), Pulled(2), Closed(2)])
  ref.delete(events)
}

pub fn bounded_queue_validation_precedes_consumer_count_without_effects_test() {
  use count <- list.each([-1, 0, 2])
  use limit <- list.each([-1, 0, 1])
  let events = ref.new([])
  let actual =
    datastream.broadcast_bounded(
      recorded([1], events),
      into: count,
      max_queue: limit,
    )
  let expected = case limit > 0 {
    True -> Ok(Nil)
    False ->
      Error(datastream.NotPositive("broadcast_bounded", "max_queue", limit))
  }
  result.map(actual, fn(_) { Nil }) |> should.equal(expected)
  history(events) |> should.equal([])
  case actual {
    Ok(consumers) -> {
      list.length(consumers)
      |> should.equal(case count > 0 {
        True -> count
        False -> 0
      })
      list.each(consumers, fn(consumer) {
        consumer |> datastream.take(up_to: 0) |> datastream.drain
      })
    }
    Error(_) -> Nil
  }
  history(events) |> should.equal([])
  ref.delete(events)
}
