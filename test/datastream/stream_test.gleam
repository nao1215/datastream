import datastream.{type Stream, Done, Next}
import datastream/internal/ref
import gleam/list
import gleam/string
import gleeunit
import gleeunit/should

@target(erlang)
import gleam/int

@target(erlang)
import gleam/dynamic

@target(erlang)
import gleam/dynamic/decode

@target(erlang)
import gleam/erlang/atom

@target(erlang)
import gleam/erlang/process

pub fn main() -> Nil {
  gleeunit.main()
}

fn from_list(list) {
  datastream.unfold(from: list, with: fn(xs) {
    case xs {
      [] -> Done
      [head, ..tail] -> Next(element: head, state: tail)
    }
  })
}

fn repeat(value) {
  datastream.unfold(from: value, with: fn(s) { Next(element: s, state: s) })
}

fn iterate(start, with f) {
  datastream.unfold(from: start, with: fn(s) { Next(element: s, state: f(s)) })
}

pub fn map_doubles_each_element_test() {
  from_list([1, 2, 3])
  |> datastream.map(with: fn(x) { x * 2 })
  |> datastream.to_list
  |> should.equal([2, 4, 6])
}

pub fn map_on_empty_returns_empty_test() {
  from_list([])
  |> datastream.map(with: fn(x) { x * 2 })
  |> datastream.to_list
  |> should.equal([])
}

pub fn filter_keeps_matching_elements_test() {
  from_list([1, 2, 3, 4])
  |> datastream.filter(keeping: fn(x) { x > 2 })
  |> datastream.to_list
  |> should.equal([3, 4])
}

pub fn filter_with_constant_false_yields_empty_test() {
  from_list([1, 2, 3])
  |> datastream.filter(keeping: fn(_) { False })
  |> datastream.to_list
  |> should.equal([])
}

pub fn take_yields_first_n_test() {
  from_list([1, 2, 3])
  |> datastream.take(up_to: 2)
  |> datastream.to_list
  |> should.equal([1, 2])
}

pub fn take_zero_yields_empty_test() {
  from_list([1, 2, 3])
  |> datastream.take(up_to: 0)
  |> datastream.to_list
  |> should.equal([])
}

pub fn take_negative_yields_empty_test() {
  from_list([1, 2, 3])
  |> datastream.take(up_to: -5)
  |> datastream.to_list
  |> should.equal([])
}

pub fn take_very_negative_yields_empty_test() {
  from_list([1, 2, 3, 4, 5])
  |> datastream.take(up_to: -1_000_000)
  |> datastream.to_list
  |> should.equal([])
}

pub fn take_more_than_available_yields_all_test() {
  from_list([1, 2, 3])
  |> datastream.take(up_to: 99)
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn take_terminates_on_infinite_source_test() {
  repeat(7)
  |> datastream.take(up_to: 3)
  |> datastream.to_list
  |> should.equal([7, 7, 7])
}

pub fn drop_skips_first_n_test() {
  from_list([1, 2, 3])
  |> datastream.drop(up_to: 1)
  |> datastream.to_list
  |> should.equal([2, 3])
}

pub fn drop_zero_is_identity_test() {
  from_list([1, 2, 3])
  |> datastream.drop(up_to: 0)
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn drop_negative_is_identity_test() {
  from_list([1, 2, 3])
  |> datastream.drop(up_to: -5)
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn drop_very_negative_is_identity_test() {
  from_list([1, 2, 3, 4, 5])
  |> datastream.drop(up_to: -1_000_000)
  |> datastream.to_list
  |> should.equal([1, 2, 3, 4, 5])
}

pub fn drop_more_than_available_yields_empty_test() {
  from_list([1, 2, 3])
  |> datastream.drop(up_to: 99)
  |> datastream.to_list
  |> should.equal([])
}

pub fn take_while_yields_longest_passing_prefix_test() {
  from_list([1, 2, 1, 4])
  |> datastream.take_while(satisfying: fn(x) { x < 3 })
  |> datastream.to_list
  |> should.equal([1, 2, 1])
}

pub fn take_while_with_constant_false_yields_empty_test() {
  from_list([5, 6, 7])
  |> datastream.take_while(satisfying: fn(_) { False })
  |> datastream.to_list
  |> should.equal([])
}

pub fn take_while_terminates_on_infinite_source_test() {
  iterate(1, with: fn(x) { x + 1 })
  |> datastream.take_while(satisfying: fn(x) { x < 4 })
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn drop_while_skips_longest_passing_prefix_test() {
  from_list([1, 2, 1, 4])
  |> datastream.drop_while(satisfying: fn(x) { x < 3 })
  |> datastream.to_list
  |> should.equal([4])
}

pub fn drop_while_with_constant_false_is_identity_test() {
  from_list([1, 2, 1])
  |> datastream.drop_while(satisfying: fn(_) { False })
  |> datastream.to_list
  |> should.equal([1, 2, 1])
}

pub fn append_concatenates_two_streams_test() {
  datastream.append(from_list([1, 2]), from_list([3, 4]))
  |> datastream.to_list
  |> should.equal([1, 2, 3, 4])
}

pub fn append_with_empty_first_yields_second_test() {
  datastream.append(from_list([]), from_list([3, 4]))
  |> datastream.to_list
  |> should.equal([3, 4])
}

pub fn append_with_empty_second_yields_first_test() {
  datastream.append(from_list([1, 2]), from_list([]))
  |> datastream.to_list
  |> should.equal([1, 2])
}

pub fn map_is_lazy_until_terminal_test() {
  let _pipeline =
    from_list([1, 2, 3])
    |> datastream.map(with: fn(_) {
      panic as "map must not run its function before a terminal"
    })

  Nil
}

pub fn map_invokes_callback_once_per_element_on_terminal_test() {
  let pipeline =
    from_list([1, 2, 3])
    |> datastream.map(with: fn(x) { x + 100 })

  pipeline |> datastream.to_list |> should.equal([101, 102, 103])
}

pub fn filter_map_keeps_some_drops_none_test() {
  from_list([1, 2, 3])
  |> datastream.filter_map(with: fn(x) {
    case x > 1 {
      True -> Ok(x * 10)
      False -> Error(Nil)
    }
  })
  |> datastream.to_list
  |> should.equal([20, 30])
}

pub fn filter_map_on_empty_yields_empty_test() {
  from_list([])
  |> datastream.filter_map(with: fn(x) { Ok(x) })
  |> datastream.to_list
  |> should.equal([])
}

pub fn flat_map_concatenates_inner_streams_test() {
  from_list([1, 2, 3])
  |> datastream.flat_map(with: fn(x) { from_list([x, x]) })
  |> datastream.to_list
  |> should.equal([1, 1, 2, 2, 3, 3])
}

pub fn flat_map_with_all_empty_inners_yields_empty_test() {
  from_list([1, 2])
  |> datastream.flat_map(with: fn(_) { datastream.empty() })
  |> datastream.to_list
  |> should.equal([])
}

pub fn flatten_concatenates_inner_streams_test() {
  from_list([from_list([1]), from_list([2, 3])])
  |> datastream.flatten
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn flatten_is_observationally_flat_map_identity_test() {
  let direct =
    from_list([from_list([1, 2]), from_list([]), from_list([3])])
    |> datastream.flatten
    |> datastream.to_list
  let via_flat_map =
    from_list([from_list([1, 2]), from_list([]), from_list([3])])
    |> datastream.flat_map(with: fn(s) { s })
    |> datastream.to_list
  direct |> should.equal(via_flat_map)
}

pub fn concat_walks_streams_in_list_order_test() {
  datastream.concat([from_list([1]), from_list([2, 3]), from_list([4])])
  |> datastream.to_list
  |> should.equal([1, 2, 3, 4])
}

pub fn concat_of_empty_list_yields_empty_test() {
  datastream.concat([]) |> datastream.to_list |> should.equal([])
}

pub fn scan_emits_running_accumulator_test() {
  from_list([1, 2, 3, 4])
  |> datastream.scan(from: 0, with: fn(acc, x) { acc + x })
  |> datastream.to_list
  |> should.equal([1, 3, 6, 10])
}

pub fn scan_does_not_emit_seed_on_empty_input_test() {
  from_list([])
  |> datastream.scan(from: 0, with: fn(acc, x) { acc + x })
  |> datastream.to_list
  |> should.equal([])
}

pub fn map_accum_threads_state_and_emits_output_test() {
  from_list([10, 20, 30])
  |> datastream.map_with_state(from: 0, with: fn(acc, x) {
    let s = acc + x
    #(s, s)
  })
  |> datastream.to_list
  |> should.equal([10, 30, 60])
}

pub fn zip_pairs_until_either_halts_test() {
  datastream.zip(from_list([1, 2, 3]), from_list(["a", "b"]))
  |> datastream.to_list
  |> should.equal([#(1, "a"), #(2, "b")])
}

pub fn zip_with_empty_left_yields_empty_test() {
  datastream.zip(from_list([]), from_list([1, 2]))
  |> datastream.to_list
  |> should.equal([])
}

pub fn zip_with_combiner_test() {
  datastream.zip_with(
    from_list([1, 2, 3]),
    from_list([10, 20, 30]),
    with: fn(a, b) { a + b },
  )
  |> datastream.to_list
  |> should.equal([11, 22, 33])
}

pub fn intersperse_inserts_separator_between_elements_test() {
  from_list([1, 2, 3])
  |> datastream.intersperse(with: 0)
  |> datastream.to_list
  |> should.equal([1, 0, 2, 0, 3])
}

pub fn intersperse_on_empty_yields_empty_test() {
  from_list([])
  |> datastream.intersperse(with: 0)
  |> datastream.to_list
  |> should.equal([])
}

pub fn intersperse_on_singleton_unchanged_test() {
  from_list([1])
  |> datastream.intersperse(with: 0)
  |> datastream.to_list
  |> should.equal([1])
}

pub fn tap_re_emits_elements_unchanged_test() {
  from_list([1, 2, 3])
  |> datastream.tap(with: fn(_) { Nil })
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn dedupe_adjacent_collapses_runs_test() {
  from_list([1, 1, 2, 2, 2, 3, 1])
  |> datastream.deduplicate_adjacent
  |> datastream.to_list
  |> should.equal([1, 2, 3, 1])
}

pub fn dedupe_adjacent_on_empty_yields_empty_test() {
  from_list([])
  |> datastream.deduplicate_adjacent
  |> datastream.to_list
  |> should.equal([])
}

pub fn dedupe_adjacent_keeps_non_adjacent_duplicates_test() {
  from_list([1, 2, 1, 2, 1])
  |> datastream.deduplicate_adjacent
  |> datastream.to_list
  |> should.equal([1, 2, 1, 2, 1])
}

pub fn buffer_preserves_elements_at_capacity_one_test() {
  from_list([1, 2, 3, 4, 5])
  |> datastream.buffer(prefetch: 1)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([1, 2, 3, 4, 5])
}

pub fn buffer_preserves_elements_at_small_capacity_test() {
  from_list([1, 2, 3, 4, 5])
  |> datastream.buffer(prefetch: 3)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([1, 2, 3, 4, 5])
}

pub fn buffer_preserves_elements_when_capacity_exceeds_source_test() {
  from_list([1, 2, 3])
  |> datastream.buffer(prefetch: 100)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn buffer_on_empty_yields_empty_test() {
  from_list([])
  |> datastream.buffer(prefetch: 4)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([])
}

pub fn buffer_terminates_on_infinite_source_via_take_test() {
  iterate(0, with: fn(n) { n + 1 })
  |> datastream.buffer(prefetch: 8)
  |> should.be_ok
  |> datastream.take(up_to: 5)
  |> datastream.to_list
  |> should.equal([0, 1, 2, 3, 4])
}

pub fn buffer_then_take_zero_yields_empty_test() {
  from_list([1, 2, 3])
  |> datastream.buffer(prefetch: 4)
  |> should.be_ok
  |> datastream.take(up_to: 0)
  |> datastream.to_list
  |> should.equal([])
}

pub fn buffer_composes_with_map_test() {
  from_list([1, 2, 3])
  |> datastream.buffer(prefetch: 2)
  |> should.be_ok
  |> datastream.map(with: fn(x) { x * 10 })
  |> datastream.to_list
  |> should.equal([10, 20, 30])
}

pub fn buffer_checked_ok_matches_buffer_test() {
  let assert Ok(s) =
    datastream.buffer(over: from_list([1, 2, 3, 4]), prefetch: 2)
  s |> datastream.to_list |> should.equal([1, 2, 3, 4])
}

pub fn buffer_checked_zero_returns_error_test() {
  let assert Error(datastream.NotPositive(argument: _, function: name, given: g)) =
    datastream.buffer(over: from_list([1, 2, 3]), prefetch: 0)
  name |> should.equal("buffer")
  g |> should.equal(0)
}

pub fn buffer_checked_negative_returns_error_test() {
  let assert Error(datastream.NotPositive(argument: _, function: name, given: g)) =
    datastream.buffer(over: from_list([1, 2, 3]), prefetch: -4)
  name |> should.equal("buffer")
  g |> should.equal(-4)
}

fn signal_after(n: Int) -> Stream(Bool) {
  // Yields False for the first `n` pulls, then True forever.
  datastream.unfold(from: 0, with: fn(count) {
    case count >= n {
      True -> Next(element: True, state: count + 1)
      False -> Next(element: False, state: count + 1)
    }
  })
}

fn signal_never() -> Stream(Bool) {
  datastream.unfold(from: Nil, with: fn(_) { Done })
}

fn signal_immediate() -> Stream(Bool) {
  signal_after(0)
}

pub fn interrupt_when_immediate_signal_yields_empty_test() {
  from_list([1, 2, 3])
  |> datastream.interrupt_when(signal: signal_immediate())
  |> datastream.to_list
  |> should.equal([])
}

pub fn interrupt_when_signal_never_is_identity_test() {
  from_list([1, 2, 3])
  |> datastream.interrupt_when(signal: signal_never())
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn interrupt_when_signal_fires_after_one_pull_test() {
  iterate(0, with: fn(n) { n + 1 })
  |> datastream.interrupt_when(signal: signal_after(1))
  |> datastream.to_list
  |> should.equal([0])
}

pub fn interrupt_when_signal_fires_after_three_pulls_test() {
  iterate(0, with: fn(n) { n + 1 })
  |> datastream.interrupt_when(signal: signal_after(3))
  |> datastream.to_list
  |> should.equal([0, 1, 2])
}

pub fn interrupt_when_on_empty_stream_yields_empty_test() {
  from_list([])
  |> datastream.interrupt_when(signal: signal_immediate())
  |> datastream.to_list
  |> should.equal([])
}

pub fn interrupt_when_on_empty_stream_with_dead_signal_yields_empty_test() {
  from_list([])
  |> datastream.interrupt_when(signal: signal_never())
  |> datastream.to_list
  |> should.equal([])
}

pub fn interrupt_when_composes_with_take_test() {
  iterate(0, with: fn(n) { n + 1 })
  |> datastream.interrupt_when(signal: signal_after(10))
  |> datastream.take(up_to: 4)
  |> datastream.to_list
  |> should.equal([0, 1, 2, 3])
}

pub fn interrupt_when_signal_wins_over_take_test() {
  iterate(0, with: fn(n) { n + 1 })
  |> datastream.interrupt_when(signal: signal_after(2))
  |> datastream.take(up_to: 100)
  |> datastream.to_list
  |> should.equal([0, 1])
}

type ResourceEvent {
  Opened(String)
  Pulled(String, Int)
  Closed(String, Int)
}

fn logged_resource(
  elements: List(a),
  name: String,
  log: ref.Ref(List(ResourceEvent)),
) -> Stream(a) {
  datastream.resource(
    open: fn() {
      ref.set(log, [Opened(name), ..read(log)])
      #(0, elements)
    },
    next: fn(state) {
      let #(index, remaining) = state
      ref.set(log, [Pulled(name, index), ..read(log)])
      case remaining {
        [] -> Done
        [element, ..rest] -> Next(element:, state: #(index + 1, rest))
      }
    },
    close: fn(state) { ref.set(log, [Closed(name, state.0), ..read(log)]) },
  )
}

pub fn interrupt_when_empty_body_closes_signal_latest_state_test() {
  let log = ref.new([])
  let interrupted =
    logged_resource([], "body", log)
    |> datastream.interrupt_when(signal: logged_resource([False], "signal", log))

  assert read(log) == []
  assert datastream.to_list(interrupted) == []
  let events = list.reverse(read(log))
  ref.delete(log)
  assert events
    == [
      Opened("signal"),
      Pulled("signal", 0),
      Opened("body"),
      Pulled("body", 0),
      Closed("body", 0),
      Closed("signal", 1),
    ]
}

pub fn interrupt_when_finished_body_closes_signal_latest_state_test() {
  let log = ref.new([])
  let values =
    logged_resource([42], "body", log)
    |> datastream.interrupt_when(signal: logged_resource(
      [False, False, False],
      "signal",
      log,
    ))
    |> datastream.to_list

  assert values == [42]
  let events = list.reverse(read(log))
  ref.delete(log)
  assert events
    == [
      Opened("signal"),
      Pulled("signal", 0),
      Opened("body"),
      Pulled("body", 0),
      Pulled("signal", 1),
      Pulled("body", 1),
      Closed("body", 1),
      Closed("signal", 2),
    ]
}

pub fn interrupt_when_finished_signal_is_not_pulled_or_closed_again_test() {
  let log = ref.new([])
  let values =
    logged_resource([10, 20, 30], "body", log)
    |> datastream.interrupt_when(signal: logged_resource([False], "signal", log))
    |> datastream.to_list

  assert values == [10, 20, 30]
  let events = list.reverse(read(log))
  ref.delete(log)
  assert events
    == [
      Opened("signal"),
      Pulled("signal", 0),
      Opened("body"),
      Pulled("body", 0),
      Pulled("signal", 1),
      Closed("signal", 1),
      Pulled("body", 1),
      Pulled("body", 2),
      Pulled("body", 3),
      Closed("body", 3),
    ]
}

pub fn interrupt_when_true_signal_closes_both_latest_states_test() {
  let log = ref.new([])
  let values =
    logged_resource([10, 20], "body", log)
    |> datastream.interrupt_when(signal: logged_resource(
      [False, True, False],
      "signal",
      log,
    ))
    |> datastream.to_list

  assert values == [10]
  let events = list.reverse(read(log))
  ref.delete(log)
  assert events
    == [
      Opened("signal"),
      Pulled("signal", 0),
      Opened("body"),
      Pulled("body", 0),
      Pulled("signal", 1),
      Closed("signal", 2),
      Closed("body", 1),
    ]
}

pub fn interrupt_when_immediate_true_signal_does_not_open_body_test() {
  let log = ref.new([])
  let values =
    logged_resource([10, 20], "body", log)
    |> datastream.interrupt_when(signal: logged_resource([True], "signal", log))
    |> datastream.to_list

  assert values == []
  let events = list.reverse(read(log))
  ref.delete(log)
  assert events == [Opened("signal"), Pulled("signal", 0), Closed("signal", 1)]
}

pub fn interrupt_when_downstream_take_closes_both_latest_states_test() {
  let log = ref.new([])
  let values =
    logged_resource([10, 20], "body", log)
    |> datastream.interrupt_when(signal: logged_resource(
      [False, False],
      "signal",
      log,
    ))
    |> datastream.take(up_to: 1)
    |> datastream.to_list

  assert values == [10]
  let events = list.reverse(read(log))
  ref.delete(log)
  assert events
    == [
      Opened("signal"),
      Pulled("signal", 0),
      Opened("body"),
      Pulled("body", 0),
      Closed("signal", 1),
      Closed("body", 1),
    ]
}

pub fn broadcast_two_consumers_see_same_elements_test() {
  let assert [a, b] = datastream.broadcast(over: from_list([1, 2, 3]), into: 2)
  datastream.to_list(a) |> should.equal([1, 2, 3])
  datastream.to_list(b) |> should.equal([1, 2, 3])
}

pub fn broadcast_consumer_one_partial_then_two_test() {
  let assert [a, b] =
    datastream.broadcast(over: from_list([1, 2, 3, 4]), into: 2)
  a |> datastream.take(up_to: 2) |> datastream.to_list |> should.equal([1, 2])
  datastream.to_list(b) |> should.equal([1, 2, 3, 4])
}

pub fn broadcast_three_consumers_independent_pace_test() {
  let assert [a, b, c] =
    datastream.broadcast(over: from_list([10, 20, 30, 40]), into: 3)
  a |> datastream.to_list |> should.equal([10, 20, 30, 40])
  b |> datastream.take(up_to: 2) |> datastream.to_list |> should.equal([10, 20])
  datastream.to_list(c) |> should.equal([10, 20, 30, 40])
}

pub fn broadcast_one_consumer_is_identity_test() {
  let assert [only] = datastream.broadcast(over: from_list([1, 2, 3]), into: 1)
  datastream.to_list(only) |> should.equal([1, 2, 3])
}

pub fn broadcast_on_empty_yields_empty_consumers_test() {
  let assert [a, b] = datastream.broadcast(over: from_list([]), into: 2)
  datastream.to_list(a) |> should.equal([])
  datastream.to_list(b) |> should.equal([])
}

pub fn broadcast_consumer_with_take_zero_does_not_block_other_test() {
  let assert [a, b] = datastream.broadcast(over: from_list([1, 2, 3]), into: 2)
  a |> datastream.take(up_to: 0) |> datastream.to_list |> should.equal([])
  datastream.to_list(b) |> should.equal([1, 2, 3])
}

pub fn broadcast_zero_returns_empty_list_test() {
  datastream.broadcast(over: from_list([1, 2, 3]), into: 0)
  |> list.length
  |> should.equal(0)
}

pub fn broadcast_negative_returns_empty_list_test() {
  datastream.broadcast(over: from_list([1, 2, 3]), into: -3)
  |> list.length
  |> should.equal(0)
}

// --- broadcast_bounded ----------------------------------------------------

pub fn broadcast_bounded_within_limit_works_like_broadcast_test() {
  // While both consumers stay within `max_queue` elements of each
  // other, behaviour is identical to `broadcast`.
  let assert [a, b] =
    datastream.broadcast_bounded(
      over: from_list([1, 2, 3]),
      into: 2,
      max_queue: 5,
    )
    |> should.be_ok
  datastream.collect_result(a) |> should.be_ok |> should.equal([1, 2, 3])
  datastream.collect_result(b) |> should.be_ok |> should.equal([1, 2, 3])
}

pub fn broadcast_bounded_high_limit_matches_broadcast_test() {
  // With a max_queue at least as large as the source length, the
  // bound is never reached and behaviour matches `broadcast`.
  let assert [a, b] =
    datastream.broadcast_bounded(
      over: from_list([1, 2, 3, 4, 5]),
      into: 2,
      max_queue: 100,
    )
    |> should.be_ok
  datastream.collect_result(a) |> should.be_ok |> should.equal([1, 2, 3, 4, 5])
  datastream.collect_result(b) |> should.be_ok |> should.equal([1, 2, 3, 4, 5])
}

pub fn broadcast_bounded_reports_a_slow_consumer_overflow_test() {
  let assert [fast, slow] =
    datastream.broadcast_bounded(
      over: from_list([1, 2, 3, 4, 5]),
      into: 2,
      max_queue: 2,
    )
    |> should.be_ok
  let failure = Error(datastream.QueueLimitExceeded(consumer: 1, max_queue: 2))
  datastream.to_list(fast) |> should.equal([Ok(1), Ok(2), failure])
  datastream.to_list(slow) |> should.equal([failure])
}

pub fn broadcast_bounded_zero_consumers_returns_empty_list_test() {
  datastream.broadcast_bounded(
    over: from_list([1, 2, 3]),
    into: 0,
    max_queue: 4,
  )
  |> should.be_ok
  |> list.length
  |> should.equal(0)
}

pub fn broadcast_bounded_negative_consumers_returns_empty_list_test() {
  datastream.broadcast_bounded(
    over: from_list([1, 2, 3]),
    into: -3,
    max_queue: 4,
  )
  |> should.be_ok
  |> list.length
  |> should.equal(0)
}

pub fn broadcast_bounded_rejects_zero_max_queue_test() {
  assert datastream.broadcast_bounded(
      from_list([1, 2, 3]),
      into: 2,
      max_queue: 0,
    )
    |> should.be_error
    == datastream.NotPositive(
      function: "broadcast_bounded",
      argument: "max_queue",
      given: 0,
    )
}

pub fn unzip_round_trip_with_zip_test() {
  let zipped = datastream.zip(from_list([1, 2, 3]), from_list(["a", "b", "c"]))
  let #(left, right) = datastream.unzip(zipped)
  datastream.to_list(left) |> should.equal([1, 2, 3])
  datastream.to_list(right) |> should.equal(["a", "b", "c"])
}

pub fn unzip_on_empty_yields_empty_pair_test() {
  let #(left, right) = datastream.unzip(from_list([]))
  datastream.to_list(left) |> should.equal([])
  datastream.to_list(right) |> should.equal([])
}

pub fn unzip_left_first_then_right_test() {
  let pairs = from_list([#(1, "a"), #(2, "b"), #(3, "c")])
  let #(left, right) = datastream.unzip(pairs)
  datastream.to_list(left) |> should.equal([1, 2, 3])
  datastream.to_list(right) |> should.equal(["a", "b", "c"])
}

pub fn unzip_right_first_then_left_test() {
  let pairs = from_list([#(1, "a"), #(2, "b"), #(3, "c")])
  let #(left, right) = datastream.unzip(pairs)
  datastream.to_list(right) |> should.equal(["a", "b", "c"])
  datastream.to_list(left) |> should.equal([1, 2, 3])
}

pub fn unzip_partial_left_full_right_test() {
  let pairs = from_list([#(1, "a"), #(2, "b"), #(3, "c"), #(4, "d")])
  let #(left, right) = datastream.unzip(pairs)
  left
  |> datastream.take(up_to: 2)
  |> datastream.to_list
  |> should.equal([1, 2])
  datastream.to_list(right) |> should.equal(["a", "b", "c", "d"])
}

fn chunks_to_lists(stream_of_chunks) {
  stream_of_chunks
  |> datastream.to_list
}

fn groups_to_pairs(
  stream_of_groups: Stream(#(k, List(a))),
) -> List(#(k, List(a))) {
  stream_of_groups
  |> datastream.to_list
  |> list.map(fn(pair) {
    let #(key, c) = pair
    #(key, c)
  })
}

pub fn chunks_of_emits_full_size_then_remainder_test() {
  from_list([1, 2, 3, 4, 5])
  |> datastream.chunks_of(into: 2)
  |> should.be_ok
  |> chunks_to_lists
  |> should.equal([[1, 2], [3, 4], [5]])
}

pub fn chunks_of_evenly_divisible_test() {
  from_list([1, 2, 3, 4])
  |> datastream.chunks_of(into: 2)
  |> should.be_ok
  |> chunks_to_lists
  |> should.equal([[1, 2], [3, 4]])
}

pub fn chunks_of_on_empty_yields_no_chunks_test() {
  from_list([])
  |> datastream.chunks_of(into: 3)
  |> should.be_ok
  |> chunks_to_lists
  |> should.equal([])
}

pub fn chunks_of_size_larger_than_source_test() {
  from_list([1, 2, 3])
  |> datastream.chunks_of(into: 99)
  |> should.be_ok
  |> chunks_to_lists
  |> should.equal([[1, 2, 3]])
}

pub fn chunks_of_terminates_on_infinite_source_test() {
  datastream.repeat(1)
  |> datastream.chunks_of(into: 2)
  |> should.be_ok
  |> datastream.take(up_to: 3)
  |> chunks_to_lists
  |> should.equal([[1, 1], [1, 1], [1, 1]])
}

pub fn chunks_of_checked_ok_matches_chunks_of_test() {
  let assert Ok(s) =
    datastream.chunks_of(over: from_list([1, 2, 3, 4, 5]), into: 2)
  s |> chunks_to_lists |> should.equal([[1, 2], [3, 4], [5]])
}

pub fn chunks_of_checked_zero_returns_error_test() {
  let assert Error(datastream.NotPositive(argument: _, function: name, given: g)) =
    datastream.chunks_of(over: from_list([1, 2, 3]), into: 0)
  name |> should.equal("chunks_of")
  g |> should.equal(0)
}

pub fn chunks_of_checked_negative_returns_error_test() {
  let assert Error(datastream.NotPositive(argument: _, function: name, given: g)) =
    datastream.chunks_of(over: from_list([1, 2, 3]), into: -2)
  name |> should.equal("chunks_of")
  g |> should.equal(-2)
}

pub fn group_adjacent_groups_runs_of_equal_test() {
  from_list([1, 1, 2, 3, 3, 3])
  |> datastream.group_adjacent(by: fn(x) { x })
  |> groups_to_pairs
  |> should.equal([#(1, [1, 1]), #(2, [2]), #(3, [3, 3, 3])])
}

pub fn group_adjacent_by_key_function_test() {
  let first_char = fn(s: String) -> String {
    case string.first(s) {
      Ok(c) -> c
      Error(_) -> ""
    }
  }

  from_list(["apple", "ant", "banana", "bee", "cat"])
  |> datastream.group_adjacent(by: first_char)
  |> groups_to_pairs
  |> should.equal([
    #("a", ["apple", "ant"]),
    #("b", ["banana", "bee"]),
    #("c", ["cat"]),
  ])
}

pub fn group_adjacent_on_empty_yields_no_groups_test() {
  from_list([])
  |> datastream.group_adjacent(by: fn(x) { x })
  |> groups_to_pairs
  |> should.equal([])
}

pub fn group_adjacent_does_not_merge_non_adjacent_duplicates_test() {
  from_list([1, 2, 1])
  |> datastream.group_adjacent(by: fn(x) { x })
  |> groups_to_pairs
  |> should.equal([#(1, [1]), #(2, [2]), #(1, [1])])
}

// --- smallest valid arguments (found by mutation testing) ------------------

pub fn buffer_checked_accepts_a_capacity_of_one_test() {
  let assert Ok(s) = datastream.buffer(over: from_list([1, 2, 3]), prefetch: 1)
  datastream.to_list(s) |> should.equal([1, 2, 3])
}

pub fn chunks_of_one_wraps_every_element_test() {
  from_list([1, 2, 3])
  |> datastream.chunks_of(into: 1)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([[1], [2], [3]])
}

pub fn chunks_of_checked_accepts_one_test() {
  let assert Ok(s) = datastream.chunks_of(over: from_list([1, 2]), into: 1)
  s |> datastream.to_list |> should.equal([[1], [2]])
}

pub fn broadcast_bounded_into_one_returns_one_consumer_test() {
  let assert [only] =
    datastream.broadcast_bounded(over: from_list([1, 2]), into: 1, max_queue: 1)
    |> should.be_ok
  datastream.collect_result(only) |> should.be_ok |> should.equal([1, 2])
}

pub fn broadcast_bounded_allows_a_queue_of_exactly_max_queue_test() {
  // Draining `a` first leaves both elements queued for `b`: a queue of
  // exactly `max_queue` is within the bound.
  let assert [a, b] =
    datastream.broadcast_bounded(over: from_list([1, 2]), into: 2, max_queue: 2)
    |> should.be_ok
  datastream.collect_result(a) |> should.be_ok |> should.equal([1, 2])
  datastream.collect_result(b) |> should.be_ok |> should.equal([1, 2])
}

pub fn broadcast_bounded_accepts_a_max_queue_of_one_test() {
  let assert [a, b] =
    datastream.broadcast_bounded(over: from_list([1, 2]), into: 2, max_queue: 1)
    |> should.be_ok
  // Alternate the consumers so neither falls behind by more than one.
  assert datastream.zip(a, b) |> datastream.to_list
    == [#(Ok(1), Ok(1)), #(Ok(2), Ok(2))]
}

pub fn broadcast_three_consumers_each_see_every_element_test() {
  let assert [a, b, c] =
    datastream.broadcast(over: from_list([1, 2, 3]), into: 3)
  datastream.to_list(c) |> should.equal([1, 2, 3])
  datastream.to_list(a) |> should.equal([1, 2, 3])
  datastream.to_list(b) |> should.equal([1, 2, 3])
}

// --- buffer does not touch an upstream that returned Done --------------------

fn logged_counter(
  upto limit: Int,
  log log: ref.Ref(List(String)),
) -> Stream(Int) {
  datastream.resource(
    open: fn() {
      ref.set(log, ["open", ..read(log)])
      0
    },
    next: fn(i) {
      ref.set(log, ["next", ..read(log)])
      case i < limit {
        True -> Next(element: i, state: i + 1)
        False -> Done
      }
    },
    close: fn(_) { ref.set(log, ["close", ..read(log)]) },
  )
}

pub fn buffer_does_not_pull_or_close_upstream_after_done_test() {
  let log = ref.new([])
  logged_counter(upto: 2, log: log)
  |> datastream.buffer(prefetch: 5)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([0, 1])
  list.reverse(read(log))
  |> should.equal(["open", "next", "next", "next", "close"])
}

pub fn buffer_take_after_upstream_done_closes_once_test() {
  let log = ref.new([])
  logged_counter(upto: 2, log: log)
  |> datastream.buffer(prefetch: 5)
  |> should.be_ok
  |> datastream.take(up_to: 1)
  |> datastream.to_list
  |> should.equal([0])
  list.reverse(read(log))
  |> should.equal(["open", "next", "next", "next", "close"])
}

// --- broadcast releases its shared state ------------------------------------

@target(erlang)
@external(erlang, "erlang", "get")
fn process_dictionary() -> List(#(dynamic.Dynamic, dynamic.Dynamic))

@target(erlang)
pub fn unzip_releases_its_state_once_both_halves_finish_test() {
  let before = list.length(process_dictionary())
  int.range(from: 0, to: 100, with: Nil, run: fn(_, _) {
    let #(left, right) = datastream.unzip(from_list([#(1, 2), #(3, 4)]))
    datastream.to_list(left) |> should.equal([1, 3])
    datastream.to_list(right) |> should.equal([2, 4])
  })
  list.length(process_dictionary()) |> should.equal(before)
}

@target(erlang)
pub fn broadcast_releases_its_state_once_every_consumer_is_closed_test() {
  let before = list.length(process_dictionary())
  int.range(from: 0, to: 100, with: Nil, run: fn(_, _) {
    let assert [a, b, c] = datastream.broadcast(over: repeat(1), into: 3)
    a |> datastream.take(up_to: 2) |> datastream.to_list |> should.equal([1, 1])
    b |> datastream.take(up_to: 1) |> datastream.to_list |> should.equal([1])
    c |> datastream.take(up_to: 0) |> datastream.to_list |> should.equal([])
  })
  list.length(process_dictionary()) |> should.equal(before)
}

@target(erlang)
pub fn broadcast_consumer_pulled_in_another_process_panics_clearly_test() {
  let assert [a, _] = datastream.broadcast(over: from_list([1, 2]), into: 2)
  let pid = process.spawn_unlinked(fn() { datastream.to_list(a) })
  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(d) { d })
  let assert Ok(process.ProcessDown(reason: process.Abnormal(reason), ..)) =
    process.selector_receive(from: selector, within: 1000)
  // The exit reason is `{Error, Stacktrace}`; the error carries the
  // panic message.
  let assert Ok(message) =
    decode.run(
      reason,
      decode.at(
        [0],
        decode.at([atom.to_dynamic(atom.create("message"))], decode.string),
      ),
    )
  message
  |> string.contains("pulled in the process that created")
  |> should.be_true
}

fn read(cell: ref.Ref(a)) -> a {
  case ref.get(cell) {
    Ok(value) -> value
    Error(_) -> panic as "test ref was deleted"
  }
}

// --- internal ref cell --------------------------------------------------------

type Marker {
  Undefined
}

pub fn ref_holds_a_value_that_looks_like_an_empty_slot_test() {
  // On Erlang `Undefined` is the atom `undefined`, which the process
  // dictionary also returns for a missing key.
  let cell = ref.new(Undefined)
  ref.get(cell) |> should.equal(Ok(Undefined))
  ref.delete(cell)
  ref.get(cell) |> should.equal(Error(Nil))
}

@target(erlang)
pub fn ref_set_from_another_process_names_set_test() {
  let cell = ref.new(1)
  let pid = process.spawn_unlinked(fn() { ref.set(cell, 2) })
  let monitor = process.monitor(pid)
  let assert Ok(process.ProcessDown(reason: process.Abnormal(reason), ..)) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(d) { d })
    |> process.selector_receive(within: 1000)
  decode.run(
    reason,
    decode.at(
      [0],
      decode.at([atom.to_dynamic(atom.create("function"))], decode.string),
    ),
  )
  |> should.equal(Ok("set"))
}
