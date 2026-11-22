//// BEAM-only tests for `datastream/erlang/par`.

@target(erlang)
import datastream

@target(erlang)
import datastream/erlang/internal/event_log

@target(erlang)
import datastream/erlang/internal/watchdog

@target(erlang)
import gleam/erlang/process

@target(erlang)
import gleam/list

@target(erlang)
import gleam/order

@target(erlang)
import gleeunit/should

@target(javascript)
/// Empty placeholder so this module compiles cleanly on JavaScript.
pub const beam_only_marker: String = "datastream/erlang/par_test is BEAM-only"

// --- map_ordered ---------------------------------------------------------

@target(erlang)
pub fn map_ordered_preserves_input_order_test() {
  datastream.from_list([1, 2, 3, 4])
  |> datastream.map_ordered(
    with: fn(x) { x * 2 },
    max_workers: 2,
    max_buffer: 4,
  )
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([2, 4, 6, 8])
}

@target(erlang)
pub fn map_ordered_on_empty_yields_empty_test() {
  datastream.from_list([])
  |> datastream.map_ordered(
    with: fn(x) { x * 2 },
    max_workers: 2,
    max_buffer: 4,
  )
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([])
}

// --- map_unordered -------------------------------------------------------

@target(erlang)
pub fn map_unordered_yields_set_equal_to_inputs_test() {
  datastream.from_list([1, 2, 3, 4])
  |> datastream.map_unordered(
    with: fn(x) { x * 2 },
    max_workers: 2,
    max_buffer: 4,
  )
  |> should.be_ok
  |> datastream.to_list
  |> list.sort(by: int_compare)
  |> should.equal([2, 4, 6, 8])
}

@target(erlang)
fn int_compare(a: Int, b: Int) -> order.Order {
  case a == b, a < b {
    True, _ -> order.Eq
    False, True -> order.Lt
    False, False -> order.Gt
  }
}

// --- each_* --------------------------------------------------------------

@target(erlang)
pub fn ordered_map_then_each_returns_nil_test() {
  datastream.from_list([1, 2, 3])
  |> datastream.map_ordered(with: fn(x) { x }, max_workers: 2, max_buffer: 4)
  |> should.be_ok
  |> datastream.each(with: fn(_) { Nil })
  |> should.equal(Nil)
}

@target(erlang)
pub fn each_unordered_returns_nil_test() {
  datastream.from_list([1, 2, 3])
  |> datastream.each_unordered(
    with: fn(_) { Nil },
    max_workers: 2,
    max_buffer: 4,
  )
  |> should.be_ok
  |> should.equal(Nil)
}

// --- merge ---------------------------------------------------------------

@target(erlang)
pub fn merge_interleaves_two_sources_test() {
  datastream.merge(
    streams: [datastream.from_list([1, 2]), datastream.from_list([3, 4])],
    max_buffer: 4,
  )
  |> should.be_ok
  |> datastream.to_list
  |> list.sort(by: int_compare)
  |> should.equal([1, 2, 3, 4])
}

@target(erlang)
pub fn merge_empty_list_yields_empty_stream_test() {
  datastream.merge(streams: [], max_buffer: 4)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([])
}

// --- race ----------------------------------------------------------------

@target(erlang)
pub fn race_empty_list_yields_empty_test() {
  datastream.race(streams: [])
  |> datastream.to_list
  |> should.equal([])
}

@target(erlang)
pub fn race_single_source_passes_through_test() {
  datastream.race(streams: [datastream.from_list([1, 2, 3])])
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

@target(erlang)
pub fn race_all_empty_contenders_yields_empty_test() {
  datastream.race(streams: [datastream.empty(), datastream.empty()])
  |> datastream.to_list
  |> should.equal([])
}

@target(erlang)
pub fn race_one_empty_and_one_emitting_yields_emitting_test() {
  datastream.race(streams: [datastream.empty(), datastream.from_list([1, 2, 3])])
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

@target(erlang)
pub fn race_multiple_empties_and_one_emitting_yields_emitting_test() {
  datastream.race(streams: [
    datastream.empty(),
    datastream.empty(),
    datastream.from_list([42]),
  ])
  |> datastream.to_list
  |> should.equal([42])
}

@target(erlang)
pub fn race_emitting_first_and_empties_later_yields_emitting_test() {
  datastream.race(streams: [
    datastream.from_list([7, 8]),
    datastream.empty(),
    datastream.empty(),
  ])
  |> datastream.to_list
  |> should.equal([7, 8])
}

@target(erlang)
pub fn race_take_one_from_two_emitting_sources_yields_one_element_test() {
  // Both sources can emit. Downstream `take(1)` must terminate the
  // pipeline after exactly one element regardless of which source
  // won the race.
  datastream.race(streams: [
    datastream.from_list([1, 2, 3]),
    datastream.from_list([10, 20, 30]),
  ])
  |> datastream.take(up_to: 1)
  |> datastream.count
  |> should.equal(1)
}

@target(erlang)
pub fn race_three_emitting_sources_yields_one_winners_cardinality_test() {
  // Three sources, each emitting two elements. Exactly one wins; the
  // winner's full tail comes through. Because the winner is
  // non-deterministic, the stable assertion is cardinality: the
  // output length equals the losing choice's emit count (2).
  datastream.race(streams: [
    datastream.from_list([10, 20]),
    datastream.from_list([30, 40]),
    datastream.from_list([50, 60]),
  ])
  |> datastream.count
  |> should.equal(2)
}

// --- close contract ------------------------------------------------------

@target(erlang)
pub fn merge_take_early_exit_closes_upstreams_test() {
  let log = event_log.new_log()
  let a = event_log.counted_resource([1, 2, 3], named: "a", log: log)
  let b = event_log.counted_resource([4, 5, 6], named: "b", log: log)

  let _result =
    datastream.merge(streams: [a, b], max_buffer: 4)
    |> should.be_ok
    |> datastream.take(up_to: 1)
    |> datastream.to_list

  process.sleep(100)
  let events = event_log.drain(log, within: 100)
  event_log.count_opens(events) |> should.equal(2)
  event_log.count_closes(events) |> should.equal(2)
}

@target(erlang)
pub fn map_unordered_take_early_exit_closes_upstream_test() {
  let log = event_log.new_log()
  let upstream = event_log.counted_resource([1, 2, 3, 4], named: "u", log: log)

  let _result =
    upstream
    |> datastream.map_unordered(
      with: fn(x) { x },
      max_workers: 2,
      max_buffer: 4,
    )
    |> should.be_ok
    |> datastream.take(up_to: 1)
    |> datastream.to_list

  process.sleep(50)
  let events = event_log.drain(log, within: 50)
  event_log.count_opens(events) |> should.equal(1)
  event_log.count_closes(events) |> should.equal(1)
}

@target(erlang)
pub fn map_ordered_take_early_exit_closes_upstream_test() {
  let log = event_log.new_log()
  let upstream = event_log.counted_resource([1, 2, 3, 4], named: "u", log: log)

  let _result =
    upstream
    |> datastream.map_ordered(with: fn(x) { x }, max_workers: 2, max_buffer: 4)
    |> should.be_ok
    |> datastream.take(up_to: 1)
    |> datastream.to_list

  process.sleep(50)
  let events = event_log.drain(log, within: 50)
  event_log.count_opens(events) |> should.equal(1)
  event_log.count_closes(events) |> should.equal(1)
}

@target(erlang)
pub fn race_winner_take_full_consumption_closes_winner_test() {
  let log = event_log.new_log()
  let winner = event_log.counted_resource([1], named: "w", log: log)
  let loser = datastream.from_list([2])

  let _result =
    datastream.race(streams: [winner, loser])
    |> datastream.to_list

  process.sleep(50)
  let events = event_log.drain(log, within: 50)
  // The resource MUST be opened at least once, and every open MUST be
  // matched by a close.
  let opens = event_log.count_opens(events)
  let closes = event_log.count_closes(events)
  opens |> should.equal(closes)
}

// --- defaults / simple variants ------------------------------------------

@target(erlang)
pub fn map_unordered_default_runs_test() {
  datastream.from_list([1, 2, 3, 4])
  |> datastream.map_unordered(
    with: fn(x) { x * 10 },
    max_workers: 4,
    max_buffer: 16,
  )
  |> should.be_ok
  |> datastream.to_list
  |> list.sort(by: int_compare)
  |> should.equal([10, 20, 30, 40])
}

@target(erlang)
pub fn map_ordered_default_runs_test() {
  datastream.from_list([1, 2, 3, 4])
  |> datastream.map_ordered(
    with: fn(x) { x * 10 },
    max_workers: 4,
    max_buffer: 16,
  )
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([10, 20, 30, 40])
}

@target(erlang)
pub fn each_unordered_default_returns_nil_test() {
  datastream.from_list([1, 2, 3])
  |> datastream.each_unordered(
    with: fn(_) { Nil },
    max_workers: 4,
    max_buffer: 16,
  )
  |> should.be_ok
  |> should.equal(Nil)
}

@target(erlang)
pub fn ordered_map_then_each_with_default_limits_test() {
  datastream.from_list([1, 2, 3])
  |> datastream.map_ordered(with: fn(x) { x }, max_workers: 4, max_buffer: 16)
  |> should.be_ok
  |> datastream.each(with: fn(_) { Nil })
  |> should.equal(Nil)
}

@target(erlang)
pub fn merge_default_runs_test() {
  datastream.merge(
    streams: [datastream.from_list([1, 2]), datastream.from_list([3, 4])],
    max_buffer: 16,
  )
  |> should.be_ok
  |> datastream.to_list
  |> list.sort(by: int_compare)
  |> should.equal([1, 2, 3, 4])
}

// --- back-pressure -------------------------------------------------------

@target(erlang)
pub fn map_unordered_with_handles_infinite_upstream_test() {
  // Without back-pressure the worker pool would spin forever pulling
  // from the infinite source. With max_workers / max_buffer set, take
  // halts the pipeline cleanly after 10 elements.
  datastream.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  |> datastream.map_unordered(with: fn(x) { x }, max_workers: 2, max_buffer: 4)
  |> should.be_ok
  |> datastream.take(up_to: 10)
  |> datastream.to_list
  |> list.length
  |> should.equal(10)
}

@target(erlang)
pub fn map_ordered_with_handles_infinite_upstream_test() {
  datastream.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  |> datastream.map_ordered(with: fn(x) { x }, max_workers: 2, max_buffer: 4)
  |> should.be_ok
  |> datastream.take(up_to: 10)
  |> datastream.to_list
  |> should.equal([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
}

@target(erlang)
pub fn merge_with_n_streams_greater_than_max_buffer_test() {
  // 5 streams, max_buffer = 2: only 2 workers can be in-flight at any
  // moment. All 15 elements must still be emitted.
  let make_stream = fn() { datastream.from_list([1, 2, 3]) }
  datastream.merge(
    streams: [
      make_stream(),
      make_stream(),
      make_stream(),
      make_stream(),
      make_stream(),
    ],
    max_buffer: 2,
  )
  |> should.be_ok
  |> datastream.to_list
  |> list.length
  |> should.equal(15)
}

@target(erlang)
pub fn merge_with_handles_infinite_streams_test() {
  let infinite =
    datastream.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  datastream.merge(streams: [infinite], max_buffer: 2)
  |> should.be_ok
  |> datastream.take(up_to: 5)
  |> datastream.to_list
  |> list.length
  |> should.equal(5)
}

@target(erlang)
pub fn map_unordered_with_bounds_in_flight_test() {
  // Each worker enters f, sleeps to overlap with peers, leaves f.
  // The peak observed concurrent in-flight count must not exceed
  // max_workers (which equals in_flight bound for map_unordered).
  let log = event_log.new_log()
  datastream.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  |> datastream.map_unordered(
    with: fn(x) {
      event_log.record_enter(log, named: "f")
      process.sleep(20)
      event_log.record_leave(log, named: "f")
      x
    },
    max_workers: 3,
    max_buffer: 8,
  )
  |> should.be_ok
  |> datastream.take(up_to: 15)
  |> datastream.to_list
  |> list.length
  |> should.equal(15)

  process.sleep(100)
  let events = event_log.drain(log, within: 100)
  let peak = event_log.peak_concurrent(events, named: "f")
  { peak <= 3 } |> should.be_true
}

@target(erlang)
pub fn map_ordered_with_bounds_in_flight_test() {
  let log = event_log.new_log()
  datastream.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  |> datastream.map_ordered(
    with: fn(x) {
      event_log.record_enter(log, named: "f")
      process.sleep(20)
      event_log.record_leave(log, named: "f")
      x
    },
    max_workers: 4,
    max_buffer: 4,
  )
  |> should.be_ok
  |> datastream.take(up_to: 20)
  |> datastream.to_list
  |> list.length
  |> should.equal(20)

  process.sleep(100)
  let events = event_log.drain(log, within: 100)
  let peak = event_log.peak_concurrent(events, named: "f")
  { peak <= 4 } |> should.be_true
}

@target(erlang)
pub fn merge_with_bounds_in_flight_test() {
  // Five infinite streams; max_buffer = 2 should keep at most two
  // workers actively pulling (i.e. inside f) at any one time.
  let log = event_log.new_log()
  let make_stream = fn() {
    datastream.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
    |> datastream.map(with: fn(x) {
      event_log.record_enter(log, named: "pump")
      process.sleep(10)
      event_log.record_leave(log, named: "pump")
      x
    })
  }
  datastream.merge(
    streams: [
      make_stream(),
      make_stream(),
      make_stream(),
      make_stream(),
      make_stream(),
    ],
    max_buffer: 2,
  )
  |> should.be_ok
  |> datastream.take(up_to: 20)
  |> datastream.to_list
  |> list.length
  |> should.equal(20)

  process.sleep(150)
  let events = event_log.drain(log, within: 150)
  let peak = event_log.peak_concurrent(events, named: "pump")
  { peak <= 2 } |> should.be_true
}

@target(erlang)
pub fn merge_with_slow_consumer_does_not_overproduce_test() {
  // Same setup as above but the consumer pulls slowly. If ack-gating
  // were broken the workers would race ahead and `peak` would exceed
  // `max_buffer`. Each pump records its enter/leave so we can compute
  // the peak in-flight count.
  let log = event_log.new_log()
  let make_stream = fn() {
    datastream.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
    |> datastream.map(with: fn(x) {
      event_log.record_enter(log, named: "pump")
      process.sleep(5)
      event_log.record_leave(log, named: "pump")
      x
    })
  }
  let result =
    datastream.merge(
      streams: [
        make_stream(),
        make_stream(),
        make_stream(),
        make_stream(),
        make_stream(),
      ],
      max_buffer: 2,
    )
    |> should.be_ok
    // Slow consumer: 15 ms between successive downstream pulls.
    |> datastream.tap(with: fn(_) { process.sleep(15) })
    |> datastream.take(up_to: 15)
    |> datastream.to_list
  list.length(result) |> should.equal(15)

  process.sleep(150)
  let events = event_log.drain(log, within: 150)
  let peak = event_log.peak_concurrent(events, named: "pump")
  { peak <= 2 } |> should.be_true
}

// --- checked variants (#189) -------------------------------------------

@target(erlang)
pub fn map_unordered_with_checked_ok_matches_unchecked_test() {
  let assert Ok(s) =
    datastream.map_unordered(
      datastream.from_list([1, 2, 3]),
      with: fn(x) { x * 2 },
      max_workers: 2,
      max_buffer: 4,
    )
  let result = s |> datastream.to_list
  list.sort(result, by: int_compare) |> should.equal([2, 4, 6])
}

@target(erlang)
pub fn map_unordered_with_checked_rejects_zero_max_workers_test() {
  let assert Error(datastream.NotPositive(
    argument: "max_workers",
    function: name,
    given: g,
  )) =
    datastream.map_unordered(
      datastream.from_list([1]),
      with: fn(x) { x },
      max_workers: 0,
      max_buffer: 4,
    )
  name |> should.equal("map_unordered")
  g |> should.equal(0)
}

@target(erlang)
pub fn map_unordered_with_checked_rejects_buffer_below_workers_test() {
  let assert Error(datastream.BufferLessThanWorkers(
    max_workers: w,
    max_buffer: b,
  )) =
    datastream.map_unordered(
      datastream.from_list([1]),
      with: fn(x) { x },
      max_workers: 4,
      max_buffer: 2,
    )
  w |> should.equal(4)
  b |> should.equal(2)
}

@target(erlang)
pub fn map_ordered_with_checked_ok_matches_unchecked_test() {
  let assert Ok(s) =
    datastream.map_ordered(
      datastream.from_list([1, 2, 3]),
      with: fn(x) { x * 10 },
      max_workers: 2,
      max_buffer: 4,
    )
  s |> datastream.to_list |> should.equal([10, 20, 30])
}

@target(erlang)
pub fn map_ordered_with_checked_rejects_negative_max_workers_test() {
  let assert Error(datastream.NotPositive(
    argument: "max_workers",
    function: name,
    given: g,
  )) =
    datastream.map_ordered(
      datastream.from_list([1]),
      with: fn(x) { x },
      max_workers: -1,
      max_buffer: 4,
    )
  name |> should.equal("map_ordered")
  g |> should.equal(-1)
}

@target(erlang)
pub fn map_ordered_with_checked_rejects_buffer_below_workers_test() {
  let assert Error(datastream.BufferLessThanWorkers(
    max_workers: w,
    max_buffer: b,
  )) =
    datastream.map_ordered(
      datastream.from_list([1]),
      with: fn(x) { x },
      max_workers: 4,
      max_buffer: 1,
    )
  w |> should.equal(4)
  b |> should.equal(1)
}

@target(erlang)
pub fn merge_with_checked_ok_matches_unchecked_test() {
  let assert Ok(s) =
    datastream.merge(
      streams: [datastream.from_list([1, 2]), datastream.from_list([3, 4])],
      max_buffer: 4,
    )
  let result = s |> datastream.to_list
  list.sort(result, by: int_compare) |> should.equal([1, 2, 3, 4])
}

@target(erlang)
pub fn merge_with_checked_rejects_zero_max_buffer_test() {
  let assert Error(datastream.NotPositive(
    argument: "max_buffer",
    function: name,
    given: g,
  )) = datastream.merge(streams: [datastream.from_list([1])], max_buffer: 0)
  name |> should.equal("merge")
  g |> should.equal(0)
}

@target(erlang)
pub fn each_unordered_with_checked_ok_runs_effect_test() {
  let collector = process.new_subject()
  let assert Ok(Nil) =
    datastream.each_unordered(
      over: datastream.from_list([1, 2, 3]),
      with: fn(x) { process.send(collector, x) },
      max_workers: 2,
      max_buffer: 4,
    )
  let received = collect_three(collector, [])
  list.sort(received, by: int_compare) |> should.equal([1, 2, 3])
}

@target(erlang)
pub fn each_unordered_with_checked_rejects_zero_max_workers_test() {
  let assert Error(datastream.NotPositive(
    argument: "max_workers",
    function: name,
    given: g,
  )) =
    datastream.each_unordered(
      over: datastream.from_list([1]),
      with: fn(_) { Nil },
      max_workers: 0,
      max_buffer: 4,
    )
  name |> should.equal("each_unordered")
  g |> should.equal(0)
}

@target(erlang)
pub fn ordered_map_then_each_runs_effect_in_order_test() {
  let collector = process.new_subject()
  let assert Ok(values) =
    datastream.map_ordered(
      datastream.from_list([1, 2, 3]),
      max_workers: 2,
      max_buffer: 4,
      with: fn(x) { x },
    )
  datastream.each(values, with: fn(x) { process.send(collector, x) })
  assert collect_three(collector, []) |> list.reverse == [1, 2, 3]
}

@target(erlang)
pub fn each_ordered_with_checked_rejects_buffer_below_workers_test() {
  let assert Error(datastream.BufferLessThanWorkers(
    max_workers: w,
    max_buffer: b,
  )) =
    datastream.each_unordered(
      over: datastream.from_list([1]),
      with: fn(_) { Nil },
      max_workers: 3,
      max_buffer: 1,
    )
  w |> should.equal(3)
  b |> should.equal(1)
}

// --- smallest valid arguments (found by mutation testing) ------------------

@target(erlang)
pub fn map_ordered_with_checked_accepts_one_worker_and_equal_buffer_test() {
  let assert Ok(s) =
    datastream.map_ordered(
      over: datastream.from_list([1, 2, 3]),
      with: fn(x) { x * 10 },
      max_workers: 1,
      max_buffer: 1,
    )
  datastream.to_list(s) |> should.equal([10, 20, 30])
}

@target(erlang)
pub fn map_unordered_with_checked_accepts_buffer_equal_to_workers_test() {
  let assert Ok(s) =
    datastream.map_unordered(
      over: datastream.from_list([1, 2, 3]),
      with: fn(x) { x * 10 },
      max_workers: 2,
      max_buffer: 2,
    )
  datastream.to_list(s)
  |> list.sort(by: int_compare)
  |> should.equal([10, 20, 30])
}

@target(erlang)
pub fn map_ordered_with_accepts_one_worker_and_equal_buffer_test() {
  datastream.map_ordered(
    over: datastream.from_list([1, 2]),
    with: fn(x) { x + 1 },
    max_workers: 1,
    max_buffer: 1,
  )
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([2, 3])
}

@target(erlang)
pub fn merge_with_checked_accepts_a_buffer_of_one_test() {
  let assert Ok(s) =
    datastream.merge(
      streams: [datastream.from_list([1]), datastream.from_list([2])],
      max_buffer: 1,
    )
  datastream.to_list(s) |> list.sort(by: int_compare) |> should.equal([1, 2])
}

@target(erlang)
pub fn merge_with_accepts_a_buffer_of_one_test() {
  datastream.merge(streams: [datastream.from_list([1, 2])], max_buffer: 1)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([1, 2])
}

@target(erlang)
fn collect_three(subj: process.Subject(Int), acc: List(Int)) -> List(Int) {
  case list.length(acc) >= 3 {
    True -> acc
    False ->
      case process.receive(from: subj, within: 1000) {
        Ok(x) -> collect_three(subj, [x, ..acc])
        Error(_) -> acc
      }
  }
}

// --- a panic in f reaches the caller ------------------------------------
//
// Worker crashes are detected via process monitors. The coordinator
// closes the upstream, stops the other workers and re-raises the crash
// in the process pulling the stream, like `datastream.map` would.

@target(erlang)
pub fn map_unordered_with_propagates_a_panic_in_f_test() {
  watchdog.run(within: 2000, running: fn() {
    datastream.from_list([1, 2, 3])
    |> datastream.map_unordered(
      with: fn(_x) { panic as "boom" },
      max_workers: 2,
      max_buffer: 4,
    )
    |> should.be_ok
    |> datastream.to_list
  })
  |> should.equal(watchdog.Crashed)
}

@target(erlang)
pub fn map_ordered_with_propagates_a_panic_in_f_test() {
  watchdog.run(within: 2000, running: fn() {
    datastream.from_list([1, 2, 3])
    |> datastream.map_ordered(
      with: fn(_x) { panic as "boom" },
      max_workers: 2,
      max_buffer: 4,
    )
    |> should.be_ok
    |> datastream.to_list
  })
  |> should.equal(watchdog.Crashed)
}
