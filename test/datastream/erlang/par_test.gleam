//// BEAM-only tests for `datastream/erlang/par`.

@target(erlang)
import datastream

@target(erlang)
import datastream/erlang/internal/event_log

@target(erlang)
import datastream/erlang/par

@target(erlang)
import datastream/fold

@target(erlang)
import datastream/source

@target(erlang)
import datastream/stream

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
  source.from_list([1, 2, 3, 4])
  |> par.map_ordered_with(with: fn(x) { x * 2 }, max_workers: 2, max_buffer: 4)
  |> fold.to_list
  |> should.equal([2, 4, 6, 8])
}

@target(erlang)
pub fn map_ordered_on_empty_yields_empty_test() {
  source.from_list([])
  |> par.map_ordered_with(with: fn(x) { x * 2 }, max_workers: 2, max_buffer: 4)
  |> fold.to_list
  |> should.equal([])
}

// --- map_unordered -------------------------------------------------------

@target(erlang)
pub fn map_unordered_yields_set_equal_to_inputs_test() {
  source.from_list([1, 2, 3, 4])
  |> par.map_unordered_with(
    with: fn(x) { x * 2 },
    max_workers: 2,
    max_buffer: 4,
  )
  |> fold.to_list
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
pub fn each_ordered_returns_nil_test() {
  source.from_list([1, 2, 3])
  |> par.each_ordered_with(with: fn(_) { Nil }, max_workers: 2, max_buffer: 4)
  |> should.equal(Nil)
}

@target(erlang)
pub fn each_unordered_returns_nil_test() {
  source.from_list([1, 2, 3])
  |> par.each_unordered_with(with: fn(_) { Nil }, max_workers: 2, max_buffer: 4)
  |> should.equal(Nil)
}

// --- merge ---------------------------------------------------------------

@target(erlang)
pub fn merge_interleaves_two_sources_test() {
  par.merge_with(
    streams: [source.from_list([1, 2]), source.from_list([3, 4])],
    max_buffer: 4,
  )
  |> fold.to_list
  |> list.sort(by: int_compare)
  |> should.equal([1, 2, 3, 4])
}

@target(erlang)
pub fn merge_empty_list_yields_empty_stream_test() {
  par.merge_with(streams: [], max_buffer: 4)
  |> fold.to_list
  |> should.equal([])
}

// --- race ----------------------------------------------------------------

@target(erlang)
pub fn race_empty_list_yields_empty_test() {
  par.race(streams: [])
  |> fold.to_list
  |> should.equal([])
}

@target(erlang)
pub fn race_single_source_passes_through_test() {
  par.race(streams: [source.from_list([1, 2, 3])])
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

@target(erlang)
pub fn race_all_empty_contenders_yields_empty_test() {
  par.race(streams: [source.empty(), source.empty()])
  |> fold.to_list
  |> should.equal([])
}

@target(erlang)
pub fn race_one_empty_and_one_emitting_yields_emitting_test() {
  par.race(streams: [source.empty(), source.from_list([1, 2, 3])])
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

@target(erlang)
pub fn race_multiple_empties_and_one_emitting_yields_emitting_test() {
  par.race(streams: [source.empty(), source.empty(), source.from_list([42])])
  |> fold.to_list
  |> should.equal([42])
}

@target(erlang)
pub fn race_emitting_first_and_empties_later_yields_emitting_test() {
  par.race(streams: [source.from_list([7, 8]), source.empty(), source.empty()])
  |> fold.to_list
  |> should.equal([7, 8])
}

// --- close contract ------------------------------------------------------

@target(erlang)
pub fn merge_take_early_exit_closes_upstreams_test() {
  let log = event_log.new_log()
  let a = event_log.counted_resource([1, 2, 3], named: "a", log: log)
  let b = event_log.counted_resource([4, 5, 6], named: "b", log: log)

  let _result =
    par.merge_with(streams: [a, b], max_buffer: 4)
    |> stream.take(up_to: 1)
    |> fold.to_list

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
    |> par.map_unordered_with(with: fn(x) { x }, max_workers: 2, max_buffer: 4)
    |> stream.take(up_to: 1)
    |> fold.to_list

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
    |> par.map_ordered_with(with: fn(x) { x }, max_workers: 2, max_buffer: 4)
    |> stream.take(up_to: 1)
    |> fold.to_list

  process.sleep(50)
  let events = event_log.drain(log, within: 50)
  event_log.count_opens(events) |> should.equal(1)
  event_log.count_closes(events) |> should.equal(1)
}

@target(erlang)
pub fn race_winner_take_full_consumption_closes_winner_test() {
  let log = event_log.new_log()
  let winner = event_log.counted_resource([1], named: "w", log: log)
  let loser = source.from_list([2])

  let _result =
    par.race(streams: [winner, loser])
    |> fold.to_list

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
  source.from_list([1, 2, 3, 4])
  |> par.map_unordered(with: fn(x) { x * 10 })
  |> fold.to_list
  |> list.sort(by: int_compare)
  |> should.equal([10, 20, 30, 40])
}

@target(erlang)
pub fn map_ordered_default_runs_test() {
  source.from_list([1, 2, 3, 4])
  |> par.map_ordered(with: fn(x) { x * 10 })
  |> fold.to_list
  |> should.equal([10, 20, 30, 40])
}

@target(erlang)
pub fn each_unordered_default_returns_nil_test() {
  source.from_list([1, 2, 3])
  |> par.each_unordered(with: fn(_) { Nil })
  |> should.equal(Nil)
}

@target(erlang)
pub fn each_ordered_default_returns_nil_test() {
  source.from_list([1, 2, 3])
  |> par.each_ordered(with: fn(_) { Nil })
  |> should.equal(Nil)
}

@target(erlang)
pub fn merge_default_runs_test() {
  par.merge(streams: [source.from_list([1, 2]), source.from_list([3, 4])])
  |> fold.to_list
  |> list.sort(by: int_compare)
  |> should.equal([1, 2, 3, 4])
}

// --- back-pressure -------------------------------------------------------

@target(erlang)
pub fn map_unordered_with_handles_infinite_upstream_test() {
  // Without back-pressure the worker pool would spin forever pulling
  // from the infinite source. With max_workers / max_buffer set, take
  // halts the pipeline cleanly after 10 elements.
  source.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  |> par.map_unordered_with(with: fn(x) { x }, max_workers: 2, max_buffer: 4)
  |> stream.take(up_to: 10)
  |> fold.to_list
  |> list.length
  |> should.equal(10)
}

@target(erlang)
pub fn map_ordered_with_handles_infinite_upstream_test() {
  source.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  |> par.map_ordered_with(with: fn(x) { x }, max_workers: 2, max_buffer: 4)
  |> stream.take(up_to: 10)
  |> fold.to_list
  |> should.equal([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
}

@target(erlang)
pub fn merge_with_n_streams_greater_than_max_buffer_test() {
  // 5 streams, max_buffer = 2: only 2 workers can be in-flight at any
  // moment. All 15 elements must still be emitted.
  let make_stream = fn() { source.from_list([1, 2, 3]) }
  par.merge_with(
    streams: [
      make_stream(),
      make_stream(),
      make_stream(),
      make_stream(),
      make_stream(),
    ],
    max_buffer: 2,
  )
  |> fold.to_list
  |> list.length
  |> should.equal(15)
}

@target(erlang)
pub fn merge_with_handles_infinite_streams_test() {
  let infinite =
    source.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  par.merge_with(streams: [infinite], max_buffer: 2)
  |> stream.take(up_to: 5)
  |> fold.to_list
  |> list.length
  |> should.equal(5)
}

@target(erlang)
pub fn map_unordered_with_bounds_in_flight_test() {
  // Each worker enters f, sleeps to overlap with peers, leaves f.
  // The peak observed concurrent in-flight count must not exceed
  // max_workers (which equals in_flight bound for map_unordered).
  let log = event_log.new_log()
  source.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  |> par.map_unordered_with(
    with: fn(x) {
      event_log.record_enter(log, named: "f")
      process.sleep(20)
      event_log.record_leave(log, named: "f")
      x
    },
    max_workers: 3,
    max_buffer: 8,
  )
  |> stream.take(up_to: 15)
  |> fold.to_list
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
  source.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
  |> par.map_ordered_with(
    with: fn(x) {
      event_log.record_enter(log, named: "f")
      process.sleep(20)
      event_log.record_leave(log, named: "f")
      x
    },
    max_workers: 4,
    max_buffer: 4,
  )
  |> stream.take(up_to: 20)
  |> fold.to_list
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
    source.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
    |> stream.map(with: fn(x) {
      event_log.record_enter(log, named: "pump")
      process.sleep(10)
      event_log.record_leave(log, named: "pump")
      x
    })
  }
  par.merge_with(
    streams: [
      make_stream(),
      make_stream(),
      make_stream(),
      make_stream(),
      make_stream(),
    ],
    max_buffer: 2,
  )
  |> stream.take(up_to: 20)
  |> fold.to_list
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
    source.unfold(from: 1, with: fn(n) { datastream.Next(n, n + 1) })
    |> stream.map(with: fn(x) {
      event_log.record_enter(log, named: "pump")
      process.sleep(5)
      event_log.record_leave(log, named: "pump")
      x
    })
  }
  let result =
    par.merge_with(
      streams: [
        make_stream(),
        make_stream(),
        make_stream(),
        make_stream(),
        make_stream(),
      ],
      max_buffer: 2,
    )
    // Slow consumer: 15 ms between successive downstream pulls.
    |> stream.tap(with: fn(_) { process.sleep(15) })
    |> stream.take(up_to: 15)
    |> fold.to_list
  list.length(result) |> should.equal(15)

  process.sleep(150)
  let events = event_log.drain(log, within: 150)
  let peak = event_log.peak_concurrent(events, named: "pump")
  { peak <= 2 } |> should.be_true
}

// --- construction-time panics --------------------------------------------
//
// `validate_par_args` and `validate_buffer` panic on construction
// when concurrency knobs are out of range. Catching a Gleam `panic`
// directly is awkward, but spawning the failing call in an unlinked
// process and observing that no completion message arrives within a
// short timeout is sufficient: a panicking process never sends, while
// a non-panicking call here completes in microseconds.

@target(erlang)
fn panicked(thunk: fn() -> Nil) -> Bool {
  let done = process.new_subject()
  let _pid =
    process.spawn_unlinked(fn() {
      thunk()
      process.send(done, Nil)
    })
  case process.receive(from: done, within: 100) {
    Ok(_) -> False
    Error(_) -> True
  }
}

@target(erlang)
pub fn map_unordered_with_panics_on_zero_max_workers_test() {
  let did_panic =
    panicked(fn() {
      let _result =
        par.map_unordered_with(
          source.from_list([1]),
          with: fn(x) { x },
          max_workers: 0,
          max_buffer: 4,
        )
      Nil
    })
  did_panic |> should.be_true
}

@target(erlang)
pub fn map_ordered_with_panics_on_max_buffer_below_max_workers_test() {
  let did_panic =
    panicked(fn() {
      let _result =
        par.map_ordered_with(
          source.from_list([1]),
          with: fn(x) { x },
          max_workers: 4,
          max_buffer: 2,
        )
      Nil
    })
  did_panic |> should.be_true
}

@target(erlang)
pub fn merge_with_panics_on_zero_max_buffer_test() {
  let did_panic =
    panicked(fn() {
      let _result =
        par.merge_with(streams: [source.from_list([1])], max_buffer: 0)
      Nil
    })
  did_panic |> should.be_true
}
