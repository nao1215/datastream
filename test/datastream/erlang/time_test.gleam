//// BEAM-only smoke tests for `datastream/erlang/time`.
////
//// Time-based combinators are inherently jittery so the assertions
//// here are intentionally loose: we check the shape of the output
//// (counts, ordering, terminal halt) rather than exact timing.

@target(erlang)
import datastream/chunk
@target(erlang)
import datastream/erlang/internal/event_log
@target(erlang)
import datastream/erlang/time as beam_time
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
import gleeunit/should

@target(javascript)
/// Empty placeholder so this module compiles cleanly on JavaScript.
pub const beam_only_marker: String = "datastream/erlang/time_test is BEAM-only"

// --- debounce ------------------------------------------------------------

@target(erlang)
pub fn debounce_on_empty_source_yields_empty_test() {
  source.empty()
  |> beam_time.debounce(quiet_for: 50)
  |> fold.to_list
  |> should.equal([])
}

// --- throttle ------------------------------------------------------------

@target(erlang)
pub fn throttle_emits_first_element_immediately_test() {
  source.from_list([1])
  |> beam_time.throttle(every: 50)
  |> fold.to_list
  |> should.equal([1])
}

@target(erlang)
pub fn throttle_on_empty_source_yields_empty_test() {
  source.empty()
  |> beam_time.throttle(every: 50)
  |> fold.to_list
  |> should.equal([])
}

// --- sample --------------------------------------------------------------

@target(erlang)
pub fn sample_on_empty_source_yields_empty_test() {
  source.empty()
  |> beam_time.sample(every: 50)
  |> fold.to_list
  |> should.equal([])
}

// --- rate_limit ----------------------------------------------------------

@target(erlang)
pub fn rate_limit_does_not_lose_elements_test() {
  source.from_list([1, 2, 3])
  |> beam_time.rate_limit(max_per_window: 10, window_ms: 100)
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

@target(erlang)
pub fn rate_limit_on_empty_source_yields_empty_test() {
  source.empty()
  |> beam_time.rate_limit(max_per_window: 2, window_ms: 100)
  |> fold.to_list
  |> should.equal([])
}

// --- window_time ---------------------------------------------------------

@target(erlang)
pub fn window_time_collects_all_elements_across_windows_test() {
  // Source emits 5 elements as fast as possible; with a 50ms window,
  // they all land in the first window's buffer. The trailing buffer
  // is flushed when the upstream signals Done.
  let result =
    source.from_list([1, 2, 3, 4, 5])
    |> beam_time.window_time(span: 50)
    |> fold.to_list
    |> list.flat_map(chunk.to_list)
  result |> should.equal([1, 2, 3, 4, 5])
}

@target(erlang)
pub fn window_time_on_empty_source_yields_empty_test() {
  source.empty()
  |> beam_time.window_time(span: 50)
  |> fold.to_list
  |> should.equal([])
}

@target(erlang)
pub fn window_time_take_one_chunk_via_take_test() {
  // 5 fast elements then upstream Done. Take only the first emitted
  // chunk (which holds the trailing buffer flush) to avoid blocking
  // on a window timer.
  source.from_list([1, 2, 3, 4, 5])
  |> beam_time.window_time(span: 50)
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> list.flat_map(chunk.to_list)
  |> list.length
  |> should.equal(5)
}

// --- close contract ------------------------------------------------------

@target(erlang)
pub fn debounce_take_early_exit_closes_upstream_test() {
  let log = event_log.new_log()
  let upstream = event_log.counted_resource([1, 2, 3], named: "u", log: log)

  let _result =
    upstream
    |> beam_time.debounce(quiet_for: 30)
    |> stream.take(up_to: 1)
    |> fold.to_list

  process.sleep(100)
  let events = event_log.drain(log, within: 100)
  event_log.count_opens(events) |> should.equal(1)
  event_log.count_closes(events) |> should.equal(1)
}

@target(erlang)
pub fn window_time_take_early_exit_closes_upstream_test() {
  let log = event_log.new_log()
  let upstream = event_log.counted_resource([1, 2, 3], named: "u", log: log)

  let _result =
    upstream
    |> beam_time.window_time(span: 30)
    |> stream.take(up_to: 1)
    |> fold.to_list

  process.sleep(100)
  let events = event_log.drain(log, within: 100)
  event_log.count_opens(events) |> should.equal(1)
  event_log.count_closes(events) |> should.equal(1)
}
