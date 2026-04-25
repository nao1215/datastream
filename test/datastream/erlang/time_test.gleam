//// BEAM-only smoke tests for `datastream/erlang/time`.
////
//// Time-based combinators are inherently jittery so the assertions
//// here are intentionally loose: we check the shape of the output
//// (counts, ordering, terminal halt) rather than exact timing.

@target(erlang)
import datastream.{Done, Next}
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

@target(erlang)
pub fn window_time_groups_close_elements_into_one_chunk_test() {
  // Two elements 30 ms apart with a 500 ms window. Both arrive
  // before the deadline trips, and upstream Done flushes the
  // trailing buffer as a single chunk.
  let timed =
    datastream.unfold(from: 0, with: fn(state) {
      case state {
        0 -> Next(element: 1, state: 1)
        1 -> {
          process.sleep(30)
          Next(element: 2, state: 2)
        }
        _ -> Done
      }
    })

  timed
  |> beam_time.window_time(span: 500)
  |> fold.to_list
  |> list.map(chunk.to_list)
  |> should.equal([[1, 2]])
}

@target(erlang)
pub fn window_time_separates_distant_elements_into_different_chunks_test() {
  // Element 1 at t=0; element 2 at t≈150 ms. With a 100 ms window,
  // the first deadline trips between them so each element ends up
  // in its own non-empty chunk. Empty chunks for silent intermediate
  // windows are tolerated; ordering across all chunks is pinned.
  let timed =
    datastream.unfold(from: 0, with: fn(state) {
      case state {
        0 -> Next(element: 1, state: 1)
        1 -> {
          process.sleep(150)
          Next(element: 2, state: 2)
        }
        _ -> Done
      }
    })

  let chunks =
    timed
    |> beam_time.window_time(span: 100)
    |> fold.to_list
    |> list.map(chunk.to_list)

  list.flatten(chunks) |> should.equal([1, 2])
  chunks
  |> list.filter(keeping: fn(c) { c != [] })
  |> list.length
  |> should.equal(2)
}

@target(erlang)
pub fn window_time_emits_empty_chunk_for_silent_window_test() {
  // One element at t=0 then a 120 ms silence before upstream Done.
  // With a 50 ms window, at least one window trips entirely silent
  // between the first chunk's flush and the trailing Done — the
  // combinator must emit it as an empty chunk.
  let timed =
    datastream.unfold(from: 0, with: fn(state) {
      case state {
        0 -> Next(element: 1, state: 1)
        1 -> {
          process.sleep(120)
          Done
        }
        _ -> Done
      }
    })

  let chunks =
    timed
    |> beam_time.window_time(span: 50)
    |> fold.to_list
    |> list.map(chunk.to_list)

  list.any(chunks, fn(c) { c == [] }) |> should.be_true
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

// --- emission semantics --------------------------------------------------
//
// These tests rely on wall-clock scheduling, so the assertions use
// loose upper / lower bounds. The numbers are chosen so a normally
// loaded CI scheduler clears the bound with margin even under jitter.

@target(erlang)
pub fn throttle_drops_elements_within_same_window_test() {
  // Five elements arrive ~10 ms apart; a 60 ms window should keep
  // only the first (and at most one more under bad scheduling).
  let result =
    source.from_list([1, 2, 3, 4, 5])
    |> stream.tap(with: fn(_) { process.sleep(10) })
    |> beam_time.throttle(every: 60)
    |> fold.to_list
  { list.length(result) <= 2 } |> should.be_true
}

@target(erlang)
pub fn debounce_emits_only_last_after_silence_test() {
  // Three elements ~5 ms apart, then upstream Done acts as the
  // terminating silence. The 30 ms debounce window is well above the
  // inter-arrival gap, so only the last value should emit.
  source.from_list([1, 2, 3])
  |> stream.tap(with: fn(_) { process.sleep(5) })
  |> beam_time.debounce(quiet_for: 30)
  |> fold.to_list
  |> should.equal([3])
}

@target(erlang)
pub fn sample_emits_at_least_one_snapshot_test() {
  // Eight elements ~25 ms apart (~200 ms total). Sampling every
  // 80 ms over that window should produce at least one snapshot
  // and at most five.
  let result =
    source.from_list([1, 2, 3, 4, 5, 6, 7, 8])
    |> stream.tap(with: fn(_) { process.sleep(25) })
    |> beam_time.sample(every: 80)
    |> fold.to_list
  let n = list.length(result)
  { n >= 1 && n <= 5 } |> should.be_true
}
