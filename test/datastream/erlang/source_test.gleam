//// BEAM-only tests for `datastream/erlang/source`.
////
//// Every test in this file is `@target(erlang)`; the JavaScript test
//// run skips them entirely.

@target(erlang)
import datastream

@target(erlang)
import datastream/erlang/internal/event_log

@target(erlang)
import datastream/erlang/source as beam_source

@target(erlang)
import datastream/fold

@target(erlang)
import datastream/source

@target(erlang)
import datastream/stream

@target(erlang)
import gleam/erlang/process

@target(erlang)
import gleeunit/should

@target(javascript)
/// Empty placeholder so this module compiles cleanly on JavaScript.
pub const beam_only_marker: String = "datastream/erlang/source_test is BEAM-only"

// --- from_subject -------------------------------------------------------

@target(erlang)
pub fn from_subject_receives_messages_in_order_test() {
  let subject = process.new_subject()
  process.spawn(fn() {
    process.send(subject, 1)
    process.send(subject, 2)
    process.send(subject, 3)
  })

  beam_source.from_subject(from: subject, while: fn() { True })
  |> stream.take(up_to: 3)
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

@target(erlang)
pub fn from_subject_halts_when_keep_running_returns_false_test() {
  let subject = process.new_subject()
  // No messages ever sent; keep_running goes False on the first poll.
  beam_source.from_subject(from: subject, while: fn() { False })
  |> fold.to_list
  |> should.equal([])
}

// --- ticks / interval ---------------------------------------------------

@target(erlang)
pub fn ticks_emits_three_monotonic_timestamps_test() {
  let stamps =
    beam_source.ticks(every: 20)
    |> stream.take(up_to: 3)
    |> fold.to_list

  case stamps {
    [a, b, c] -> {
      should.be_true(b >= a)
      should.be_true(c >= b)
    }
    _ -> panic as "expected three timestamps"
  }
}

@target(erlang)
pub fn interval_emits_three_nils_test() {
  beam_source.interval(every: 20)
  |> stream.take(up_to: 3)
  |> fold.to_list
  |> should.equal([Nil, Nil, Nil])
}

// --- timeout ------------------------------------------------------------

@target(erlang)
pub fn timeout_passes_fast_elements_through_test() {
  source.from_list([1, 2, 3])
  |> beam_source.timeout(within: 100)
  |> fold.to_list
  |> should.equal([Ok(1), Ok(2), Ok(3)])
}

@target(erlang)
pub fn timeout_emits_error_when_upstream_is_too_slow_test() {
  // upstream that sleeps 200ms before returning Done. The 50ms
  // deadline trips first.
  let slow =
    source.unfold(from: Nil, with: fn(_) {
      process.sleep(200)
      datastream.Done
    })

  slow
  |> beam_source.timeout(within: 50)
  |> fold.to_list
  |> should.equal([Error(Nil)])
}

@target(erlang)
pub fn timeout_take_early_exit_closes_upstream_test() {
  let log = event_log.new_log()
  let upstream = event_log.counted_resource([1, 2, 3], named: "u", log: log)

  upstream
  |> beam_source.timeout(within: 100)
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([Ok(1)])

  process.sleep(50)
  let events = event_log.drain(log, within: 50)
  event_log.count_opens(events) |> should.equal(1)
  event_log.count_closes(events) |> should.equal(1)
}

@target(erlang)
pub fn timeout_passes_periodic_fast_elements_through_test() {
  // Upstream emits three elements, 20ms apart. A 500ms deadline
  // never trips, so every element comes through as Ok and the stream
  // ends normally with Done.
  source.unfold(from: 0, with: fn(s) {
    case s >= 3 {
      True -> datastream.Done
      False -> {
        process.sleep(20)
        datastream.Next(element: s, state: s + 1)
      }
    }
  })
  |> beam_source.timeout(within: 500)
  |> fold.to_list
  |> should.equal([Ok(0), Ok(1), Ok(2)])
}

@target(erlang)
pub fn timeout_halts_stream_after_deadline_trip_test() {
  // A slow upstream takes 200ms to produce one element. With a 50ms
  // deadline, the first pull trips the deadline. The stream must
  // halt — the abandoned worker is not retried, even if downstream
  // requests more elements. `take(up_to: 5)` is satisfied by exactly
  // one Error(Nil), proving the single-shot halt contract.
  let slow =
    source.unfold(from: 0, with: fn(s) {
      case s {
        0 -> {
          process.sleep(200)
          datastream.Next(element: 99, state: 1)
        }
        _ -> datastream.Done
      }
    })

  slow
  |> beam_source.timeout(within: 50)
  |> stream.take(up_to: 5)
  |> fold.to_list
  |> should.equal([Error(Nil)])
}
