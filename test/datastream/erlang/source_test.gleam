//// BEAM-only tests for `datastream/erlang/source`.
////
//// Every test in this file is `@target(erlang)`; the JavaScript test
//// run skips them entirely.

@target(erlang)
import datastream

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
