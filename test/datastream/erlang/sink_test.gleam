//// BEAM-only tests for `datastream/erlang/sink`.

@target(erlang)
import datastream/erlang/sink as beam_sink

@target(erlang)
import datastream/source

@target(erlang)
import gleam/erlang/process

@target(erlang)
import gleeunit/should

@target(javascript)
/// Empty placeholder so this module compiles cleanly on JavaScript.
pub const beam_only_marker: String = "datastream/erlang/sink_test is BEAM-only"

@target(erlang)
pub fn into_subject_sends_elements_in_order_test() {
  let subject = process.new_subject()

  source.from_list([1, 2, 3])
  |> beam_sink.into_subject(into: subject)
  |> should.equal(Nil)

  process.receive(from: subject, within: 100) |> should.equal(Ok(1))
  process.receive(from: subject, within: 100) |> should.equal(Ok(2))
  process.receive(from: subject, within: 100) |> should.equal(Ok(3))
  process.receive(from: subject, within: 50) |> should.equal(Error(Nil))
}

@target(erlang)
pub fn into_subject_on_empty_stream_sends_nothing_test() {
  let subject = process.new_subject()

  source.empty()
  |> beam_sink.into_subject(into: subject)
  |> should.equal(Nil)

  process.receive(from: subject, within: 50) |> should.equal(Error(Nil))
}
