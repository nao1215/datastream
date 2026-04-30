//// End-to-end pipeline #2: chunked byte source → length-prefixed
//// frame reassembly → typed payload list.
////
//// Models a streaming wire protocol on top of an HTTP body, a TCP
//// socket, or any other byte source that delivers data in arbitrary
//// chunks: each frame is a 4-byte big-endian length followed by
//// that many payload bytes. `binary.length_prefixed_with` owns the
//// buffering, so frames that straddle chunk boundaries reassemble
//// correctly without callers writing a state machine.
////
//// Cross-target: `binary.length_prefixed_with` and
//// `fold.collect_result` run on both the Erlang and JavaScript
//// targets.

import datastream/binary
import datastream/fold
import datastream/source
import gleeunit/should

pub fn main() -> Nil {
  Nil
}

/// Run the example pipeline against an in-memory chunked source and
/// return the decoded frames. The fixture intentionally splits the
/// second frame across two chunks to exercise the buffer-stitching
/// path.
pub fn run() -> Result(List(BitArray), binary.IncompleteFrame) {
  let chunks =
    source.from_list([
      // Frame 1: length 5, payload "hello".
      <<5:32, "hello">>,
      // Frame 2: length 5, payload "world" — split so the last two
      // bytes arrive in the next chunk.
      <<5:32, "wor">>,
      <<"ld">>,
      // Frame 3: length 2, payload <<1, 2>>.
      <<2:32, 1, 2>>,
    ])

  // `length_prefixed_with` rejects any prefix that claims more than
  // `max_frame_size` bytes immediately, before any payload
  // buffering — the right shape when the input is adversarial.
  chunks
  |> binary.length_prefixed_with(prefix_size: 4, max_frame_size: 4096)
  |> fold.collect_result
}

pub fn length_prefixed_pipeline_example_test() {
  // `fold.collect_result` short-circuits on the first
  // `Error(IncompleteFrame)`. A well-formed input yields `Ok(frames)`
  // with one element per frame.
  run() |> should.equal(Ok([<<"hello">>, <<"world">>, <<1, 2>>]))
}

pub fn length_prefixed_pipeline_truncated_input_test() {
  // A truncated trailing frame surfaces as exactly one
  // `Error(IncompleteFrame(expected:, got:))` element before the
  // stream halts. Pair with `fold.collect_result` for a Result-shaped
  // terminal that stops at the first truncation.
  let truncated =
    source.from_list([
      <<3:32, "ok!">>,
      // Prefix declares 99 payload bytes but only 5 follow.
      <<99:32, "short">>,
    ])

  truncated
  |> binary.length_prefixed_with(prefix_size: 4, max_frame_size: 4096)
  |> fold.collect_result
  |> should.equal(Error(binary.IncompleteFrame(expected: 99, got: 5)))
}
