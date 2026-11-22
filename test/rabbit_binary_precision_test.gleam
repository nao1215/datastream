import datastream.{type Stream, Done, FrameTooLarge, IncompleteFrame, Next}
import datastream/internal/ref
import gleam/list

fn recorded(
  chunks: List(BitArray),
  history: ref.Ref(List(Int)),
) -> Stream(BitArray) {
  datastream.resource(
    open: fn() { #(0, chunks) },
    next: fn(state) {
      let #(index, chunks) = state
      let assert Ok(events) = ref.get(history)
      ref.set(history, [index, ..events])
      case chunks {
        [] -> Done
        [chunk, ..rest] -> Next(chunk, #(index + 1, rest))
      }
    },
    close: fn(state) {
      let assert Ok(events) = ref.get(history)
      ref.set(history, [-state.0 - 1, ..events])
    },
  )
}

pub fn eight_byte_length_above_representable_cap_stops_at_prefix_test() {
  let events = ref.new([])
  let prefix = <<0, 32, 0, 0, 0, 0, 0, 1>>
  let limit = 4_294_967_296 * 2_097_152
  let assert Ok(frames) =
    datastream.length_prefixed(
      recorded([prefix, <<7>>], events),
      prefix_size: 8,
      max_frame_size: limit,
    )
  let output = datastream.first(frames)
  let assert Ok(history) = ref.get(events)
  assert list.reverse(history) == [0, -2]
  let assert Ok(Error(FrameTooLarge(_, max))) = output
  assert max == limit
  assert ref.get(events) == Ok(history)
  ref.delete(events)
}

pub fn oversized_eight_byte_prefix_preserves_exact_low_word_test() {
  let prefix = <<0, 32, 0, 0, 0, 0, 0, 1>>
  let assert Ok(frames) =
    datastream.length_prefixed(
      datastream.once(prefix),
      prefix_size: 8,
      max_frame_size: 0,
    )
  let assert [Error(FrameTooLarge(claimed, 0))] = datastream.to_list(frames)
  assert claimed == #(2_097_152, 1)
}

pub fn accepted_large_prefix_preserves_exact_expected_length_at_eof_test() {
  let prefix = <<0, 32, 0, 0, 0, 0, 0, 1>>
  let assert Ok(frames) =
    datastream.length_prefixed(
      datastream.once(prefix),
      prefix_size: 8,
      max_frame_size: 4_294_967_296 * 4_194_304,
    )
  let assert [Error(IncompleteFrame(expected, 0))] = datastream.to_list(frames)
  assert expected == #(2_097_152, 1)
}

pub fn eight_byte_small_frames_keep_zero_and_split_payloads_test() {
  let chunks = [
    <<0, 0, 0, 0>>,
    <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, "a">>,
    <<"bc">>,
  ]
  let assert Ok(frames) =
    datastream.length_prefixed(
      datastream.from_list(chunks),
      prefix_size: 8,
      max_frame_size: 3,
    )
  assert datastream.to_list(frames) == [Ok(<<>>), Ok(<<"abc">>)]
}

pub fn max_unsigned_64_bit_payload_is_accepted_under_two_to_64_cap_test() {
  let maximum = <<255, 255, 255, 255, 255, 255, 255, 255>>
  let assert Ok(frames) =
    datastream.length_prefixed(
      datastream.once(maximum),
      prefix_size: 8,
      max_frame_size: 4_294_967_296 * 4_294_967_296,
    )
  let assert [Error(IncompleteFrame(expected, 0))] = datastream.to_list(frames)
  assert expected == #(4_294_967_295, 4_294_967_295)
}

pub fn max_unsigned_64_bit_prefix_above_exact_cap_reports_both_words_test() {
  let maximum = <<255, 255, 255, 255, 255, 255, 255, 255>>
  let limit = 4_294_967_296 * 4_294_967_295
  let events = ref.new([])
  let assert Ok(frames) =
    datastream.length_prefixed(
      recorded([maximum, <<7>>], events),
      prefix_size: 8,
      max_frame_size: limit,
    )
  let assert [Error(FrameTooLarge(claimed, max))] = datastream.to_list(frames)
  assert claimed == #(4_294_967_295, 4_294_967_295)
  assert max == limit
  assert ref.get(events) == Ok([-2, 0])
  ref.delete(events)
}

pub fn exact_two_to_53_prefix_is_not_rejected_at_equal_limit_test() {
  let prefix = <<0, 32, 0, 0, 0, 0, 0, 0>>
  let assert Ok(frames) =
    datastream.length_prefixed(
      datastream.once(prefix),
      prefix_size: 8,
      max_frame_size: 4_294_967_296 * 2_097_152,
    )
  let assert [Error(IncompleteFrame(expected, 0))] = datastream.to_list(frames)
  assert expected == #(2_097_152, 0)
}

pub fn short_prefix_reports_its_byte_requirement_test() {
  let assert Ok(frames) =
    datastream.length_prefixed(
      datastream.once(<<0, 0, 0>>),
      prefix_size: 8,
      max_frame_size: 3,
    )
  let assert [Error(IncompleteFrame(expected, 3))] = datastream.to_list(frames)
  assert expected == #(0, 8)
}
