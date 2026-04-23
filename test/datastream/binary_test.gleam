import datastream.{Done, Next}
import datastream/binary
import datastream/fold
import gleam/list
import gleeunit
import gleeunit/should

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

// --- bytes ----------------------------------------------------------------

pub fn bytes_yields_each_byte_in_order_test() {
  from_list([<<1, 2>>, <<3>>])
  |> binary.bytes
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

pub fn bytes_empty_chunk_yields_no_bytes_test() {
  from_list([<<>>]) |> binary.bytes |> fold.to_list |> should.equal([])
}

pub fn bytes_empty_source_yields_empty_test() {
  from_list([]) |> binary.bytes |> fold.to_list |> should.equal([])
}

pub fn bytes_preserves_full_byte_range_test() {
  from_list([<<255, 0, 128>>])
  |> binary.bytes
  |> fold.to_list
  |> should.equal([255, 0, 128])
}

// --- length_prefixed -----------------------------------------------------

pub fn length_prefixed_one_byte_prefix_two_frames_test() {
  from_list([<<2, 65, 66, 1, 67>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([<<65, 66>>, <<67>>])
}

pub fn length_prefixed_chunk_boundary_inside_payload_test() {
  from_list([<<2, 65>>, <<66>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([<<65, 66>>])
}

pub fn length_prefixed_chunk_boundary_between_prefix_and_payload_test() {
  from_list([<<2>>, <<65, 66>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([<<65, 66>>])
}

pub fn length_prefixed_zero_length_frame_test() {
  from_list([<<0>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([<<>>])
}

pub fn length_prefixed_incomplete_payload_drops_frame_test() {
  from_list([<<2, 65>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([])
}

pub fn length_prefixed_incomplete_prefix_drops_frame_test() {
  from_list([<<2>>])
  |> binary.length_prefixed(prefix_size: 2)
  |> fold.to_list
  |> should.equal([])
}

pub fn length_prefixed_four_byte_big_endian_prefix_test() {
  from_list([<<0, 0, 0, 1, 99>>])
  |> binary.length_prefixed(prefix_size: 4)
  |> fold.to_list
  |> should.equal([<<99>>])
}

pub fn length_prefixed_two_byte_prefix_decodes_big_endian_test() {
  // Length 257 = 0x0101 = <<1, 1>>; payload is 257 bytes.
  let payload = list.repeat(7, 257)
  let payload_bits = list.fold(payload, <<>>, fn(acc, b) { <<acc:bits, b>> })
  from_list([<<<<1, 1>>:bits, payload_bits:bits>>])
  |> binary.length_prefixed(prefix_size: 2)
  |> fold.to_list
  |> should.equal([payload_bits])
}

pub fn length_prefixed_eight_byte_prefix_small_frame_test() {
  from_list([<<0, 0, 0, 0, 0, 0, 0, 2, 65, 66>>])
  |> binary.length_prefixed(prefix_size: 8)
  |> fold.to_list
  |> should.equal([<<65, 66>>])
}

pub fn length_prefixed_prefix_split_across_chunks_test() {
  // prefix_size = 4, prefix bytes <<0, 0, 0, 1>> spans chunks
  // [<<0, 0>>, <<0, 1, 99>>]; payload is the trailing 0x63.
  from_list([<<0, 0>>, <<0, 1, 99>>])
  |> binary.length_prefixed(prefix_size: 4)
  |> fold.to_list
  |> should.equal([<<99>>])
}

pub fn length_prefixed_multiple_frames_back_to_back_two_byte_prefix_test() {
  // Three frames of length 1, 2, 3 each with a 2-byte big-endian
  // prefix, all in a single chunk.
  from_list([<<0, 1, 65, 0, 2, 66, 67, 0, 3, 68, 69, 70>>])
  |> binary.length_prefixed(prefix_size: 2)
  |> fold.to_list
  |> should.equal([<<65>>, <<66, 67>>, <<68, 69, 70>>])
}

// --- delimited ----------------------------------------------------------

pub fn delimited_yields_frames_separated_by_delimiter_test() {
  from_list([<<65, 66, 10, 67, 10>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65, 66>>, <<67>>, <<>>])
}

pub fn delimited_preserves_empty_frames_between_consecutive_delimiters_test() {
  from_list([<<65, 10, 10, 66, 10>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65>>, <<>>, <<66>>, <<>>])
}

pub fn delimited_chunk_boundary_after_delimiter_test() {
  from_list([<<65, 66, 10>>, <<67>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65, 66>>, <<67>>])
}

pub fn delimited_delimiter_at_start_of_next_chunk_test() {
  from_list([<<65>>, <<10, 66>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65>>, <<66>>])
}

pub fn delimited_trailing_partial_frame_preserved_test() {
  from_list([<<65, 10, 66>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65>>, <<66>>])
}

pub fn delimited_empty_input_yields_no_frames_test() {
  from_list([<<>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([])
}

// --- fixed_size --------------------------------------------------------

pub fn fixed_size_evenly_divisible_test() {
  from_list([<<1, 2, 3, 4>>])
  |> binary.fixed_size(size: 2)
  |> fold.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>])
}

pub fn fixed_size_drops_trailing_partial_test() {
  from_list([<<1, 2, 3, 4, 5>>])
  |> binary.fixed_size(size: 2)
  |> fold.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>])
}

pub fn fixed_size_frame_straddles_chunks_test() {
  from_list([<<1, 2, 3>>, <<4, 5, 6>>])
  |> binary.fixed_size(size: 2)
  |> fold.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>, <<5, 6>>])
}

pub fn fixed_size_empty_input_yields_no_frames_test() {
  from_list([<<>>])
  |> binary.fixed_size(size: 2)
  |> fold.to_list
  |> should.equal([])
}
