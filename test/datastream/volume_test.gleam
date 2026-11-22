//// High-cardinality smoke tests for the cross-target core.
////
//// These verify that the library actually streams — meaning a lazy
//// pipeline across hundreds of thousands of elements completes
//// without retaining the full input. If a combinator regressed to
//// materialising its source into a list, these tests would either
//// time out or exhaust memory; the assertion is therefore completion
//// plus final cardinality, not a direct memory measurement.

import datastream/binary
import datastream/fold
import datastream/source
import datastream/stream
import datastream/text
import gleam/bit_array
import gleam/list
import gleam/string
import gleeunit
import gleeunit/should

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn range_map_count_one_million_test() {
  source.range(from: 0, to: 1_000_000)
  |> stream.map(with: fn(x) { x + 1 })
  |> fold.count
  |> should.equal(1_000_000)
}

pub fn chunks_of_on_one_hundred_thousand_test() {
  source.range(from: 0, to: 100_000)
  |> stream.chunks_of(into: 100)
  |> fold.count
  |> should.equal(1000)
}

pub fn text_lines_on_one_hundred_thousand_chunks_test() {
  source.repeat("line\n")
  |> stream.take(up_to: 100_000)
  |> text.lines
  |> fold.count
  |> should.equal(100_000)
}

pub fn binary_fixed_size_on_one_hundred_thousand_chunks_test() {
  source.repeat(<<1, 2, 3, 4>>)
  |> stream.take(up_to: 100_000)
  |> binary.fixed_size(size: 4)
  |> fold.count
  |> should.equal(100_000)
}

pub fn buffer_with_large_prefetch_is_linear_test() {
  source.range(from: 0, to: 100_000)
  |> stream.buffer(prefetch: 50_000)
  |> fold.count
  |> should.equal(100_000)
}

pub fn flat_map_over_many_empty_inners_does_not_grow_the_stack_test() {
  source.range(from: 0, to: 100_000)
  |> stream.flat_map(with: fn(_) { source.empty() })
  |> fold.count
  |> should.equal(0)
}

pub fn group_adjacent_on_one_long_run_does_not_grow_the_stack_test() {
  source.repeat(1)
  |> stream.take(up_to: 1_000_000)
  |> stream.group_adjacent(by: fn(x) { x })
  |> fold.count
  |> should.equal(1)
}

pub fn text_lines_on_many_tiny_chunks_without_newline_test() {
  source.repeat("a")
  |> stream.take(up_to: 1_000_000)
  |> text.lines
  |> fold.count
  |> should.equal(1)
}

pub fn text_lines_on_one_large_chunk_is_linear_test() {
  let line = string.repeat("x", times: 99) <> "\n"
  source.once(string.repeat(line, times: 4000))
  |> text.lines
  |> fold.count
  |> should.equal(4000)
}

fn one_byte_chunks(count: Int) {
  source.repeat(<<7>>) |> stream.take(up_to: count)
}

pub fn length_prefixed_frame_from_one_byte_chunks_is_linear_test() {
  // A 1 MB frame whose bytes arrive one per chunk.
  source.from_list([<<0>>, <<15>>, <<66>>, <<64>>])
  |> stream.append(one_byte_chunks(1_000_000))
  |> binary.length_prefixed(prefix_size: 4)
  |> fold.to_list
  |> list.map(fn(frame) {
    case frame {
      Ok(bytes) -> bit_array.byte_size(bytes)
      Error(_) -> -1
    }
  })
  |> should.equal([1_000_000])
}

pub fn fixed_size_frame_from_one_byte_chunks_is_linear_test() {
  one_byte_chunks(1_000_000)
  |> binary.fixed_size(size: 1_000_000)
  |> fold.count
  |> should.equal(1)
}

pub fn delimited_frame_from_one_byte_chunks_is_linear_test() {
  one_byte_chunks(1_000_000)
  |> binary.delimited(on: <<0, 0>>)
  |> stream.map(bit_array.byte_size)
  |> fold.to_list
  |> should.equal([1_000_000])
}

pub fn utf8_decode_of_one_large_chunk_is_linear_test() {
  source.once(bit_array.from_string(string.repeat("aé", times: 400_000)))
  |> text.utf8_decode
  |> fold.count
  |> should.equal(1)
}

pub fn text_split_without_delimiter_over_many_chunks_is_linear_test() {
  source.repeat("0123456789")
  |> stream.take(up_to: 200_000)
  |> text.split(on: ",")
  |> stream.map(string.byte_size)
  |> fold.to_list
  |> should.equal([2_000_000])
}
