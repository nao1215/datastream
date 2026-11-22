//// High-cardinality smoke tests for the cross-target core.
////
//// These verify that the library actually streams — meaning a lazy
//// pipeline across hundreds of thousands of elements completes
//// without retaining the full input. If a combinator regressed to
//// materialising its source into a list, these tests would either
//// time out or exhaust memory; the assertion is therefore completion
//// plus final cardinality, not a direct memory measurement.

import datastream

import gleam/bit_array
import gleam/list
import gleam/string
import gleeunit
import gleeunit/should

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn range_map_count_one_million_test() {
  datastream.range(from: 0, to: 1_000_000)
  |> datastream.map(with: fn(x) { x + 1 })
  |> datastream.count
  |> should.equal(1_000_000)
}

pub fn chunks_of_on_one_hundred_thousand_test() {
  datastream.range(from: 0, to: 100_000)
  |> datastream.chunks_of(into: 100)
  |> should.be_ok
  |> datastream.count
  |> should.equal(1000)
}

pub fn text_lines_on_one_hundred_thousand_chunks_test() {
  datastream.repeat("line\n")
  |> datastream.take(up_to: 100_000)
  |> datastream.lines
  |> datastream.count
  |> should.equal(100_000)
}

pub fn binary_fixed_size_on_one_hundred_thousand_chunks_test() {
  datastream.repeat(<<1, 2, 3, 4>>)
  |> datastream.take(up_to: 100_000)
  |> datastream.fixed_size(size: 4)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.count
  |> should.equal(100_000)
}

pub fn buffer_with_large_prefetch_is_linear_test() {
  datastream.range(from: 0, to: 100_000)
  |> datastream.buffer(prefetch: 50_000)
  |> should.be_ok
  |> datastream.count
  |> should.equal(100_000)
}

pub fn flat_map_over_many_empty_inners_does_not_grow_the_stack_test() {
  datastream.range(from: 0, to: 100_000)
  |> datastream.flat_map(with: fn(_) { datastream.empty() })
  |> datastream.count
  |> should.equal(0)
}

pub fn group_adjacent_on_one_long_run_does_not_grow_the_stack_test() {
  datastream.repeat(1)
  |> datastream.take(up_to: 1_000_000)
  |> datastream.group_adjacent(by: fn(x) { x })
  |> datastream.count
  |> should.equal(1)
}

pub fn text_lines_on_many_tiny_chunks_without_newline_test() {
  datastream.repeat("a")
  |> datastream.take(up_to: 1_000_000)
  |> datastream.lines
  |> datastream.count
  |> should.equal(1)
}

pub fn text_lines_on_one_large_chunk_is_linear_test() {
  let line = string.repeat("x", times: 99) <> "\n"
  datastream.once(string.repeat(line, times: 4000))
  |> datastream.lines
  |> datastream.count
  |> should.equal(4000)
}

fn one_byte_chunks(count: Int) {
  datastream.repeat(<<7>>) |> datastream.take(up_to: count)
}

pub fn length_prefixed_frame_from_one_byte_chunks_is_linear_test() {
  // A 1 MB frame whose bytes arrive one per chunk.
  datastream.from_list([<<0>>, <<15>>, <<66>>, <<64>>])
  |> datastream.append(one_byte_chunks(1_000_000))
  |> datastream.length_prefixed(prefix_size: 4, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
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
  |> datastream.fixed_size(size: 1_000_000)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.count
  |> should.equal(1)
}

pub fn delimited_frame_from_one_byte_chunks_is_linear_test() {
  one_byte_chunks(1_000_000)
  |> datastream.delimited(on: <<0, 0>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.map(bit_array.byte_size)
  |> datastream.to_list
  |> should.equal([1_000_000])
}

pub fn utf8_decode_of_one_large_chunk_is_linear_test() {
  datastream.once(bit_array.from_string(string.repeat("aé", times: 400_000)))
  |> datastream.utf8_decode
  |> datastream.count
  |> should.equal(1)
}

pub fn text_split_without_delimiter_over_many_chunks_is_linear_test() {
  datastream.repeat("0123456789")
  |> datastream.take(up_to: 200_000)
  |> datastream.split(on: ",")
  |> datastream.map(string.byte_size)
  |> datastream.to_list
  |> should.equal([2_000_000])
}

pub fn broadcast_lagging_consumer_queue_is_linear_test() {
  let assert [fast, slow] =
    datastream.broadcast(over: datastream.range(from: 0, to: 100_000), into: 2)
  datastream.count(fast) |> should.equal(100_000)
  datastream.count(slow) |> should.equal(100_000)
}

pub fn broadcast_bounded_large_queue_is_linear_test() {
  let assert [fast, slow] =
    datastream.broadcast_bounded(
      over: datastream.range(from: 0, to: 100_000),
      into: 2,
      max_queue: 100_000,
    )
    |> should.be_ok
  datastream.count(fast) |> should.equal(100_000)
  datastream.count(slow) |> should.equal(100_000)
}
