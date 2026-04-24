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
