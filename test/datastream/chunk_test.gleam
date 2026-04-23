import datastream/chunk
import gleeunit
import gleeunit/should

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn empty_to_list_is_empty_test() {
  chunk.empty() |> chunk.to_list |> should.equal([])
}

pub fn empty_size_is_zero_test() {
  chunk.empty() |> chunk.size |> should.equal(0)
}

pub fn empty_is_empty_test() {
  chunk.empty() |> chunk.is_empty |> should.equal(True)
}

pub fn singleton_to_list_yields_one_element_test() {
  chunk.singleton(7) |> chunk.to_list |> should.equal([7])
}

pub fn singleton_size_is_one_test() {
  chunk.singleton(7) |> chunk.size |> should.equal(1)
}

pub fn singleton_is_not_empty_test() {
  chunk.singleton(7) |> chunk.is_empty |> should.equal(False)
}

pub fn from_list_to_list_round_trip_test() {
  chunk.from_list([1, 2, 3]) |> chunk.to_list |> should.equal([1, 2, 3])
}

pub fn from_list_size_matches_input_test() {
  chunk.from_list([1, 2, 3]) |> chunk.size |> should.equal(3)
}

pub fn from_list_empty_is_empty_test() {
  chunk.from_list([]) |> chunk.is_empty |> should.equal(True)
}

pub fn map_transforms_each_element_test() {
  chunk.from_list([1, 2, 3])
  |> chunk.map(with: fn(x) { x * 2 })
  |> chunk.to_list
  |> should.equal([2, 4, 6])
}

pub fn map_on_empty_returns_empty_test() {
  chunk.from_list([])
  |> chunk.map(with: fn(x) { x * 2 })
  |> chunk.to_list
  |> should.equal([])
}

pub fn map_preserves_size_test() {
  chunk.from_list([1, 2, 3, 4])
  |> chunk.map(with: fn(x) { x * 10 })
  |> chunk.size
  |> should.equal(4)
}

pub fn filter_keeps_matching_elements_test() {
  chunk.from_list([1, 2, 3, 4])
  |> chunk.filter(keeping: fn(x) { x > 2 })
  |> chunk.to_list
  |> should.equal([3, 4])
}

pub fn filter_with_constant_false_yields_empty_test() {
  chunk.from_list([1, 2, 3])
  |> chunk.filter(keeping: fn(_) { False })
  |> chunk.to_list
  |> should.equal([])
}

pub fn concat_walks_chunks_in_list_order_test() {
  chunk.concat([
    chunk.from_list([1]),
    chunk.from_list([2, 3]),
    chunk.from_list([4]),
  ])
  |> chunk.to_list
  |> should.equal([1, 2, 3, 4])
}

pub fn concat_of_empty_list_yields_empty_test() {
  chunk.concat([]) |> chunk.to_list |> should.equal([])
}

pub fn concat_skips_empty_inner_chunks_test() {
  chunk.concat([chunk.empty(), chunk.singleton(1), chunk.empty()])
  |> chunk.to_list
  |> should.equal([1])
}

pub fn from_list_to_list_round_trip_property_empty_test() {
  let xs: List(Int) = []
  chunk.to_list(chunk.from_list(xs)) |> should.equal(xs)
}

pub fn from_list_to_list_round_trip_property_singleton_test() {
  let xs = [42]
  chunk.to_list(chunk.from_list(xs)) |> should.equal(xs)
}

pub fn from_list_to_list_round_trip_property_strings_test() {
  let xs = ["a", "b", "c", "d"]
  chunk.to_list(chunk.from_list(xs)) |> should.equal(xs)
}
