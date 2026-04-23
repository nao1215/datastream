//// BEAM-only tests for `datastream/erlang/par`.

@target(erlang)
import datastream/erlang/par

@target(erlang)
import datastream/fold

@target(erlang)
import datastream/source

@target(erlang)
import gleam/list

@target(erlang)
import gleam/order

@target(erlang)
import gleeunit/should

@target(javascript)
/// Empty placeholder so this module compiles cleanly on JavaScript.
pub const beam_only_marker: String = "datastream/erlang/par_test is BEAM-only"

// --- map_ordered ---------------------------------------------------------

@target(erlang)
pub fn map_ordered_preserves_input_order_test() {
  source.from_list([1, 2, 3, 4])
  |> par.map_ordered(with: fn(x) { x * 2 }, max_workers: 2, max_buffer: 4)
  |> fold.to_list
  |> should.equal([2, 4, 6, 8])
}

@target(erlang)
pub fn map_ordered_on_empty_yields_empty_test() {
  source.from_list([])
  |> par.map_ordered(with: fn(x) { x * 2 }, max_workers: 2, max_buffer: 4)
  |> fold.to_list
  |> should.equal([])
}

// --- map_unordered -------------------------------------------------------

@target(erlang)
pub fn map_unordered_yields_set_equal_to_inputs_test() {
  source.from_list([1, 2, 3, 4])
  |> par.map_unordered(with: fn(x) { x * 2 }, max_workers: 2, max_buffer: 4)
  |> fold.to_list
  |> list.sort(by: int_compare)
  |> should.equal([2, 4, 6, 8])
}

@target(erlang)
fn int_compare(a: Int, b: Int) -> order.Order {
  case a == b, a < b {
    True, _ -> order.Eq
    False, True -> order.Lt
    False, False -> order.Gt
  }
}

// --- each_* --------------------------------------------------------------

@target(erlang)
pub fn each_ordered_returns_nil_test() {
  source.from_list([1, 2, 3])
  |> par.each_ordered(with: fn(_) { Nil }, max_workers: 2, max_buffer: 4)
  |> should.equal(Nil)
}

@target(erlang)
pub fn each_unordered_returns_nil_test() {
  source.from_list([1, 2, 3])
  |> par.each_unordered(with: fn(_) { Nil }, max_workers: 2, max_buffer: 4)
  |> should.equal(Nil)
}

// --- merge ---------------------------------------------------------------

@target(erlang)
pub fn merge_interleaves_two_sources_test() {
  par.merge(
    streams: [source.from_list([1, 2]), source.from_list([3, 4])],
    max_buffer: 4,
  )
  |> fold.to_list
  |> list.sort(by: int_compare)
  |> should.equal([1, 2, 3, 4])
}

@target(erlang)
pub fn merge_empty_list_yields_empty_stream_test() {
  par.merge(streams: [], max_buffer: 4)
  |> fold.to_list
  |> should.equal([])
}

// --- race ----------------------------------------------------------------

@target(erlang)
pub fn race_empty_list_yields_empty_test() {
  par.race(streams: [])
  |> fold.to_list
  |> should.equal([])
}

@target(erlang)
pub fn race_single_source_passes_through_test() {
  par.race(streams: [source.from_list([1, 2, 3])])
  |> fold.to_list
  |> should.equal([1, 2, 3])
}
