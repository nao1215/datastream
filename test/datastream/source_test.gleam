import datastream.{Done, Next}
import gleam/dict
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleeunit
import gleeunit/should

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn empty_yields_no_elements_test() {
  datastream.empty() |> datastream.to_list |> should.equal([])
}

pub fn once_yields_exactly_one_element_test() {
  datastream.once(7) |> datastream.to_list |> should.equal([7])
}

pub fn from_list_on_empty_yields_empty_test() {
  datastream.from_list([]) |> datastream.to_list |> should.equal([])
}

pub fn from_list_preserves_source_order_test() {
  datastream.from_list([1, 2, 3])
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn range_counts_up_when_start_less_than_stop_test() {
  datastream.range(from: 0, to: 5)
  |> datastream.to_list
  |> should.equal([0, 1, 2, 3, 4])
}

pub fn range_counts_down_when_start_greater_than_stop_test() {
  datastream.range(from: 5, to: 0)
  |> datastream.to_list
  |> should.equal([5, 4, 3, 2, 1])
}

// Regression pin for the documented "from > to auto-reverses" surprise.
pub fn range_descending_silently_when_args_swapped_test() {
  datastream.range(from: 5, to: 1)
  |> datastream.to_list
  |> should.equal([5, 4, 3, 2])
}

pub fn range_is_empty_when_start_equals_stop_test() {
  datastream.range(from: 3, to: 3) |> datastream.to_list |> should.equal([])
}

pub fn range_strict_yields_ascending_when_start_less_than_stop_test() {
  let assert Ok(s) = datastream.range_strict(from: 0, to: 5)
  s |> datastream.to_list |> should.equal([0, 1, 2, 3, 4])
}

pub fn range_strict_yields_empty_when_start_equals_stop_test() {
  let assert Ok(s) = datastream.range_strict(from: 3, to: 3)
  s |> datastream.to_list |> should.equal([])
}

pub fn range_strict_errors_when_start_greater_than_stop_test() {
  datastream.range_strict(from: 5, to: 1)
  |> should.equal(Error(datastream.DescendingNotAllowed))
}

pub fn range_descending_yields_descending_when_start_greater_than_stop_test() {
  let assert Ok(s) = datastream.range_descending(from: 5, to: 0)
  s |> datastream.to_list |> should.equal([5, 4, 3, 2, 1])
}

pub fn range_descending_yields_empty_when_start_equals_stop_test() {
  let assert Ok(s) = datastream.range_descending(from: 3, to: 3)
  s |> datastream.to_list |> should.equal([])
}

pub fn range_descending_errors_when_start_less_than_stop_test() {
  datastream.range_descending(from: 1, to: 5)
  |> should.equal(Error(datastream.AscendingNotAllowed))
}

pub fn repeat_with_take_yields_n_copies_test() {
  datastream.repeat(0)
  |> datastream.take(up_to: 3)
  |> datastream.to_list
  |> should.equal([0, 0, 0])
}

pub fn iterate_with_take_yields_seeded_sequence_test() {
  datastream.iterate(from: 1, with: fn(x) { x * 2 })
  |> datastream.take(up_to: 4)
  |> datastream.to_list
  |> should.equal([1, 2, 4, 8])
}

pub fn unfold_halts_on_done_test() {
  datastream.unfold(from: 0, with: fn(s) {
    case s < 3 {
      True -> Next(element: s, state: s + 1)
      False -> Done
    }
  })
  |> datastream.to_list
  |> should.equal([0, 1, 2])
}

pub fn unfold_immediate_done_yields_empty_test() {
  datastream.unfold(from: 0, with: fn(_) { Done })
  |> datastream.to_list
  |> should.equal([])
}

pub fn from_list_is_repeatable_test() {
  let s = datastream.from_list([1, 2, 3])
  datastream.to_list(s) |> should.equal([1, 2, 3])
  datastream.to_list(s) |> should.equal([1, 2, 3])
}

pub fn range_is_repeatable_test() {
  let s = datastream.range(from: 0, to: 3)
  datastream.to_list(s) |> should.equal([0, 1, 2])
  datastream.to_list(s) |> should.equal([0, 1, 2])
}

pub fn empty_is_repeatable_test() {
  let s = datastream.empty()
  datastream.to_list(s) |> should.equal([])
  datastream.to_list(s) |> should.equal([])
}

pub fn once_is_repeatable_test() {
  let s = datastream.once("x")
  datastream.to_list(s) |> should.equal(["x"])
  datastream.to_list(s) |> should.equal(["x"])
}

pub fn repeat_with_take_is_repeatable_test() {
  let s = datastream.repeat(9) |> datastream.take(up_to: 2)
  datastream.to_list(s) |> should.equal([9, 9])
  datastream.to_list(s) |> should.equal([9, 9])
}

pub fn iterate_with_take_is_repeatable_test() {
  let s =
    datastream.iterate(from: 0, with: fn(x) { x + 1 })
    |> datastream.take(up_to: 4)
  datastream.to_list(s) |> should.equal([0, 1, 2, 3])
  datastream.to_list(s) |> should.equal([0, 1, 2, 3])
}

pub fn unfold_is_repeatable_test() {
  let s =
    datastream.unfold(from: 0, with: fn(s) {
      case s < 2 {
        True -> Next(element: s, state: s + 1)
        False -> Done
      }
    })
  datastream.to_list(s) |> should.equal([0, 1])
  datastream.to_list(s) |> should.equal([0, 1])
}

pub fn from_option_some_yields_one_element_test() {
  datastream.from_option(Some(5)) |> datastream.to_list |> should.equal([5])
}

pub fn from_option_none_yields_empty_test() {
  datastream.from_option(None) |> datastream.to_list |> should.equal([])
}

pub fn from_result_ok_yields_one_ok_test() {
  datastream.once(Ok(7)) |> datastream.to_list |> should.equal([Ok(7)])
}

pub fn from_result_error_yields_one_error_test() {
  datastream.once(Error("nope"))
  |> datastream.to_list
  |> should.equal([Error("nope")])
}

pub fn from_dict_empty_yields_empty_test() {
  datastream.from_dict(dict.from_list([]))
  |> datastream.to_list
  |> should.equal([])
}

pub fn from_dict_yields_each_entry_once_test() {
  datastream.from_dict(dict.from_list([#("a", 1), #("b", 2)]))
  |> datastream.to_list
  |> list.sort(by: fn(left, right) { string.compare(left.0, right.0) })
  |> should.equal([#("a", 1), #("b", 2)])
}

pub fn from_bit_array_empty_yields_empty_test() {
  datastream.from_bit_array(<<>>)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([])
}

pub fn from_bit_array_yields_each_byte_test() {
  datastream.from_bit_array(<<1, 2, 255>>)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([1, 2, 255])
}

pub fn from_bit_array_preserves_zero_bytes_test() {
  datastream.from_bit_array(<<0, 0, 0>>)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([0, 0, 0])
}

pub fn from_option_is_repeatable_test() {
  let s = datastream.from_option(Some(42))
  datastream.to_list(s) |> should.equal([42])
  datastream.to_list(s) |> should.equal([42])
}

pub fn from_result_is_repeatable_test() {
  let s = datastream.once(Ok("v"))
  datastream.to_list(s) |> should.equal([Ok("v")])
  datastream.to_list(s) |> should.equal([Ok("v")])
}

pub fn from_bit_array_is_repeatable_test() {
  let s = datastream.from_bit_array(<<10, 20>>) |> should.be_ok
  datastream.to_list(s) |> should.equal([10, 20])
  datastream.to_list(s) |> should.equal([10, 20])
}

pub fn from_bit_array_rejects_sub_byte_input_test() {
  assert datastream.from_bit_array(<<7:size(3)>>)
    |> should.be_error
    == datastream.UnalignedArgument(argument: "bytes", bit_size: 3)
}

pub fn from_bit_array_rejects_partial_trailing_bits_test() {
  assert datastream.from_bit_array(<<5:size(11)>>)
    |> should.be_error
    == datastream.UnalignedArgument(argument: "bytes", bit_size: 11)
}
