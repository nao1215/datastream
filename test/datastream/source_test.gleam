import datastream.{Done, Next}
import datastream/fold
import datastream/source
import datastream/stream
import gleam/dict
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleeunit
import gleeunit/should

@target(erlang)
import gleam/erlang/process

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn empty_yields_no_elements_test() {
  source.empty() |> fold.to_list |> should.equal([])
}

pub fn once_yields_exactly_one_element_test() {
  source.once(7) |> fold.to_list |> should.equal([7])
}

pub fn from_list_on_empty_yields_empty_test() {
  source.from_list([]) |> fold.to_list |> should.equal([])
}

pub fn from_list_preserves_source_order_test() {
  source.from_list([1, 2, 3]) |> fold.to_list |> should.equal([1, 2, 3])
}

pub fn range_counts_up_when_start_less_than_stop_test() {
  source.range(from: 0, to: 5) |> fold.to_list |> should.equal([0, 1, 2, 3, 4])
}

pub fn range_counts_down_when_start_greater_than_stop_test() {
  source.range(from: 5, to: 0) |> fold.to_list |> should.equal([5, 4, 3, 2, 1])
}

pub fn range_is_empty_when_start_equals_stop_test() {
  source.range(from: 3, to: 3) |> fold.to_list |> should.equal([])
}

pub fn repeat_with_take_yields_n_copies_test() {
  source.repeat(0)
  |> stream.take(up_to: 3)
  |> fold.to_list
  |> should.equal([0, 0, 0])
}

pub fn iterate_with_take_yields_seeded_sequence_test() {
  source.iterate(from: 1, with: fn(x) { x * 2 })
  |> stream.take(up_to: 4)
  |> fold.to_list
  |> should.equal([1, 2, 4, 8])
}

pub fn unfold_halts_on_done_test() {
  source.unfold(from: 0, with: fn(s) {
    case s < 3 {
      True -> Next(element: s, state: s + 1)
      False -> Done
    }
  })
  |> fold.to_list
  |> should.equal([0, 1, 2])
}

pub fn unfold_immediate_done_yields_empty_test() {
  source.unfold(from: 0, with: fn(_) { Done })
  |> fold.to_list
  |> should.equal([])
}

pub fn from_list_is_repeatable_test() {
  let s = source.from_list([1, 2, 3])
  fold.to_list(s) |> should.equal([1, 2, 3])
  fold.to_list(s) |> should.equal([1, 2, 3])
}

pub fn range_is_repeatable_test() {
  let s = source.range(from: 0, to: 3)
  fold.to_list(s) |> should.equal([0, 1, 2])
  fold.to_list(s) |> should.equal([0, 1, 2])
}

pub fn empty_is_repeatable_test() {
  let s = source.empty()
  fold.to_list(s) |> should.equal([])
  fold.to_list(s) |> should.equal([])
}

pub fn once_is_repeatable_test() {
  let s = source.once("x")
  fold.to_list(s) |> should.equal(["x"])
  fold.to_list(s) |> should.equal(["x"])
}

pub fn repeat_with_take_is_repeatable_test() {
  let s = source.repeat(9) |> stream.take(up_to: 2)
  fold.to_list(s) |> should.equal([9, 9])
  fold.to_list(s) |> should.equal([9, 9])
}

pub fn iterate_with_take_is_repeatable_test() {
  let s =
    source.iterate(from: 0, with: fn(x) { x + 1 }) |> stream.take(up_to: 4)
  fold.to_list(s) |> should.equal([0, 1, 2, 3])
  fold.to_list(s) |> should.equal([0, 1, 2, 3])
}

pub fn unfold_is_repeatable_test() {
  let s =
    source.unfold(from: 0, with: fn(s) {
      case s < 2 {
        True -> Next(element: s, state: s + 1)
        False -> Done
      }
    })
  fold.to_list(s) |> should.equal([0, 1])
  fold.to_list(s) |> should.equal([0, 1])
}

pub fn from_option_some_yields_one_element_test() {
  source.from_option(Some(5)) |> fold.to_list |> should.equal([5])
}

pub fn from_option_none_yields_empty_test() {
  source.from_option(None) |> fold.to_list |> should.equal([])
}

pub fn from_result_ok_yields_one_ok_test() {
  source.from_result(Ok(7)) |> fold.to_list |> should.equal([Ok(7)])
}

pub fn from_result_error_yields_one_error_test() {
  source.from_result(Error("nope"))
  |> fold.to_list
  |> should.equal([Error("nope")])
}

pub fn from_dict_empty_yields_empty_test() {
  source.from_dict(dict.from_list([])) |> fold.to_list |> should.equal([])
}

pub fn from_dict_yields_each_entry_once_test() {
  source.from_dict(dict.from_list([#("a", 1), #("b", 2)]))
  |> fold.to_list
  |> list.sort(by: fn(left, right) { string.compare(left.0, right.0) })
  |> should.equal([#("a", 1), #("b", 2)])
}

pub fn from_bit_array_empty_yields_empty_test() {
  source.from_bit_array(<<>>) |> fold.to_list |> should.equal([])
}

pub fn from_bit_array_yields_each_byte_test() {
  source.from_bit_array(<<1, 2, 255>>)
  |> fold.to_list
  |> should.equal([1, 2, 255])
}

pub fn from_bit_array_preserves_zero_bytes_test() {
  source.from_bit_array(<<0, 0, 0>>)
  |> fold.to_list
  |> should.equal([0, 0, 0])
}

pub fn from_option_is_repeatable_test() {
  let s = source.from_option(Some(42))
  fold.to_list(s) |> should.equal([42])
  fold.to_list(s) |> should.equal([42])
}

pub fn from_result_is_repeatable_test() {
  let s = source.from_result(Ok("v"))
  fold.to_list(s) |> should.equal([Ok("v")])
  fold.to_list(s) |> should.equal([Ok("v")])
}

pub fn from_bit_array_is_repeatable_test() {
  let s = source.from_bit_array(<<10, 20>>)
  fold.to_list(s) |> should.equal([10, 20])
  fold.to_list(s) |> should.equal([10, 20])
}

// --- construction-time panics (Erlang target) ----------------------------
//
// `from_bit_array` rejects non-byte-aligned input at construction time
// with a panic per #144. These tests pin that contract so a future
// refactor of the validation path has to update them deliberately.
// Erlang-only because the panic-detection helper spawns a process;
// JavaScript has no matching primitive. The validation logic itself is
// cross-target.

@target(erlang)
fn panicked(thunk: fn() -> Nil) -> Bool {
  let done = process.new_subject()
  let _pid =
    process.spawn_unlinked(fn() {
      thunk()
      process.send(done, Nil)
    })
  case process.receive(from: done, within: 100) {
    Ok(_) -> False
    Error(_) -> True
  }
}

@target(erlang)
pub fn from_bit_array_panics_on_sub_byte_input_test() {
  let did_panic =
    panicked(fn() {
      let _result = source.from_bit_array(<<7:size(3)>>)
      Nil
    })
  did_panic |> should.be_true
}

@target(erlang)
pub fn from_bit_array_panics_on_partial_trailing_bits_test() {
  let did_panic =
    panicked(fn() {
      let _result = source.from_bit_array(<<5:size(11)>>)
      Nil
    })
  did_panic |> should.be_true
}
