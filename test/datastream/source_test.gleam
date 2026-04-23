import datastream.{Done, Next}
import datastream/fold
import datastream/source
import datastream/stream
import gleeunit
import gleeunit/should

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
