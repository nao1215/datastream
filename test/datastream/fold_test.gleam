import datastream.{Done, Next}
import datastream/fold
import gleam/option.{None, Some}
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

fn repeat(value) {
  datastream.unfold(from: value, with: fn(s) { Next(element: s, state: s) })
}

pub fn to_list_returns_elements_in_source_order_test() {
  from_list([1, 2, 3]) |> fold.to_list |> should.equal([1, 2, 3])
}

pub fn to_list_on_empty_returns_empty_test() {
  from_list([]) |> fold.to_list |> should.equal([])
}

pub fn count_returns_number_of_elements_test() {
  from_list([1, 2, 3]) |> fold.count |> should.equal(3)
}

pub fn count_on_empty_returns_zero_test() {
  from_list([]) |> fold.count |> should.equal(0)
}

pub fn first_returns_some_singleton_test() {
  from_list([10]) |> fold.first |> should.equal(Some(10))
}

pub fn first_returns_some_head_of_many_test() {
  from_list([1, 2, 3]) |> fold.first |> should.equal(Some(1))
}

pub fn first_on_empty_returns_none_test() {
  from_list([]) |> fold.first |> should.equal(None)
}

pub fn first_terminates_on_infinite_stream_test() {
  repeat(7) |> fold.first |> should.equal(Some(7))
}

pub fn first_pulls_at_most_one_element_test() {
  let stream =
    datastream.unfold(from: 0, with: fn(s) {
      case s {
        0 -> Next(element: s, state: 1)
        _ -> panic as "first must not pull beyond the first Next"
      }
    })

  fold.first(stream) |> should.equal(Some(0))
}

pub fn last_returns_some_tail_test() {
  from_list([1, 2, 3]) |> fold.last |> should.equal(Some(3))
}

pub fn last_on_empty_returns_none_test() {
  from_list([]) |> fold.last |> should.equal(None)
}

pub fn fold_sums_ints_test() {
  from_list([1, 2, 3])
  |> fold.fold(from: 0, with: fn(acc, x) { acc + x })
  |> should.equal(6)
}

pub fn fold_returns_initial_on_empty_test() {
  from_list([])
  |> fold.fold(from: 10, with: fn(acc, x) { acc + x })
  |> should.equal(10)
}

pub fn fold_concatenates_strings_test() {
  from_list(["a", "b", "c"])
  |> fold.fold(from: "", with: fn(acc, x) { acc <> x })
  |> should.equal("abc")
}

pub fn reduce_seeds_with_head_then_folds_tail_test() {
  from_list([1, 2, 3])
  |> fold.reduce(with: fn(a, b) { a + b })
  |> should.equal(Some(6))
}

pub fn reduce_returns_some_only_on_singleton_test() {
  from_list([1])
  |> fold.reduce(with: fn(a, b) { a + b })
  |> should.equal(Some(1))
}

pub fn reduce_on_empty_returns_none_test() {
  from_list([])
  |> fold.reduce(with: fn(a, b) { a + b })
  |> should.equal(None)
}

pub fn drain_returns_nil_test() {
  from_list([1, 2, 3]) |> fold.drain |> should.equal(Nil)
}
