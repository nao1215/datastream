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

pub fn all_true_when_predicate_holds_for_every_element_test() {
  from_list([1, 2, 3])
  |> fold.all(satisfying: fn(x) { x > 0 })
  |> should.equal(True)
}

pub fn all_false_on_first_failing_element_test() {
  from_list([1, -2, 3])
  |> fold.all(satisfying: fn(x) { x > 0 })
  |> should.equal(False)
}

pub fn all_true_on_empty_test() {
  from_list([])
  |> fold.all(satisfying: fn(x) { x > 0 })
  |> should.equal(True)
}

pub fn all_terminates_on_infinite_source_when_predicate_fails_test() {
  repeat(0)
  |> fold.all(satisfying: fn(x) { x > 0 })
  |> should.equal(False)
}

pub fn any_true_when_some_element_matches_test() {
  from_list([0, 0, 1])
  |> fold.any(satisfying: fn(x) { x > 0 })
  |> should.equal(True)
}

pub fn any_false_on_empty_test() {
  from_list([])
  |> fold.any(satisfying: fn(x) { x > 0 })
  |> should.equal(False)
}

pub fn any_terminates_on_infinite_source_when_predicate_matches_test() {
  repeat(1)
  |> fold.any(satisfying: fn(x) { x > 0 })
  |> should.equal(True)
}

pub fn find_returns_first_match_test() {
  from_list([1, 2, 3])
  |> fold.find(satisfying: fn(x) { x > 1 })
  |> should.equal(Some(2))
}

pub fn find_returns_none_when_nothing_matches_test() {
  from_list([1, 2, 3])
  |> fold.find(satisfying: fn(x) { x > 99 })
  |> should.equal(None)
}

pub fn sum_int_sums_elements_test() {
  from_list([1, 2, 3]) |> fold.sum_int |> should.equal(6)
}

pub fn sum_int_on_empty_returns_zero_test() {
  from_list([]) |> fold.sum_int |> should.equal(0)
}

pub fn sum_float_sums_elements_test() {
  from_list([1.5, 2.5]) |> fold.sum_float |> should.equal(4.0)
}

pub fn sum_float_on_empty_returns_zero_point_zero_test() {
  from_list([]) |> fold.sum_float |> should.equal(0.0)
}

pub fn product_int_multiplies_elements_test() {
  from_list([1, 2, 3]) |> fold.product_int |> should.equal(6)
}

pub fn product_int_on_empty_returns_one_test() {
  from_list([]) |> fold.product_int |> should.equal(1)
}

pub fn collect_result_all_ok_returns_ok_list_test() {
  from_list([Ok(1), Ok(2), Ok(3)])
  |> fold.collect_result
  |> should.equal(Ok([1, 2, 3]))
}

pub fn collect_result_short_circuits_on_first_error_test() {
  from_list([Ok(1), Error("x"), Ok(2)])
  |> fold.collect_result
  |> should.equal(Error("x"))
}

pub fn collect_result_does_not_pull_past_first_error_test() {
  // Element 1 is Error; element 2 would panic if pulled. The absence
  // of a panic proves collect_result stops driving the stream the
  // moment it observes the first Error.
  let stream =
    datastream.unfold(from: 0, with: fn(s) {
      case s {
        0 -> Next(element: Ok(1), state: 1)
        1 -> Next(element: Error("halt"), state: 2)
        _ -> panic as "collect_result must stop pulling after first Error"
      }
    })

  fold.collect_result(stream) |> should.equal(Error("halt"))
}

pub fn collect_result_on_empty_returns_ok_empty_test() {
  from_list([])
  |> fold.collect_result
  |> should.equal(Ok([]))
}

pub fn partition_result_splits_oks_and_errors_in_order_test() {
  from_list([Ok(1), Error("a"), Ok(2), Error("b")])
  |> fold.partition_result
  |> should.equal(#([1, 2], ["a", "b"]))
}

pub fn partition_result_visits_elements_after_first_error_test() {
  // If partition_result short-circuited on the first Error like
  // collect_result does, Ok(3) would be missing from the oks list.
  // Its presence proves this reducer is a full-traversal one.
  from_list([Error("a"), Ok(3)])
  |> fold.partition_result
  |> should.equal(#([3], ["a"]))
}

pub fn partition_map_routes_via_split_test() {
  from_list([1, 2, 3, 4])
  |> fold.partition_map(with: fn(x) {
    case x % 2 == 0 {
      True -> Ok(x)
      False -> Error(x)
    }
  })
  |> should.equal(#([2, 4], [1, 3]))
}
