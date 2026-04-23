import datastream.{Done, Next}
import datastream/fold
import datastream/source
import datastream/stream
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

fn iterate(start, with f) {
  datastream.unfold(from: start, with: fn(s) { Next(element: s, state: f(s)) })
}

pub fn map_doubles_each_element_test() {
  from_list([1, 2, 3])
  |> stream.map(with: fn(x) { x * 2 })
  |> fold.to_list
  |> should.equal([2, 4, 6])
}

pub fn map_on_empty_returns_empty_test() {
  from_list([])
  |> stream.map(with: fn(x) { x * 2 })
  |> fold.to_list
  |> should.equal([])
}

pub fn filter_keeps_matching_elements_test() {
  from_list([1, 2, 3, 4])
  |> stream.filter(keeping: fn(x) { x > 2 })
  |> fold.to_list
  |> should.equal([3, 4])
}

pub fn filter_with_constant_false_yields_empty_test() {
  from_list([1, 2, 3])
  |> stream.filter(keeping: fn(_) { False })
  |> fold.to_list
  |> should.equal([])
}

pub fn take_yields_first_n_test() {
  from_list([1, 2, 3])
  |> stream.take(up_to: 2)
  |> fold.to_list
  |> should.equal([1, 2])
}

pub fn take_zero_yields_empty_test() {
  from_list([1, 2, 3])
  |> stream.take(up_to: 0)
  |> fold.to_list
  |> should.equal([])
}

pub fn take_negative_yields_empty_test() {
  from_list([1, 2, 3])
  |> stream.take(up_to: -5)
  |> fold.to_list
  |> should.equal([])
}

pub fn take_more_than_available_yields_all_test() {
  from_list([1, 2, 3])
  |> stream.take(up_to: 99)
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

pub fn take_terminates_on_infinite_source_test() {
  repeat(7)
  |> stream.take(up_to: 3)
  |> fold.to_list
  |> should.equal([7, 7, 7])
}

pub fn drop_skips_first_n_test() {
  from_list([1, 2, 3])
  |> stream.drop(up_to: 1)
  |> fold.to_list
  |> should.equal([2, 3])
}

pub fn drop_zero_is_identity_test() {
  from_list([1, 2, 3])
  |> stream.drop(up_to: 0)
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

pub fn drop_negative_is_identity_test() {
  from_list([1, 2, 3])
  |> stream.drop(up_to: -5)
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

pub fn drop_more_than_available_yields_empty_test() {
  from_list([1, 2, 3])
  |> stream.drop(up_to: 99)
  |> fold.to_list
  |> should.equal([])
}

pub fn take_while_yields_longest_passing_prefix_test() {
  from_list([1, 2, 1, 4])
  |> stream.take_while(satisfying: fn(x) { x < 3 })
  |> fold.to_list
  |> should.equal([1, 2, 1])
}

pub fn take_while_with_constant_false_yields_empty_test() {
  from_list([5, 6, 7])
  |> stream.take_while(satisfying: fn(_) { False })
  |> fold.to_list
  |> should.equal([])
}

pub fn take_while_terminates_on_infinite_source_test() {
  iterate(1, with: fn(x) { x + 1 })
  |> stream.take_while(satisfying: fn(x) { x < 4 })
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

pub fn drop_while_skips_longest_passing_prefix_test() {
  from_list([1, 2, 1, 4])
  |> stream.drop_while(satisfying: fn(x) { x < 3 })
  |> fold.to_list
  |> should.equal([4])
}

pub fn drop_while_with_constant_false_is_identity_test() {
  from_list([1, 2, 1])
  |> stream.drop_while(satisfying: fn(_) { False })
  |> fold.to_list
  |> should.equal([1, 2, 1])
}

pub fn append_concatenates_two_streams_test() {
  stream.append(from_list([1, 2]), from_list([3, 4]))
  |> fold.to_list
  |> should.equal([1, 2, 3, 4])
}

pub fn append_with_empty_first_yields_second_test() {
  stream.append(from_list([]), from_list([3, 4]))
  |> fold.to_list
  |> should.equal([3, 4])
}

pub fn append_with_empty_second_yields_first_test() {
  stream.append(from_list([1, 2]), from_list([]))
  |> fold.to_list
  |> should.equal([1, 2])
}

pub fn map_is_lazy_until_terminal_test() {
  let _pipeline =
    from_list([1, 2, 3])
    |> stream.map(with: fn(_) {
      panic as "map must not run its function before a terminal"
    })

  Nil
}

pub fn map_invokes_callback_once_per_element_on_terminal_test() {
  let pipeline =
    from_list([1, 2, 3])
    |> stream.map(with: fn(x) { x + 100 })

  pipeline |> fold.to_list |> should.equal([101, 102, 103])
}

pub fn filter_map_keeps_some_drops_none_test() {
  from_list([1, 2, 3])
  |> stream.filter_map(with: fn(x) {
    case x > 1 {
      True -> Some(x * 10)
      False -> None
    }
  })
  |> fold.to_list
  |> should.equal([20, 30])
}

pub fn filter_map_on_empty_yields_empty_test() {
  from_list([])
  |> stream.filter_map(with: fn(x) { Some(x) })
  |> fold.to_list
  |> should.equal([])
}

pub fn flat_map_concatenates_inner_streams_test() {
  from_list([1, 2, 3])
  |> stream.flat_map(with: fn(x) { from_list([x, x]) })
  |> fold.to_list
  |> should.equal([1, 1, 2, 2, 3, 3])
}

pub fn flat_map_with_all_empty_inners_yields_empty_test() {
  from_list([1, 2])
  |> stream.flat_map(with: fn(_) { source.empty() })
  |> fold.to_list
  |> should.equal([])
}

pub fn flatten_concatenates_inner_streams_test() {
  from_list([from_list([1]), from_list([2, 3])])
  |> stream.flatten
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

pub fn flatten_is_observationally_flat_map_identity_test() {
  let direct =
    from_list([from_list([1, 2]), from_list([]), from_list([3])])
    |> stream.flatten
    |> fold.to_list
  let via_flat_map =
    from_list([from_list([1, 2]), from_list([]), from_list([3])])
    |> stream.flat_map(with: fn(s) { s })
    |> fold.to_list
  direct |> should.equal(via_flat_map)
}

pub fn concat_walks_streams_in_list_order_test() {
  stream.concat([from_list([1]), from_list([2, 3]), from_list([4])])
  |> fold.to_list
  |> should.equal([1, 2, 3, 4])
}

pub fn concat_of_empty_list_yields_empty_test() {
  stream.concat([]) |> fold.to_list |> should.equal([])
}

pub fn scan_emits_running_accumulator_test() {
  from_list([1, 2, 3, 4])
  |> stream.scan(from: 0, with: fn(acc, x) { acc + x })
  |> fold.to_list
  |> should.equal([1, 3, 6, 10])
}

pub fn scan_does_not_emit_seed_on_empty_input_test() {
  from_list([])
  |> stream.scan(from: 0, with: fn(acc, x) { acc + x })
  |> fold.to_list
  |> should.equal([])
}

pub fn map_accum_threads_state_and_emits_output_test() {
  from_list([10, 20, 30])
  |> stream.map_accum(from: 0, with: fn(acc, x) {
    let s = acc + x
    #(s, s)
  })
  |> fold.to_list
  |> should.equal([10, 30, 60])
}

pub fn zip_pairs_until_either_halts_test() {
  stream.zip(from_list([1, 2, 3]), from_list(["a", "b"]))
  |> fold.to_list
  |> should.equal([#(1, "a"), #(2, "b")])
}

pub fn zip_with_empty_left_yields_empty_test() {
  stream.zip(from_list([]), from_list([1, 2]))
  |> fold.to_list
  |> should.equal([])
}

pub fn zip_with_combiner_test() {
  stream.zip_with(from_list([1, 2, 3]), from_list([10, 20, 30]), with: fn(a, b) {
    a + b
  })
  |> fold.to_list
  |> should.equal([11, 22, 33])
}

pub fn intersperse_inserts_separator_between_elements_test() {
  from_list([1, 2, 3])
  |> stream.intersperse(with: 0)
  |> fold.to_list
  |> should.equal([1, 0, 2, 0, 3])
}

pub fn intersperse_on_empty_yields_empty_test() {
  from_list([])
  |> stream.intersperse(with: 0)
  |> fold.to_list
  |> should.equal([])
}

pub fn intersperse_on_singleton_unchanged_test() {
  from_list([1])
  |> stream.intersperse(with: 0)
  |> fold.to_list
  |> should.equal([1])
}

pub fn tap_re_emits_elements_unchanged_test() {
  from_list([1, 2, 3])
  |> stream.tap(with: fn(_) { Nil })
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

pub fn dedupe_adjacent_collapses_runs_test() {
  from_list([1, 1, 2, 2, 2, 3, 1])
  |> stream.dedupe_adjacent
  |> fold.to_list
  |> should.equal([1, 2, 3, 1])
}

pub fn dedupe_adjacent_on_empty_yields_empty_test() {
  from_list([])
  |> stream.dedupe_adjacent
  |> fold.to_list
  |> should.equal([])
}

pub fn dedupe_adjacent_keeps_non_adjacent_duplicates_test() {
  from_list([1, 2, 1, 2, 1])
  |> stream.dedupe_adjacent
  |> fold.to_list
  |> should.equal([1, 2, 1, 2, 1])
}
