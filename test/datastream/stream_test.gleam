import datastream.{type Stream, Done, Next}
import datastream/chunk.{type Chunk}
import datastream/fold
import datastream/source
import datastream/stream
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

@target(erlang)
pub fn take_negative_panics_test() {
  let did_panic =
    panicked(fn() {
      let _result =
        from_list([1, 2, 3])
        |> stream.take(up_to: -5)
      Nil
    })
  did_panic |> should.be_true
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

@target(erlang)
pub fn drop_negative_panics_test() {
  let did_panic =
    panicked(fn() {
      let _result =
        from_list([1, 2, 3])
        |> stream.drop(up_to: -5)
      Nil
    })
  did_panic |> should.be_true
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

fn chunks_to_lists(stream_of_chunks) {
  stream_of_chunks
  |> fold.to_list
  |> list.map(chunk.to_list)
}

fn groups_to_pairs(
  stream_of_groups: Stream(#(k, Chunk(a))),
) -> List(#(k, List(a))) {
  stream_of_groups
  |> fold.to_list
  |> list.map(fn(pair) {
    let #(key, c) = pair
    #(key, chunk.to_list(c))
  })
}

pub fn chunks_of_emits_full_size_then_remainder_test() {
  from_list([1, 2, 3, 4, 5])
  |> stream.chunks_of(into: 2)
  |> chunks_to_lists
  |> should.equal([[1, 2], [3, 4], [5]])
}

pub fn chunks_of_evenly_divisible_test() {
  from_list([1, 2, 3, 4])
  |> stream.chunks_of(into: 2)
  |> chunks_to_lists
  |> should.equal([[1, 2], [3, 4]])
}

pub fn chunks_of_on_empty_yields_no_chunks_test() {
  from_list([])
  |> stream.chunks_of(into: 3)
  |> chunks_to_lists
  |> should.equal([])
}

pub fn chunks_of_size_larger_than_source_test() {
  from_list([1, 2, 3])
  |> stream.chunks_of(into: 99)
  |> chunks_to_lists
  |> should.equal([[1, 2, 3]])
}

@target(erlang)
pub fn chunks_of_zero_panics_test() {
  let did_panic =
    panicked(fn() {
      let _result =
        from_list([1, 2, 3])
        |> stream.chunks_of(into: 0)
      Nil
    })
  did_panic |> should.be_true
}

@target(erlang)
pub fn chunks_of_negative_panics_test() {
  let did_panic =
    panicked(fn() {
      let _result =
        from_list([1, 2, 3])
        |> stream.chunks_of(into: -5)
      Nil
    })
  did_panic |> should.be_true
}

pub fn chunks_of_terminates_on_infinite_source_test() {
  source.repeat(1)
  |> stream.chunks_of(into: 2)
  |> stream.take(up_to: 3)
  |> chunks_to_lists
  |> should.equal([[1, 1], [1, 1], [1, 1]])
}

pub fn group_adjacent_groups_runs_of_equal_test() {
  from_list([1, 1, 2, 3, 3, 3])
  |> stream.group_adjacent(by: fn(x) { x })
  |> groups_to_pairs
  |> should.equal([#(1, [1, 1]), #(2, [2]), #(3, [3, 3, 3])])
}

pub fn group_adjacent_by_key_function_test() {
  let first_char = fn(s: String) -> String {
    case string.first(s) {
      Ok(c) -> c
      Error(_) -> ""
    }
  }

  from_list(["apple", "ant", "banana", "bee", "cat"])
  |> stream.group_adjacent(by: first_char)
  |> groups_to_pairs
  |> should.equal([
    #("a", ["apple", "ant"]),
    #("b", ["banana", "bee"]),
    #("c", ["cat"]),
  ])
}

pub fn group_adjacent_on_empty_yields_no_groups_test() {
  from_list([])
  |> stream.group_adjacent(by: fn(x) { x })
  |> groups_to_pairs
  |> should.equal([])
}

pub fn group_adjacent_does_not_merge_non_adjacent_duplicates_test() {
  from_list([1, 2, 1])
  |> stream.group_adjacent(by: fn(x) { x })
  |> groups_to_pairs
  |> should.equal([#(1, [1]), #(2, [2]), #(1, [1])])
}

// --- construction-time panics (Erlang target) ----------------------------
//
// `take`, `drop`, and `chunks_of` reject nonsensical count / size
// arguments at construction time with a panic per the datastream
// module-level invalid-argument policy unified in #145. These tests
// pin that contract. Erlang-only because the panic-detection helper
// spawns a process; JavaScript has no matching primitive. The
// validation logic itself is cross-target.

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
