import datastream/fold
import datastream/source
import datastream/stream
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import metamon
import metamon/generator
import metamon/generator/range

fn small_int_list_generator() -> generator.Generator(List(Int)) {
  generator.list_of(
    generator.int(range.constant(-50, 50)),
    range.constant(0, 8),
  )
}

// ---------- source / fold round-trip ----------

pub fn from_list_to_list_round_trips_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.to_list(source.from_list(items)) == items
  })
}

pub fn empty_stream_to_list_is_empty_test() -> Nil {
  assert fold.to_list(source.empty()) == []
}

pub fn once_yields_exactly_one_element_test() -> Nil {
  metamon.forall(generator.int(range.constant(-50, 50)), fn(value) {
    fold.to_list(source.once(value)) == [value]
  })
}

pub fn count_matches_list_length_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.count(source.from_list(items)) == list.length(items)
  })
}

// ---------- stream.map ----------

pub fn map_preserves_length_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.count(
      stream.map(over: source.from_list(items), with: fn(value) { value * 2 }),
    )
    == list.length(items)
  })
}

pub fn map_matches_list_map_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let mapped =
      stream.map(over: source.from_list(items), with: fn(value) { value + 1 })
    fold.to_list(mapped) == list.map(items, fn(value) { value + 1 })
  })
}

pub fn map_composition_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let plus_one = fn(value) { value + 1 }
    let times_two = fn(value) { value * 2 }
    let two_step =
      stream.map(
        over: stream.map(over: source.from_list(items), with: plus_one),
        with: times_two,
      )
    let one_step =
      stream.map(over: source.from_list(items), with: fn(value) {
        times_two(plus_one(value))
      })
    fold.to_list(two_step) == fold.to_list(one_step)
  })
}

// ---------- stream.filter ----------

pub fn filter_is_subset_of_input_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let filtered =
      stream.filter(over: source.from_list(items), keeping: int.is_even)
    let result = fold.to_list(filtered)
    list.all(result, fn(value) { list.contains(items, value) })
  })
}

pub fn filter_preserves_relative_order_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let via_stream =
      fold.to_list(stream.filter(
        over: source.from_list(items),
        keeping: int.is_even,
      ))
    let via_list = list.filter(items, int.is_even)
    via_stream == via_list
  })
}

pub fn filter_then_filter_is_filter_with_and_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let positive = fn(value) { value > 0 }
    let chained =
      source.from_list(items)
      |> stream.filter(keeping: int.is_even)
      |> stream.filter(keeping: positive)
    let combined =
      source.from_list(items)
      |> stream.filter(keeping: fn(value) {
        int.is_even(value) && positive(value)
      })
    fold.to_list(chained) == fold.to_list(combined)
  })
}

// ---------- stream.take / stream.drop ----------

pub fn take_count_is_bounded_test() -> Nil {
  metamon.forall(
    generator.tuple2(
      small_int_list_generator(),
      generator.int(range.constant(0, 10)),
    ),
    fn(pair) {
      let #(items, n) = pair
      let taken =
        fold.count(stream.take(over: source.from_list(items), up_to: n))
      taken <= n && taken <= list.length(items)
    },
  )
}

pub fn take_matches_list_take_test() -> Nil {
  metamon.forall(
    generator.tuple2(
      small_int_list_generator(),
      generator.int(range.constant(0, 10)),
    ),
    fn(pair) {
      let #(items, n) = pair
      let taken =
        fold.to_list(stream.take(over: source.from_list(items), up_to: n))
      taken == list.take(items, n)
    },
  )
}

pub fn drop_matches_list_drop_test() -> Nil {
  metamon.forall(
    generator.tuple2(
      small_int_list_generator(),
      generator.int(range.constant(0, 10)),
    ),
    fn(pair) {
      let #(items, n) = pair
      let dropped =
        fold.to_list(stream.drop(over: source.from_list(items), up_to: n))
      dropped == list.drop(items, n)
    },
  )
}

pub fn take_then_drop_concat_recovers_original_test() -> Nil {
  metamon.forall(
    generator.tuple2(
      small_int_list_generator(),
      generator.int(range.constant(0, 10)),
    ),
    fn(pair) {
      let #(items, n) = pair
      let taken =
        fold.to_list(stream.take(over: source.from_list(items), up_to: n))
      let dropped =
        fold.to_list(stream.drop(over: source.from_list(items), up_to: n))
      list.append(taken, dropped) == items
    },
  )
}

pub fn take_zero_yields_empty_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.to_list(stream.take(over: source.from_list(items), up_to: 0)) == []
  })
}

pub fn drop_zero_is_identity_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.to_list(stream.drop(over: source.from_list(items), up_to: 0)) == items
  })
}

// ---------- stream.append ----------

pub fn append_concatenates_lists_test() -> Nil {
  metamon.forall(
    generator.tuple2(small_int_list_generator(), small_int_list_generator()),
    fn(pair) {
      let #(left, right) = pair
      let combined =
        stream.append(source.from_list(left), source.from_list(right))
      fold.to_list(combined) == list.append(left, right)
    },
  )
}

pub fn append_length_is_sum_test() -> Nil {
  metamon.forall(
    generator.tuple2(small_int_list_generator(), small_int_list_generator()),
    fn(pair) {
      let #(left, right) = pair
      fold.count(stream.append(source.from_list(left), source.from_list(right)))
      == list.length(left) + list.length(right)
    },
  )
}

pub fn append_empty_left_is_identity_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.to_list(stream.append(source.empty(), source.from_list(items)))
    == items
  })
}

pub fn append_empty_right_is_identity_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.to_list(stream.append(source.from_list(items), source.empty()))
    == items
  })
}

// ---------- source.range ----------

pub fn range_ascending_count_test() -> Nil {
  metamon.forall(
    generator.tuple2(
      generator.int(range.constant(-20, 20)),
      generator.int(range.constant(0, 20)),
    ),
    fn(pair) {
      let #(start, span) = pair
      let stop = start + span
      fold.count(source.range(from: start, to: stop)) == span
    },
  )
}

pub fn range_empty_when_endpoints_equal_test() -> Nil {
  metamon.forall(generator.int(range.constant(-50, 50)), fn(value) {
    fold.to_list(source.range(from: value, to: value)) == []
  })
}

// ---------- fold.sum_int / fold.first ----------

pub fn sum_int_matches_list_fold_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.sum_int(source.from_list(items))
    == list.fold(items, 0, fn(acc, value) { acc + value })
  })
}

pub fn first_matches_list_first_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let stream_first = fold.first(source.from_list(items))
    let expected = case items {
      [] -> None
      [head, ..] -> Some(head)
    }
    stream_first == expected
  })
}

pub fn last_matches_list_last_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let stream_last = fold.last(source.from_list(items))
    let expected = case list.last(items) {
      Ok(value) -> Some(value)
      Error(Nil) -> None
    }
    stream_last == expected
  })
}

pub fn all_matches_list_all_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.all(over: source.from_list(items), satisfying: fn(value) { value >= 0 })
    == list.all(items, fn(value) { value >= 0 })
  })
}

pub fn any_matches_list_any_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    fold.any(over: source.from_list(items), satisfying: fn(value) { value > 0 })
    == list.any(items, fn(value) { value > 0 })
  })
}
