import datastream
import gleam/int
import gleam/list
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
    datastream.to_list(datastream.from_list(items)) == items
  })
}

pub fn empty_stream_to_list_is_empty_test() -> Nil {
  assert datastream.to_list(datastream.empty()) == []
}

pub fn once_yields_exactly_one_element_test() -> Nil {
  metamon.forall(generator.int(range.constant(-50, 50)), fn(value) {
    datastream.to_list(datastream.once(value)) == [value]
  })
}

pub fn count_matches_list_length_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.count(datastream.from_list(items)) == list.length(items)
  })
}

// ---------- datastream.map ----------

pub fn map_preserves_length_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.count(
      datastream.map(over: datastream.from_list(items), with: fn(value) {
        value * 2
      }),
    )
    == list.length(items)
  })
}

pub fn map_matches_list_map_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let mapped =
      datastream.map(over: datastream.from_list(items), with: fn(value) {
        value + 1
      })
    datastream.to_list(mapped) == list.map(items, fn(value) { value + 1 })
  })
}

pub fn map_composition_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let plus_one = fn(value) { value + 1 }
    let times_two = fn(value) { value * 2 }
    let two_step =
      datastream.map(
        over: datastream.map(over: datastream.from_list(items), with: plus_one),
        with: times_two,
      )
    let one_step =
      datastream.map(over: datastream.from_list(items), with: fn(value) {
        times_two(plus_one(value))
      })
    datastream.to_list(two_step) == datastream.to_list(one_step)
  })
}

// ---------- datastream.filter ----------

pub fn filter_is_subset_of_input_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let filtered =
      datastream.filter(over: datastream.from_list(items), keeping: int.is_even)
    let result = datastream.to_list(filtered)
    list.all(result, fn(value) { list.contains(items, value) })
  })
}

pub fn filter_preserves_relative_order_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let via_stream =
      datastream.to_list(datastream.filter(
        over: datastream.from_list(items),
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
      datastream.from_list(items)
      |> datastream.filter(keeping: int.is_even)
      |> datastream.filter(keeping: positive)
    let combined =
      datastream.from_list(items)
      |> datastream.filter(keeping: fn(value) {
        int.is_even(value) && positive(value)
      })
    datastream.to_list(chained) == datastream.to_list(combined)
  })
}

// ---------- datastream.take / datastream.drop ----------

pub fn take_count_is_bounded_test() -> Nil {
  metamon.forall(
    generator.tuple2(
      small_int_list_generator(),
      generator.int(range.constant(0, 10)),
    ),
    fn(pair) {
      let #(items, n) = pair
      let taken =
        datastream.count(datastream.take(
          over: datastream.from_list(items),
          up_to: n,
        ))
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
        datastream.to_list(datastream.take(
          over: datastream.from_list(items),
          up_to: n,
        ))
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
        datastream.to_list(datastream.drop(
          over: datastream.from_list(items),
          up_to: n,
        ))
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
        datastream.to_list(datastream.take(
          over: datastream.from_list(items),
          up_to: n,
        ))
      let dropped =
        datastream.to_list(datastream.drop(
          over: datastream.from_list(items),
          up_to: n,
        ))
      list.append(taken, dropped) == items
    },
  )
}

pub fn take_zero_yields_empty_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.to_list(datastream.take(
      over: datastream.from_list(items),
      up_to: 0,
    ))
    == []
  })
}

pub fn drop_zero_is_identity_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.to_list(datastream.drop(
      over: datastream.from_list(items),
      up_to: 0,
    ))
    == items
  })
}

// ---------- datastream.append ----------

pub fn append_concatenates_lists_test() -> Nil {
  metamon.forall(
    generator.tuple2(small_int_list_generator(), small_int_list_generator()),
    fn(pair) {
      let #(left, right) = pair
      let combined =
        datastream.append(
          datastream.from_list(left),
          datastream.from_list(right),
        )
      datastream.to_list(combined) == list.append(left, right)
    },
  )
}

pub fn append_length_is_sum_test() -> Nil {
  metamon.forall(
    generator.tuple2(small_int_list_generator(), small_int_list_generator()),
    fn(pair) {
      let #(left, right) = pair
      datastream.count(datastream.append(
        datastream.from_list(left),
        datastream.from_list(right),
      ))
      == list.length(left) + list.length(right)
    },
  )
}

pub fn append_empty_left_is_identity_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.to_list(datastream.append(
      datastream.empty(),
      datastream.from_list(items),
    ))
    == items
  })
}

pub fn append_empty_right_is_identity_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.to_list(datastream.append(
      datastream.from_list(items),
      datastream.empty(),
    ))
    == items
  })
}

// ---------- datastream.range ----------

pub fn range_ascending_count_test() -> Nil {
  metamon.forall(
    generator.tuple2(
      generator.int(range.constant(-20, 20)),
      generator.int(range.constant(0, 20)),
    ),
    fn(pair) {
      let #(start, span) = pair
      let stop = start + span
      datastream.count(datastream.range(from: start, to: stop)) == span
    },
  )
}

pub fn range_empty_when_endpoints_equal_test() -> Nil {
  metamon.forall(generator.int(range.constant(-50, 50)), fn(value) {
    datastream.to_list(datastream.range(from: value, to: value)) == []
  })
}

// ---------- datastream.sum_int / datastream.first ----------

pub fn sum_int_matches_list_fold_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.sum_int(datastream.from_list(items))
    == list.fold(items, 0, fn(acc, value) { acc + value })
  })
}

pub fn first_matches_list_first_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let stream_first = datastream.first(datastream.from_list(items))
    let expected = case items {
      [] -> Error(Nil)
      [head, ..] -> Ok(head)
    }
    stream_first == expected
  })
}

pub fn last_matches_list_last_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    let stream_last = datastream.last(datastream.from_list(items))
    let expected = case list.last(items) {
      Ok(value) -> Ok(value)
      Error(Nil) -> Error(Nil)
    }
    stream_last == expected
  })
}

pub fn all_matches_list_all_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.all(over: datastream.from_list(items), satisfying: fn(value) {
      value >= 0
    })
    == list.all(items, fn(value) { value >= 0 })
  })
}

pub fn any_matches_list_any_test() -> Nil {
  metamon.forall(small_int_list_generator(), fn(items) {
    datastream.any(over: datastream.from_list(items), satisfying: fn(value) {
      value > 0
    })
    == list.any(items, fn(value) { value > 0 })
  })
}
