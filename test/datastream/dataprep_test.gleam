import dataprep/non_empty_list
import dataprep/validated.{Invalid, Valid}
import datastream.{Done, Next}
import datastream/dataprep
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

// --- collect_validated ---------------------------------------------------

pub fn collect_validated_all_valid_test() {
  from_list([Valid(1), Valid(2), Valid(3)])
  |> dataprep.collect_validated
  |> should.equal(Valid([1, 2, 3]))
}

pub fn collect_validated_empty_test() {
  let empty: List(validated.Validated(Int, String)) = []
  from_list(empty)
  |> dataprep.collect_validated
  |> should.equal(Valid([]))
}

pub fn collect_validated_accumulates_errors_in_source_order_test() {
  let result =
    from_list([
      Valid(1),
      Invalid(non_empty_list.single("e1")),
      Valid(2),
      Invalid(non_empty_list.NonEmptyList(first: "e2", rest: ["e3"])),
    ])
    |> dataprep.collect_validated
  case result {
    Valid(_) -> panic as "expected Invalid"
    Invalid(errors) ->
      non_empty_list.to_list(errors) |> should.equal(["e1", "e2", "e3"])
  }
}

pub fn collect_validated_first_invalid_alone_test() {
  let result =
    from_list([Valid(1), Invalid(non_empty_list.single("only"))])
    |> dataprep.collect_validated
  case result {
    Valid(_) -> panic as "expected Invalid"
    Invalid(errors) -> non_empty_list.to_list(errors) |> should.equal(["only"])
  }
}

// --- partition_validated ------------------------------------------------

pub fn partition_validated_one_valid_one_invalid_test() {
  let #(valids, invalids) =
    from_list([Valid(1), Invalid(non_empty_list.single("e"))])
    |> dataprep.partition_validated
  valids |> should.equal([1])
  invalids
  |> list_of_nel_to_list_of_list
  |> should.equal([["e"]])
}

pub fn partition_validated_preserves_per_element_grouping_test() {
  let #(valids, invalids) =
    from_list([
      Invalid(non_empty_list.NonEmptyList(first: "e1", rest: ["e2"])),
      Valid(7),
      Invalid(non_empty_list.single("e3")),
    ])
    |> dataprep.partition_validated
  valids |> should.equal([7])
  invalids
  |> list_of_nel_to_list_of_list
  |> should.equal([["e1", "e2"], ["e3"]])
}

pub fn partition_validated_empty_test() {
  let empty: List(validated.Validated(Int, String)) = []
  let #(valids, invalids) =
    from_list(empty)
    |> dataprep.partition_validated
  valids |> should.equal([])
  invalids
  |> list_of_nel_to_list_of_list
  |> should.equal([])
}

fn list_of_nel_to_list_of_list(
  nels: List(non_empty_list.NonEmptyList(e)),
) -> List(List(e)) {
  case nels {
    [] -> []
    [head, ..tail] -> [
      non_empty_list.to_list(head),
      ..list_of_nel_to_list_of_list(tail)
    ]
  }
}
