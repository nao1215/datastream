//// End-to-end pipeline #5: streaming request-processing with
//// `dataprep` for accumulating validation.
////
//// Models a row-shaped request body (CSV-like, NDJSON-like, or any
//// per-record protocol): the pipeline lazily walks the source, runs
//// each record through a validator that produces a
//// `Validated(Row, Error)`, and folds the per-record verdicts into a
//// single `Validated(List(Row), Error)` so callers see *every*
//// failure rather than only the first.
////
//// Pair this with `fold.collect_result` / `fold.partition_result`
//// from the surrounding library when short-circuit-on-first-error is
//// the right behaviour — `Validated` is for the "show me everything
//// that's wrong" path.
////
//// Cross-target: `dataprep`, `fold.fold`, and the surrounding
//// stream combinators all run on both the Erlang and JavaScript
//// targets.

import dataprep/non_empty_list
import dataprep/validated.{type Validated, Invalid, Valid}
import datastream/fold
import datastream/source
import datastream/stream
import gleam/int
import gleam/list
import gleam/string
import gleeunit/should

pub fn main() -> Nil {
  Nil
}

pub type Row {
  Row(id: Int, amount: Int)
}

pub type RowError {
  /// Non-numeric `id` field.
  BadId(raw: String)
  /// Non-numeric `amount` field.
  BadAmount(raw: String)
  /// Wrong number of comma-separated fields.
  WrongShape(line: String)
}

/// Run the example pipeline against an in-memory CSV-shaped input
/// and return either every well-formed row or every collected error.
/// On success the returned `Validated` is `Valid([Row, ...])`; on
/// any failure it is `Invalid(NonEmptyList(RowError, ...))`.
pub fn run() -> Validated(List(Row), RowError) {
  let lines = ["1,100", "two,200", "3,abc", "4,400"]

  source.from_list(lines)
  |> stream.map(with: parse_row)
  |> fold.fold(from: Valid([]), with: combine)
  |> validated.map(list.reverse)
}

fn parse_row(line: String) -> Validated(Row, RowError) {
  case string.split(line, on: ",") {
    [raw_id, raw_amount] -> {
      let id = int.parse(raw_id)
      let amount = int.parse(raw_amount)
      case id, amount {
        Ok(i), Ok(a) -> Valid(Row(id: i, amount: a))
        Error(_), Ok(_) -> validated.fail(BadId(raw: raw_id))
        Ok(_), Error(_) -> validated.fail(BadAmount(raw: raw_amount))
        Error(_), Error(_) ->
          Invalid(non_empty_list.cons(
            head: BadId(raw: raw_id),
            tail: non_empty_list.single(BadAmount(raw: raw_amount)),
          ))
      }
    }
    _ -> validated.fail(WrongShape(line: line))
  }
}

fn combine(
  acc: Validated(List(Row), RowError),
  next: Validated(Row, RowError),
) -> Validated(List(Row), RowError) {
  case acc, next {
    Valid(rows), Valid(row) -> Valid([row, ..rows])
    Valid(_), Invalid(es) -> Invalid(es)
    Invalid(es), Valid(_) -> Invalid(es)
    Invalid(a), Invalid(b) -> Invalid(non_empty_list.append(a, b))
  }
}

pub fn dataprep_pipeline_example_collects_every_error_test() {
  // Two errors are expected: line 2 has a bad id ("two"); line 3 has
  // a bad amount ("abc"). The accumulating Validated keeps both,
  // unlike `Result` which would short-circuit on the first.
  case run() {
    Valid(_) -> should.fail()
    Invalid(errors) -> {
      non_empty_list.to_list(errors)
      |> should.equal([BadId(raw: "two"), BadAmount(raw: "abc")])
    }
  }
}

pub fn dataprep_pipeline_example_all_valid_input_test() {
  let lines = ["1,10", "2,20", "3,30"]
  let result =
    source.from_list(lines)
    |> stream.map(with: parse_row)
    |> fold.fold(from: Valid([]), with: combine)
    |> validated.map(list.reverse)
  result
  |> should.equal(
    Valid([
      Row(id: 1, amount: 10),
      Row(id: 2, amount: 20),
      Row(id: 3, amount: 30),
    ]),
  )
}
