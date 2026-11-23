//// End-to-end pipeline #5: streaming request-processing with
//// `dataprep` for accumulating validation.
////
//// Models a row-shaped request body (CSV-like, NDJSON-like, or any
//// per-record protocol): the pipeline lazily walks the source, checks
//// each record's fields with `dataprep`, and folds the per-record
//// results into one `Result(List(Row), NonEmptyList(RowError))`, so
//// callers see every failure rather than only the first.
////
//// Use `datastream.collect_result` or `datastream.partition_result`
//// when stopping at the first error is the right behaviour.
////
//// Cross-target: `dataprep`, `datastream.fold`, and the surrounding
//// stream combinators all run on both the Erlang and JavaScript
//// targets.

import dataprep
import datastream
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import gleeunit/should
import non_empty_list.{type NonEmptyList}

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

pub type Checked(a) =
  Result(a, NonEmptyList(RowError))

/// Run the example pipeline against an in-memory CSV-shaped input
/// and return either every well-formed row or every collected error.
pub fn run() -> Checked(List(Row)) {
  validate_lines(["1,100", "two,200", "3,abc", "4,400"])
}

fn validate_lines(lines: List(String)) -> Checked(List(Row)) {
  datastream.from_list(lines)
  |> datastream.map(with: parse_row)
  |> datastream.fold(from: Ok([]), with: fn(rows, row) {
    dataprep.map2(rows, row, with: fn(rows, row) { [row, ..rows] })
  })
  |> result.map(list.reverse)
}

fn parse_row(line: String) -> Checked(Row) {
  case string.split(line, on: ",") {
    [raw_id, raw_amount] ->
      dataprep.map2(
        int_field(raw_id, BadId),
        int_field(raw_amount, BadAmount),
        with: fn(id, amount) { Row(id:, amount:) },
      )
    _ -> Error(non_empty_list.single(WrongShape(line: line)))
  }
}

fn int_field(raw: String, error: fn(String) -> RowError) -> Checked(Int) {
  int.parse(raw)
  |> result.replace_error(non_empty_list.single(error(raw)))
}

pub fn dataprep_pipeline_example_collects_every_error_test() {
  // Two errors are expected: line 2 has a bad id ("two"); line 3 has
  // a bad amount ("abc"). The accumulated result keeps both, unlike
  // `collect_result`, which stops at the first.
  case run() {
    Ok(_) -> should.fail()
    Error(errors) ->
      non_empty_list.to_list(errors)
      |> should.equal([BadId(raw: "two"), BadAmount(raw: "abc")])
  }
}

pub fn dataprep_pipeline_example_reports_both_fields_of_one_row_test() {
  validate_lines(["x,y", "1"])
  |> result.map_error(non_empty_list.to_list)
  |> should.equal(
    Error([BadId(raw: "x"), BadAmount(raw: "y"), WrongShape(line: "1")]),
  )
}

pub fn dataprep_pipeline_example_all_valid_input_test() {
  validate_lines(["1,10", "2,20", "3,30"])
  |> should.equal(
    Ok([Row(id: 1, amount: 10), Row(id: 2, amount: 20), Row(id: 3, amount: 30)]),
  )
}
