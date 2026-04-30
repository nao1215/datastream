//// End-to-end pipeline #4 (BEAM-only): parallel batch processing
//// over a finite source with a configurable concurrency cap.
////
//// Models a batch worker: a list of inputs is mapped through a
//// CPU-bound or I/O-bound function across at most `max_workers`
//// BEAM processes, with `max_buffer` controlling the upper bound on
//// in-flight work. The unordered variant is used here because the
//// downstream aggregator does not care about input order — only the
//// final aggregated value.
////
//// BEAM-only: every combinator under `datastream/erlang/par` runs
//// only on the Erlang target. The module-level `@target(erlang)`
//// guard keeps the file out of the JavaScript build.

@target(erlang)
import datastream/erlang/par
@target(erlang)
import datastream/fold
@target(erlang)
import datastream/source
@target(erlang)
import gleam/list
@target(erlang)
import gleam/order
@target(erlang)
import gleeunit/should

@target(erlang)
pub fn main() -> Nil {
  Nil
}

@target(javascript)
/// Empty placeholder so the test module compiles cleanly on
/// JavaScript even though every test inside is BEAM-only.
pub const beam_only_marker: String = "examples/parallel_pipeline_example is BEAM-only"

@target(erlang)
/// Run the example pipeline against an in-memory list of inputs and
/// return the sum of squares. The unordered combinator is the right
/// fit when the downstream is order-independent (aggregation, sink
/// writes, fan-out to a queue).
pub fn run(inputs: List(Int)) -> Int {
  source.from_list(inputs)
  |> par.map_unordered_with(
    with: fn(x) { x * x },
    max_workers: 4,
    max_buffer: 8,
  )
  |> fold.sum_int
}

@target(erlang)
pub fn parallel_pipeline_example_sum_test() {
  // Sum of squares 1²..6² is 1+4+9+16+25+36 = 91. Order doesn't
  // matter for the addition, so the unordered combinator is fine.
  run([1, 2, 3, 4, 5, 6]) |> should.equal(91)
}

@target(erlang)
pub fn parallel_pipeline_example_set_equality_test() {
  // The element identities (independent of order) are stable across
  // runs. `list.sort` re-orders the unordered output for comparison.
  source.from_list([1, 2, 3, 4])
  |> par.map_unordered_with(
    with: fn(x) { x * 10 },
    max_workers: 2,
    max_buffer: 4,
  )
  |> fold.to_list
  |> list.sort(by: int_compare)
  |> should.equal([10, 20, 30, 40])
}

@target(erlang)
fn int_compare(a: Int, b: Int) -> order.Order {
  case a == b, a < b {
    True, _ -> order.Eq
    False, True -> order.Lt
    False, False -> order.Gt
  }
}
