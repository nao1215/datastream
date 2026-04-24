//// High-cardinality smoke tests for the BEAM-only extension.
////
//// Each BEAM-only combinator spawns worker processes, so "completes
//// in bounded time on 10k+ inputs" is the load-bearing invariant
//// rather than raw throughput. A regression that leaks workers or
//// fails to back-pressure under load would either hang this test or
//// OOM the BEAM node.

@target(erlang)
import datastream/erlang/par

@target(erlang)
import datastream/fold

@target(erlang)
import datastream/source

@target(erlang)
import gleeunit/should

@target(javascript)
/// Empty placeholder so this module compiles cleanly on JavaScript.
pub const beam_only_marker: String = "datastream/erlang/volume_test is BEAM-only"

@target(erlang)
pub fn par_map_unordered_ten_thousand_test() {
  source.range(from: 0, to: 10_000)
  |> par.map_unordered(with: fn(x) { x * 2 })
  |> fold.count
  |> should.equal(10_000)
}
