import datastream.{Done, Next}
import gleeunit
import gleeunit/should

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn unfold_yields_one_then_done_test() {
  let stream =
    datastream.unfold(from: 0, with: fn(s) {
      case s {
        0 -> Next(element: 1, state: 1)
        _ -> Done
      }
    })

  let assert Next(1, rest) = datastream.pull(stream)
  datastream.pull(rest) |> should.equal(Done)
}

pub fn unfold_done_immediately_test() {
  let stream = datastream.unfold(from: Nil, with: fn(_) { Done })

  datastream.pull(stream) |> should.equal(Done)
}

pub fn unfold_yields_two_then_done_test() {
  let stream =
    datastream.unfold(from: 0, with: fn(s) {
      case s {
        0 -> Next(element: "a", state: 1)
        1 -> Next(element: "b", state: 2)
        _ -> Done
      }
    })

  let assert Next("a", after_a) = datastream.pull(stream)
  let assert Next("b", after_b) = datastream.pull(after_a)
  datastream.pull(after_b) |> should.equal(Done)
}

pub fn pull_re_runs_from_source_each_time_test() {
  let stream =
    datastream.unfold(from: 0, with: fn(s) {
      case s {
        0 -> Next(element: 1, state: 1)
        1 -> Next(element: 2, state: 2)
        _ -> Done
      }
    })

  let assert Next(1, _) = datastream.pull(stream)
  let assert Next(1, _) = datastream.pull(stream)
}

pub fn unfold_state_is_private_to_constructed_stream_test() {
  let stream =
    datastream.unfold(from: #(0, "x"), with: fn(s) {
      let #(i, label) = s
      case i {
        0 -> Next(element: label, state: #(1, label))
        _ -> Done
      }
    })

  let assert Next("x", rest) = datastream.pull(stream)
  datastream.pull(rest) |> should.equal(Done)
}
