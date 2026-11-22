import datastream.{Done, Next}
import gleeunit

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

  assert datastream.to_list(stream) == [1]
}

pub fn unfold_done_immediately_test() {
  let stream = datastream.unfold(from: Nil, with: fn(_) { Done })

  assert datastream.to_list(stream) == []
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

  assert datastream.to_list(stream) == ["a", "b"]
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

  assert datastream.first(stream) == Ok(1)
  assert datastream.first(stream) == Ok(1)
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

  assert datastream.to_list(stream) == ["x"]
}
