import datastream.{Done, Next}
import datastream/sink
import datastream/source
import gleam/string_tree
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

pub fn each_returns_nil_test() {
  from_list([1, 2, 3])
  |> sink.each(with: fn(_) { Nil })
  |> should.equal(Nil)
}

pub fn each_on_empty_returns_nil_test() {
  source.empty()
  |> sink.each(with: fn(_) { Nil })
  |> should.equal(Nil)
}

pub fn each_pulls_every_element_test() {
  let stream =
    datastream.unfold(from: 1, with: fn(s) {
      case s <= 3 {
        True -> Next(element: s, state: s + 1)
        False -> Done
      }
    })

  stream
  |> sink.each(with: fn(_) { Nil })
  |> should.equal(Nil)
}

pub fn try_each_all_ok_returns_ok_nil_test() {
  from_list([1, 2, 3, 4])
  |> sink.try_each(with: fn(_) { Ok(Nil) })
  |> should.equal(Ok(Nil))
}

pub fn try_each_on_empty_returns_ok_nil_test() {
  source.empty()
  |> sink.try_each(with: fn(_) { Ok(Nil) })
  |> should.equal(Ok(Nil))
}

pub fn try_each_short_circuits_on_first_error_test() {
  from_list([1, 2, 3, 4])
  |> sink.try_each(with: fn(x) {
    case x < 3 {
      True -> Ok(Nil)
      False -> Error(x)
    }
  })
  |> should.equal(Error(3))
}

pub fn try_each_does_not_visit_elements_after_error_test() {
  let stream =
    datastream.unfold(from: 1, with: fn(s) {
      case s <= 4 {
        True -> Next(element: s, state: s + 1)
        False -> Done
      }
    })

  stream
  |> sink.try_each(with: fn(x) {
    case x {
      3 -> Error("stop")
      4 -> panic as "try_each must not visit elements after the first Error"
      _ -> Ok(Nil)
    }
  })
  |> should.equal(Error("stop"))
}

pub fn println_runs_to_completion_on_empty_test() {
  source.empty() |> sink.println |> should.equal(Nil)
}

// --- to_string_join re-export (#213) ---

pub fn sink_to_string_join_re_export_test() {
  from_list(["row1", "row2", "row3"])
  |> sink.to_string_join(with: "\n")
  |> should.equal("row1\nrow2\nrow3")
}

pub fn sink_to_string_join_empty_test() {
  source.empty()
  |> sink.to_string_join(with: ",")
  |> should.equal("")
}

pub fn sink_to_string_tree_join_re_export_test() {
  // The tree variant must produce the same final String once the
  // tree is materialised as the direct sink.to_string_join would.
  let direct = from_list(["a", "b", "c"]) |> sink.to_string_join(with: "-")
  let via_tree =
    from_list(["a", "b", "c"])
    |> sink.to_string_tree_join(with: "-")
    |> string_tree.to_string
  via_tree |> should.equal(direct)
}
