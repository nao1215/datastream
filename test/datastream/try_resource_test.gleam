//// Tests for `source.try_resource` and `ResourceError`.
////
//// Cross-target tests pin the element shape and the OpenError /
//// NextError lifting. Erlang-only tests use the process dictionary as
//// counters to confirm `close` is called once on success and zero
//// times on `Error(OpenError)`.

import datastream.{Done, Next}
import datastream/fold
import datastream/source.{NextError, OpenError}
import datastream/stream
import gleam/option
import gleeunit/should

// --- cross-target ---------------------------------------------------------

fn ok_resource(elements: List(a)) {
  source.try_resource(
    open: fn() { Ok(elements) },
    next: fn(state) {
      case state {
        [] -> Done
        [head, ..tail] -> Next(element: Ok(head), state: tail)
      }
    },
    close: fn(_) { Nil },
  )
}

pub fn try_resource_open_ok_yields_ok_elements_test() {
  ok_resource([1, 2])
  |> fold.to_list
  |> should.equal([Ok(1), Ok(2)])
}

pub fn try_resource_open_error_yields_one_open_error_test() {
  source.try_resource(
    open: fn() -> Result(Nil, String) { Error("oops") },
    next: fn(_) -> datastream.Step(Result(Int, String), Nil) { Done },
    close: fn(_) { Nil },
  )
  |> fold.to_list
  |> should.equal([Error(OpenError("oops"))])
}

pub fn try_resource_next_error_is_lifted_to_next_error_test() {
  let s =
    source.try_resource(
      open: fn() { Ok(0) },
      next: fn(state) {
        case state {
          0 -> Next(element: Ok(1), state: 1)
          1 -> Next(element: Error("bad"), state: 2)
          2 -> Next(element: Ok(3), state: 3)
          _ -> Done
        }
      },
      close: fn(_) { Nil },
    )

  s
  |> fold.to_list
  |> should.equal([Ok(1), Error(NextError("bad")), Ok(3)])
}

pub fn try_resource_re_runs_open_per_terminal_test() {
  let s = ok_resource([1, 2])
  fold.to_list(s) |> should.equal([Ok(1), Ok(2)])
  fold.to_list(s) |> should.equal([Ok(1), Ok(2)])
}

pub fn try_resource_first_returns_open_error_test() {
  source.try_resource(
    open: fn() -> Result(Nil, String) { Error("oops") },
    next: fn(_) -> datastream.Step(Result(Int, String), Nil) { Done },
    close: fn(_) { Nil },
  )
  |> fold.first
  |> should.equal(option.Some(Error(OpenError("oops"))))
}

pub fn try_resource_with_take_one_yields_first_element_test() {
  ok_resource([1, 2, 3])
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([Ok(1)])
}

pub fn try_resource_collect_result_on_all_ok_test() {
  ok_resource([1, 2])
  |> fold.collect_result
  |> should.equal(Ok([1, 2]))
}

pub fn try_resource_collect_result_short_circuits_on_open_error_test() {
  source.try_resource(
    open: fn() -> Result(Nil, String) { Error("oops") },
    next: fn(_) -> datastream.Step(Result(Int, String), Nil) { Done },
    close: fn(_) { Nil },
  )
  |> fold.collect_result
  |> should.equal(Error(OpenError("oops")))
}

// --- Erlang-only counter assertions --------------------------------------

@target(erlang)
@external(erlang, "erlang", "put")
fn put_dict(key: String, value: Int) -> Int

@target(erlang)
@external(erlang, "erlang", "get")
fn get_dict(key: String) -> Int

@target(erlang)
fn reset(key: String) -> Nil {
  let _previous = put_dict(key, 0)
  Nil
}

@target(erlang)
fn bump(key: String) -> Nil {
  let current = get_dict(key)
  let _previous = put_dict(key, current + 1)
  Nil
}

@target(erlang)
pub fn try_resource_open_ok_closes_once_on_normal_end_test() {
  reset("c13_open_a")
  reset("c13_close_a")

  source.try_resource(
    open: fn() {
      bump("c13_open_a")
      Ok([1, 2])
    },
    next: fn(state) {
      case state {
        [] -> Done
        [head, ..tail] -> Next(element: Ok(head), state: tail)
      }
    },
    close: fn(_) {
      bump("c13_close_a")
      Nil
    },
  )
  |> fold.to_list
  |> should.equal([Ok(1), Ok(2)])

  get_dict("c13_open_a") |> should.equal(1)
  get_dict("c13_close_a") |> should.equal(1)
}

@target(erlang)
pub fn try_resource_open_error_does_not_call_close_test() {
  reset("c13_open_b")
  reset("c13_close_b")

  source.try_resource(
    open: fn() -> Result(Nil, String) {
      bump("c13_open_b")
      Error("oops")
    },
    next: fn(_) -> datastream.Step(Result(Int, String), Nil) { Done },
    close: fn(_) {
      bump("c13_close_b")
      Nil
    },
  )
  |> fold.to_list
  |> should.equal([Error(OpenError("oops"))])

  get_dict("c13_open_b") |> should.equal(1)
  get_dict("c13_close_b") |> should.equal(0)
}

@target(erlang)
pub fn try_resource_next_error_does_not_close_until_done_test() {
  reset("c13_open_c")
  reset("c13_close_c")

  source.try_resource(
    open: fn() {
      bump("c13_open_c")
      Ok(0)
    },
    next: fn(state) {
      case state {
        0 -> Next(element: Ok(1), state: 1)
        1 -> Next(element: Error("bad"), state: 2)
        2 -> Next(element: Ok(3), state: 3)
        _ -> Done
      }
    },
    close: fn(_) {
      bump("c13_close_c")
      Nil
    },
  )
  |> fold.to_list
  |> should.equal([Ok(1), Error(NextError("bad")), Ok(3)])

  get_dict("c13_open_c") |> should.equal(1)
  get_dict("c13_close_c") |> should.equal(1)
}

@target(erlang)
pub fn try_resource_re_run_re_attempts_open_test() {
  reset("c13_open_d")
  reset("c13_close_d")

  let s =
    source.try_resource(
      open: fn() -> Result(Nil, String) {
        bump("c13_open_d")
        Error("oops")
      },
      next: fn(_) -> datastream.Step(Result(Int, String), Nil) { Done },
      close: fn(_) {
        bump("c13_close_d")
        Nil
      },
    )

  fold.to_list(s) |> should.equal([Error(OpenError("oops"))])
  fold.to_list(s) |> should.equal([Error(OpenError("oops"))])

  get_dict("c13_open_d") |> should.equal(2)
  get_dict("c13_close_d") |> should.equal(0)
}

@target(erlang)
pub fn try_resource_take_closes_once_on_early_exit_test() {
  reset("c13_open_e")
  reset("c13_close_e")

  source.try_resource(
    open: fn() {
      bump("c13_open_e")
      Ok([1, 2, 3])
    },
    next: fn(state) {
      case state {
        [] -> Done
        [head, ..tail] -> Next(element: Ok(head), state: tail)
      }
    },
    close: fn(_) {
      bump("c13_close_e")
      Nil
    },
  )
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([Ok(1)])

  get_dict("c13_open_e") |> should.equal(1)
  get_dict("c13_close_e") |> should.equal(1)
}
