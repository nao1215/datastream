//// Tests for `source.resource`'s close contract.
////
//// Two flavours of test:
////
//// - Cross-target tests use a `panic`-as-tripwire pattern: the
////   resource's `close` is set to `Nil`, but a follow-up element installs
////   `panic`. If the close contract weren't honoured (resource closed
////   too early, or upstream pulled past an early exit) the panic would
////   trip and the test would fail.
//// - Erlang-only tests use the process dictionary as a counter to pin
////   precise open / close counts and ordering.

import datastream.{Done, Next}
import datastream/fold
import datastream/sink
import datastream/source
import datastream/stream
import gleam/option
import gleeunit/should

@target(erlang)
import datastream/binary

@target(erlang)
import datastream/text

// --- cross-target: behavioural tests ---------------------------------------

fn list_resource(elements: List(a)) {
  source.resource(
    open: fn() { elements },
    next: fn(state) {
      case state {
        [] -> Done
        [head, ..tail] -> Next(element: head, state: tail)
      }
    },
    close: fn(_) { Nil },
  )
}

pub fn resource_to_list_yields_source_order_test() {
  list_resource([1, 2, 3]) |> fold.to_list |> should.equal([1, 2, 3])
}

pub fn resource_take_one_yields_first_element_test() {
  list_resource([1, 2, 3])
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([1])
}

pub fn resource_first_returns_some_head_test() {
  list_resource([1, 2, 3]) |> fold.first |> should.equal(option.Some(1))
}

pub fn resource_re_runs_open_per_terminal_test() {
  let s = list_resource([1, 2, 3])
  fold.to_list(s) |> should.equal([1, 2, 3])
  fold.to_list(s) |> should.equal([1, 2, 3])
}

pub fn resource_on_empty_yields_empty_test() {
  list_resource([]) |> fold.to_list |> should.equal([])
}

// --- cross-target: short-circuit and ordering tripwires --------------------

pub fn map_passes_resource_through_test() {
  list_resource([1, 2, 3])
  |> stream.map(with: fn(x) { x + 1 })
  |> fold.to_list
  |> should.equal([2, 3, 4])
}

pub fn try_each_short_circuits_and_does_not_pull_past_error_test() {
  // After try_each returns Error("stop") on element 3, element 4 must
  // not be pulled (which would trip the panic).
  let s =
    source.resource(
      open: fn() { 1 },
      next: fn(state) {
        case state {
          n if n <= 4 -> Next(element: n, state: n + 1)
          _ -> Done
        }
      },
      close: fn(_) { Nil },
    )

  s
  |> sink.try_each(with: fn(x) {
    case x {
      3 -> Error("stop")
      4 -> panic as "try_each must not pull element 4 after error on 3"
      _ -> Ok(Nil)
    }
  })
  |> should.equal(Error("stop"))
}

pub fn append_does_not_open_second_when_first_short_circuits_test() {
  // The second resource's `open` panics; if `append` opened it before
  // the take(2) early-exit on the (infinite) first, the test would
  // panic.
  let infinite_first =
    source.resource(
      open: fn() { 1 },
      next: fn(state) { Next(element: state, state: state + 1) },
      close: fn(_) { Nil },
    )
  let panicking_second =
    source.resource(
      open: fn() {
        panic as "second resource opened despite take(2) early-exit"
      },
      next: fn(_) { Done },
      close: fn(_) { Nil },
    )

  stream.append(infinite_first, panicking_second)
  |> stream.take(up_to: 2)
  |> fold.to_list
  |> should.equal([1, 2])
}

// --- Erlang-only: precise counter assertions ------------------------------

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
@external(erlang, "erlang", "put")
fn put_events(key: String, value: List(String)) -> List(String)

@target(erlang)
@external(erlang, "erlang", "get")
fn get_events(key: String) -> List(String)

@target(erlang)
fn reset_events(key: String) -> Nil {
  let _previous = put_events(key, [])
  Nil
}

@target(erlang)
fn record(log_key: String, event: String) -> Nil {
  let current = get_events(log_key)
  let _previous = put_events(log_key, [event, ..current])
  Nil
}

@target(erlang)
fn events(log_key: String) -> List(String) {
  get_events(log_key) |> reverse_events
}

@target(erlang)
fn reverse_events(events: List(String)) -> List(String) {
  reverse_events_loop(events, [])
}

@target(erlang)
fn reverse_events_loop(
  remaining: List(String),
  acc: List(String),
) -> List(String) {
  case remaining {
    [] -> acc
    [head, ..tail] -> reverse_events_loop(tail, [head, ..acc])
  }
}

@target(erlang)
fn logged_resource(elements: List(a), log_key: String, name: String) {
  source.resource(
    open: fn() {
      record(log_key, "open:" <> name)
      elements
    },
    next: fn(state) {
      case state {
        [] -> Done
        [head, ..tail] -> Next(element: head, state: tail)
      }
    },
    close: fn(_) {
      record(log_key, "close:" <> name)
      Nil
    },
  )
}

@target(erlang)
fn counted_resource(elements: List(a), opens_key: String, closes_key: String) {
  source.resource(
    open: fn() {
      bump(opens_key)
      elements
    },
    next: fn(state) {
      case state {
        [] -> Done
        [head, ..tail] -> Next(element: head, state: tail)
      }
    },
    close: fn(_) {
      bump(closes_key)
      Nil
    },
  )
}

@target(erlang)
pub fn resource_normal_end_closes_once_test() {
  reset("c12_open_a")
  reset("c12_close_a")
  counted_resource([1, 2, 3], "c12_open_a", "c12_close_a")
  |> fold.to_list
  |> should.equal([1, 2, 3])
  get_dict("c12_open_a") |> should.equal(1)
  get_dict("c12_close_a") |> should.equal(1)
}

@target(erlang)
pub fn resource_take_early_exit_closes_once_test() {
  reset("c12_open_b")
  reset("c12_close_b")
  counted_resource([1, 2, 3], "c12_open_b", "c12_close_b")
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([1])
  get_dict("c12_open_b") |> should.equal(1)
  get_dict("c12_close_b") |> should.equal(1)
}

@target(erlang)
pub fn resource_first_closes_once_test() {
  reset("c12_open_c")
  reset("c12_close_c")
  counted_resource([1, 2, 3], "c12_open_c", "c12_close_c")
  |> fold.first
  |> should.equal(option.Some(1))
  get_dict("c12_open_c") |> should.equal(1)
  get_dict("c12_close_c") |> should.equal(1)
}

@target(erlang)
pub fn resource_each_terminal_re_opens_test() {
  reset("c12_open_d")
  reset("c12_close_d")
  let s = counted_resource([1, 2, 3], "c12_open_d", "c12_close_d")
  fold.to_list(s) |> should.equal([1, 2, 3])
  fold.to_list(s) |> should.equal([1, 2, 3])
  get_dict("c12_open_d") |> should.equal(2)
  get_dict("c12_close_d") |> should.equal(2)
}

@target(erlang)
pub fn resource_empty_closes_once_test() {
  reset("c12_open_e")
  reset("c12_close_e")
  counted_resource([], "c12_open_e", "c12_close_e")
  |> fold.to_list
  |> should.equal([])
  get_dict("c12_open_e") |> should.equal(1)
  get_dict("c12_close_e") |> should.equal(1)
}

@target(erlang)
pub fn resource_lazy_open_test() {
  reset("c12_open_f")
  reset("c12_close_f")
  let _pipeline = counted_resource([1, 2, 3], "c12_open_f", "c12_close_f")
  // No terminal yet → open must not have run
  get_dict("c12_open_f") |> should.equal(0)
  get_dict("c12_close_f") |> should.equal(0)
}

@target(erlang)
pub fn resource_flat_map_inner_then_outer_close_test() {
  reset("c12_open_outer")
  reset("c12_close_outer")
  reset("c12_open_inner_a")
  reset("c12_close_inner_a")
  reset("c12_open_inner_b")
  reset("c12_close_inner_b")

  let inner_a =
    counted_resource([1, 2], "c12_open_inner_a", "c12_close_inner_a")
  let inner_b =
    counted_resource([3, 4], "c12_open_inner_b", "c12_close_inner_b")
  let outer =
    counted_resource([inner_a, inner_b], "c12_open_outer", "c12_close_outer")

  outer
  |> stream.flat_map(with: fn(s) { s })
  |> fold.to_list
  |> should.equal([1, 2, 3, 4])

  get_dict("c12_open_outer") |> should.equal(1)
  get_dict("c12_close_outer") |> should.equal(1)
  get_dict("c12_open_inner_a") |> should.equal(1)
  get_dict("c12_close_inner_a") |> should.equal(1)
  get_dict("c12_open_inner_b") |> should.equal(1)
  get_dict("c12_close_inner_b") |> should.equal(1)
}

@target(erlang)
pub fn resource_flat_map_take_does_not_open_second_inner_test() {
  reset("c12_open_outer2")
  reset("c12_close_outer2")
  reset("c12_open_inner2_a")
  reset("c12_close_inner2_a")
  reset("c12_open_inner2_b")
  reset("c12_close_inner2_b")

  let inner_a =
    counted_resource([1, 2], "c12_open_inner2_a", "c12_close_inner2_a")
  let inner_b =
    counted_resource([3, 4], "c12_open_inner2_b", "c12_close_inner2_b")
  let outer =
    counted_resource([inner_a, inner_b], "c12_open_outer2", "c12_close_outer2")

  outer
  |> stream.flat_map(with: fn(s) { s })
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([1])

  get_dict("c12_open_inner2_a") |> should.equal(1)
  get_dict("c12_close_inner2_a") |> should.equal(1)
  get_dict("c12_open_inner2_b") |> should.equal(0)
  get_dict("c12_close_inner2_b") |> should.equal(0)
}

@target(erlang)
pub fn resource_append_left_closed_before_right_opens_test() {
  reset("c12_open_l")
  reset("c12_close_l")
  reset("c12_open_r")
  reset("c12_close_r")

  let left = counted_resource([1, 2], "c12_open_l", "c12_close_l")
  let right = counted_resource([3, 4], "c12_open_r", "c12_close_r")

  stream.append(left, right) |> fold.to_list |> should.equal([1, 2, 3, 4])

  get_dict("c12_open_l") |> should.equal(1)
  get_dict("c12_close_l") |> should.equal(1)
  get_dict("c12_open_r") |> should.equal(1)
  get_dict("c12_close_r") |> should.equal(1)
}

@target(erlang)
pub fn resource_zip_closes_both_test() {
  reset("c12_open_zl")
  reset("c12_close_zl")
  reset("c12_open_zr")
  reset("c12_close_zr")

  let left = counted_resource([1, 2], "c12_open_zl", "c12_close_zl")
  let right = counted_resource([10, 20, 30], "c12_open_zr", "c12_close_zr")

  stream.zip(left, right)
  |> fold.to_list
  |> should.equal([#(1, 10), #(2, 20)])

  get_dict("c12_open_zl") |> should.equal(1)
  get_dict("c12_close_zl") |> should.equal(1)
  get_dict("c12_open_zr") |> should.equal(1)
  get_dict("c12_close_zr") |> should.equal(1)
}

@target(erlang)
pub fn resource_try_each_error_closes_once_test() {
  reset("c12_open_te")
  reset("c12_close_te")

  counted_resource([1, 2, 3, 4], "c12_open_te", "c12_close_te")
  |> sink.try_each(with: fn(x) {
    case x < 3 {
      True -> Ok(Nil)
      False -> Error("stop")
    }
  })
  |> should.equal(Error("stop"))

  get_dict("c12_open_te") |> should.equal(1)
  get_dict("c12_close_te") |> should.equal(1)
}

// --- Erlang-only: close ordering assertions -------------------------------

@target(erlang)
pub fn resource_flat_map_closes_inner_before_outer_on_early_exit_test() {
  reset_events("c12_log_fmt_te")

  let inner = logged_resource([1, 2, 3], "c12_log_fmt_te", "inner")
  let outer = logged_resource([inner], "c12_log_fmt_te", "outer")

  outer
  |> stream.flat_map(with: fn(s) { s })
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([1])

  events("c12_log_fmt_te")
  |> should.equal(["open:outer", "open:inner", "close:inner", "close:outer"])
}

@target(erlang)
pub fn resource_flat_map_take_does_not_open_second_inner_ordering_test() {
  reset_events("c12_log_fmt_take")

  let inner_a = logged_resource([1, 2], "c12_log_fmt_take", "inner_a")
  let inner_b = logged_resource([3, 4], "c12_log_fmt_take", "inner_b")
  let outer = logged_resource([inner_a, inner_b], "c12_log_fmt_take", "outer")

  outer
  |> stream.flat_map(with: fn(s) { s })
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([1])

  // outer opened, then inner_a; on take(1) the active inner_a closes
  // first, then outer; inner_b is never touched.
  events("c12_log_fmt_take")
  |> should.equal(["open:outer", "open:inner_a", "close:inner_a", "close:outer"])
}

@target(erlang)
pub fn resource_append_left_fully_closes_before_right_opens_test() {
  reset_events("c12_log_append")

  let left = logged_resource([1, 2], "c12_log_append", "left")
  let right = logged_resource([3, 4], "c12_log_append", "right")

  stream.append(left, right) |> fold.to_list |> should.equal([1, 2, 3, 4])

  // left must close before right opens.
  events("c12_log_append")
  |> should.equal(["open:left", "close:left", "open:right", "close:right"])
}

@target(erlang)
pub fn resource_append_take_does_not_open_right_test() {
  reset_events("c12_log_append_take")

  let left =
    source.resource(
      open: fn() {
        record("c12_log_append_take", "open:left")
        1
      },
      next: fn(state) { Next(element: state, state: state + 1) },
      close: fn(_) {
        record("c12_log_append_take", "close:left")
        Nil
      },
    )
  let right = logged_resource([10, 20], "c12_log_append_take", "right")

  stream.append(left, right)
  |> stream.take(up_to: 2)
  |> fold.to_list
  |> should.equal([1, 2])

  events("c12_log_append_take")
  |> should.equal(["open:left", "close:left"])
}

@target(erlang)
pub fn resource_zip_closes_right_then_left_test() {
  reset_events("c12_log_zip")

  let left = logged_resource([1, 2], "c12_log_zip", "left")
  let right = logged_resource([10, 20, 30], "c12_log_zip", "right")

  stream.zip(left, right)
  |> fold.to_list
  |> should.equal([#(1, 10), #(2, 20)])

  // Order in the log: opens happen during the first zip pull (left
  // first, then right). Then on the third pull, left returns Done and
  // closes itself, after which zip closes the still-active right.
  // Spec: "close right then left" applies when the zip combinator
  // itself is asked to close — here the left already closed via Done,
  // so only right's close is recorded after left's.
  events("c12_log_zip")
  |> should.equal(["open:left", "open:right", "close:left", "close:right"])
}

@target(erlang)
pub fn resource_fold_find_closes_once_on_match_test() {
  reset("c12_open_find")
  reset("c12_close_find")
  counted_resource([1, 2, 3, 4], "c12_open_find", "c12_close_find")
  |> fold.find(satisfying: fn(x) { x == 2 })
  |> should.equal(option.Some(2))
  get_dict("c12_open_find") |> should.equal(1)
  get_dict("c12_close_find") |> should.equal(1)
}

@target(erlang)
pub fn resource_fold_any_closes_once_on_first_true_test() {
  reset("c12_open_any")
  reset("c12_close_any")
  counted_resource([1, 2, 3, 4], "c12_open_any", "c12_close_any")
  |> fold.any(satisfying: fn(x) { x >= 2 })
  |> should.equal(True)
  get_dict("c12_open_any") |> should.equal(1)
  get_dict("c12_close_any") |> should.equal(1)
}

@target(erlang)
pub fn resource_fold_all_closes_once_on_first_false_test() {
  reset("c12_open_all")
  reset("c12_close_all")
  counted_resource([1, 2, 3, 4], "c12_open_all", "c12_close_all")
  |> fold.all(satisfying: fn(x) { x < 2 })
  |> should.equal(False)
  get_dict("c12_open_all") |> should.equal(1)
  get_dict("c12_close_all") |> should.equal(1)
}

@target(erlang)
pub fn resource_fold_collect_result_closes_once_on_first_error_test() {
  reset("c12_open_cr")
  reset("c12_close_cr")
  counted_resource(
    [Ok(1), Ok(2), Error("bad"), Ok(3)],
    "c12_open_cr",
    "c12_close_cr",
  )
  |> fold.collect_result
  |> should.equal(Error("bad"))
  get_dict("c12_open_cr") |> should.equal(1)
  get_dict("c12_close_cr") |> should.equal(1)
}

// --- Erlang-only: close contract through chunk-aware combinators --------
//
// Each chunk-boundary-aware combinator owns its own close callback that
// forwards to the wrapped source. These tests catch regressions where
// the forwarding breaks — a class of bug that per-combinator tests in
// isolation do not reveal.

@target(erlang)
pub fn lines_over_resource_take_one_closes_once_test() {
  reset("c14_open_lines_take")
  reset("c14_close_lines_take")
  counted_resource(
    ["hel", "lo\nwor", "ld\n"],
    "c14_open_lines_take",
    "c14_close_lines_take",
  )
  |> text.lines
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal(["hello"])
  get_dict("c14_open_lines_take") |> should.equal(1)
  get_dict("c14_close_lines_take") |> should.equal(1)
}

@target(erlang)
pub fn lines_over_resource_fold_find_closes_once_test() {
  reset("c14_open_lines_find")
  reset("c14_close_lines_find")
  counted_resource(
    ["alpha\nbeta\n", "gamma\n"],
    "c14_open_lines_find",
    "c14_close_lines_find",
  )
  |> text.lines
  |> fold.find(satisfying: fn(line) { line == "beta" })
  |> should.equal(option.Some("beta"))
  get_dict("c14_open_lines_find") |> should.equal(1)
  get_dict("c14_close_lines_find") |> should.equal(1)
}

@target(erlang)
pub fn delimited_over_resource_take_one_closes_once_test() {
  reset("c14_open_delim")
  reset("c14_close_delim")
  counted_resource(
    [<<65, 0, 66, 0, 67, 0>>],
    "c14_open_delim",
    "c14_close_delim",
  )
  |> binary.delimited(on: <<0>>)
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([<<65>>])
  get_dict("c14_open_delim") |> should.equal(1)
  get_dict("c14_close_delim") |> should.equal(1)
}

@target(erlang)
pub fn length_prefixed_over_resource_take_one_closes_once_test() {
  reset("c14_open_lp")
  reset("c14_close_lp")
  counted_resource([<<2, 65, 66, 1, 67>>], "c14_open_lp", "c14_close_lp")
  |> binary.length_prefixed(prefix_size: 1)
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([Ok(<<65, 66>>)])
  get_dict("c14_open_lp") |> should.equal(1)
  get_dict("c14_close_lp") |> should.equal(1)
}

@target(erlang)
pub fn fixed_size_over_resource_take_one_closes_once_test() {
  reset("c14_open_fs")
  reset("c14_close_fs")
  counted_resource([<<1, 2, 3, 4, 5, 6>>], "c14_open_fs", "c14_close_fs")
  |> binary.fixed_size(size: 2)
  |> stream.take(up_to: 1)
  |> fold.to_list
  |> should.equal([<<1, 2>>])
  get_dict("c14_open_fs") |> should.equal(1)
  get_dict("c14_close_fs") |> should.equal(1)
}
