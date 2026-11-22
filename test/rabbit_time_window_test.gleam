@target(erlang)
import datastream.{Done, Next}
@target(erlang)
import datastream/erlang/internal/watchdog.{Finished}
@target(erlang)
import gleam/erlang/atom
@target(erlang)
import gleam/erlang/process.{type Subject}
@target(erlang)
import gleam/int
@target(erlang)
import gleam/list
@target(erlang)
import gleeunit/should

@target(javascript)
pub const erlang_only_marker = "Erlang-only"

@target(erlang)
const span = 400

@target(erlang)
@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: atom.Atom) -> Int

@target(erlang)
fn now() -> Int {
  monotonic_time(atom.create("millisecond"))
}

@target(erlang)
fn wait_until(deadline: Int) {
  case deadline - now() {
    remaining if remaining > 0 -> {
      process.sleep(remaining)
      wait_until(deadline)
    }
    _ -> Nil
  }
}

@target(erlang)
fn release_at(gate: Subject(Nil), deadline: Int) {
  let _timer = process.send_after(gate, int.max(0, deadline - now()), Nil)
  Nil
}

@target(erlang)
fn drain(subject: Subject(a)) -> List(a) {
  case process.receive(subject, within: 0) {
    Ok(value) -> [value, ..drain(subject)]
    Error(_) -> []
  }
}

@target(erlang)
fn assert_closed_once(
  monitor: process.Monitor,
  closed: Subject(Int),
  returned: Subject(#(Int, Int)),
) -> List(#(Int, Int)) {
  let assert Ok(closed_state) = process.receive(closed, within: 1500)
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(within: 1500)
  process.receive(closed, within: 0) |> should.equal(Error(Nil))
  let returns = drain(returned)
  let assert Ok(#(last_state, _)) = list.last(returns)
  closed_state |> should.equal(last_state + 1)
  returns
}

@target(erlang)
fn window_case(late: Bool) {
  let ready = process.new_subject()
  let worker_seen = process.new_subject()
  let closed = process.new_subject()
  let returned = process.new_subject()
  let source =
    datastream.resource(
      open: fn() {
        let gate = process.new_subject()
        process.send(ready, #(gate, now(), process.self()))
        #(gate, 0)
      },
      next: fn(state) {
        let #(gate, index) = state
        case index {
          0 -> {
            let assert Ok(Nil) = process.receive(gate, within: 2000)
            process.send(returned, #(index, now()))
            Next(1, #(gate, 1))
          }
          _ -> Done
        }
      },
      close: fn(state) { process.send(closed, state.1) },
    )
  let before = now()
  let assert Ok(windows) = datastream.window_time(source, span: span)
  let values =
    windows
    |> datastream.map_with_state(from: 0, with: fn(index, batch) {
      case index {
        0 -> {
          let assert Ok(#(gate, opened, worker)) =
            process.receive(ready, within: 1000)
          assert opened - before < span / 8
            as "fixture startup exceeded its margin"
          let window = case late {
            True -> 2
            False -> 1
          }
          release_at(gate, opened + window * span + span / 4)
          process.send(worker_seen, #(process.monitor(worker), opened))
          wait_until(opened + 2 * span + span / 2)
        }
        _ -> Nil
      }
      #(index + 1, batch)
    })
    |> datastream.take(up_to: case late {
      True -> 3
      False -> 2
    })
    |> datastream.to_list
  let assert Ok(#(monitor, opened)) = process.receive(worker_seen, within: 0)
  let assert [#(0, released)] = assert_closed_once(monitor, closed, returned)
  let window = case late {
    True -> 2
    False -> 1
  }
  assert released >= opened + window * span
    as "fixture released before its window"
  assert released < before + { window + 1 } * span
    as "fixture missed its arrival window"
  values
}

@target(erlang)
pub fn late_reply_is_not_added_to_an_expired_window_test() {
  watchdog.run(within: 4500, running: fn() { window_case(True) })
  |> should.equal(Finished([[], [], [1]]))
}

@target(erlang)
pub fn early_reply_keeps_its_window_when_downstream_resumes_late_test() {
  watchdog.run(within: 4500, running: fn() { window_case(False) })
  |> should.equal(Finished([[], [1]]))
}

@target(erlang)
fn await_sample_gate(index: Int, first: Subject(Nil), second: Subject(Nil)) {
  case index {
    1 -> {
      let assert Ok(Nil) = process.receive(first, within: 2000)
      Nil
    }
    2 -> {
      let assert Ok(Nil) = process.receive(second, within: 2000)
      Nil
    }
    _ -> Nil
  }
}

@target(erlang)
fn sample_case(late: Bool) {
  let ready = process.new_subject()
  let worker_seen = process.new_subject()
  let closed = process.new_subject()
  let returned = process.new_subject()
  let source =
    datastream.resource(
      open: fn() {
        let first = process.new_subject()
        let second = process.new_subject()
        process.send(ready, #(first, second, now(), process.self()))
        #(first, second, 0)
      },
      next: fn(state) {
        let #(first, second, index) = state
        case index {
          0 | 1 | 2 -> {
            await_sample_gate(index, first, second)
            process.send(returned, #(index, now()))
            Next(index, #(first, second, index + 1))
          }
          _ -> Done
        }
      },
      close: fn(state) { process.send(closed, state.2) },
    )
  let before = now()
  let assert Ok(sampled) = datastream.sample(source, every: span)
  let values =
    sampled
    |> datastream.map_with_state(from: 0, with: fn(index, value) {
      case index {
        0 -> {
          let assert Ok(#(first, second, opened, worker)) =
            process.receive(ready, within: 1000)
          assert opened - before < span / 8
            as "fixture startup exceeded its margin"
          let first_window = case late {
            True -> 2
            False -> 1
          }
          release_at(first, opened + first_window * span + span / 4)
          release_at(second, opened + 2 * span + 3 * span / 4)
          process.send(worker_seen, #(process.monitor(worker), opened))
          wait_until(opened + 2 * span + span / 2)
        }
        _ -> Nil
      }
      #(index + 1, value)
    })
    |> datastream.take(up_to: 2)
    |> datastream.to_list
  let assert Ok(#(monitor, opened)) = process.receive(worker_seen, within: 0)
  let returns = assert_closed_once(monitor, closed, returned)
  let assert [#(0, initial), #(1, released), ..] = returns
  assert initial < before + span as "fixture missed its initial window"
  let first_window = case late {
    True -> 2
    False -> 1
  }
  assert released >= opened + first_window * span
    as "fixture released before its window"
  assert released < before + { first_window + 1 } * span
    as "fixture missed its arrival window"
  case list.find(returns, fn(entry) { entry.0 == 2 }) {
    Ok(#(_, replacement)) -> {
      assert replacement >= opened + 2 * span
      assert replacement < before + 3 * span
        as "fixture missed replacement window"
    }
    Error(_) -> Nil
  }
  values
}

@target(erlang)
pub fn sample_uses_latest_reply_in_the_actual_arrival_window_test() {
  watchdog.run(within: 4500, running: fn() { sample_case(True) })
  |> should.equal(Finished([0, 2]))
}

@target(erlang)
pub fn sample_preserves_an_early_reply_consumed_after_its_deadline_test() {
  watchdog.run(within: 4500, running: fn() { sample_case(False) })
  |> should.equal(Finished([0, 1]))
}
