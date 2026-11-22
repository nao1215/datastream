//// BEAM-only tests for how `datastream/erlang/time` and
//// `datastream/erlang/source.timeout` use their pump process: no work
//// before a terminal runs, re-running, backpressure, and closing.

@target(erlang)
import datastream.{type Stream}

@target(erlang)
import datastream/chunk

@target(erlang)
import datastream/erlang/internal/event_log

@target(erlang)
import datastream/erlang/internal/watchdog.{Crashed, Finished}

@target(erlang)
import datastream/erlang/par

@target(erlang)
import datastream/erlang/source as esource

@target(erlang)
import datastream/erlang/time

@target(erlang)
import datastream/fold

@target(erlang)
import datastream/source

@target(erlang)
import datastream/stream

@target(erlang)
import gleam/erlang/process

@target(erlang)
import gleam/list

@target(erlang)
import gleeunit/should

@target(javascript)
pub const beam_only_marker: String =
  "datastream/erlang/time_process_test is BEAM-only"

@target(erlang)
fn combinators() -> List(fn(Stream(Int)) -> Stream(Int)) {
  [
    time.throttle(_, every: 1),
    time.debounce(_, quiet_for: 1),
    time.sample(_, every: 5),
    time.rate_limit(_, max_per_window: 100, window_ms: 10),
    fn(s) {
      time.window_time(s, span: 5)
      |> stream.flat_map(fn(c) { source.from_list(chunk.to_list(c)) })
    },
  ]
}

@target(erlang)
pub fn time_combinators_do_not_pull_before_a_terminal_runs_test() {
  list.each(combinators(), fn(combinator) {
    let seen = process.new_subject()
    let _built =
      source.from_list([1, 2])
      |> stream.tap(process.send(seen, _))
      |> combinator
    process.receive(from: seen, within: 50) |> should.equal(Error(Nil))
  })
}

@target(erlang)
pub fn time_combinators_can_run_twice_test() {
  list.each(combinators(), fn(combinator) {
    watchdog.run(within: 3000, running: fn() {
      let s = source.from_list([1, 2, 3]) |> combinator
      let first = fold.to_list(s) != []
      let second = fold.to_list(s) != []
      #(first, second)
    })
    |> should.equal(Finished(#(True, True)))
  })
}

@target(erlang)
pub fn throttle_does_not_flood_the_mailbox_test() {
  watchdog.run(within: 3000, running: fn() {
    let taken =
      source.repeat(1)
      |> time.throttle(every: 200)
      |> stream.take(up_to: 1)
      |> fold.to_list
    process.sleep(100)
    #(taken, watchdog.mailbox_is_empty())
  })
  |> should.equal(Finished(#([1], True)))
}

@target(erlang)
pub fn throttle_reports_a_panicking_upstream_test() {
  watchdog.run(within: 3000, running: fn() {
    source.unfold(from: Nil, with: fn(_) { panic as "upstream" })
    |> time.throttle(every: 1)
    |> fold.to_list
  })
  |> should.equal(Crashed)
}

// --- source.timeout ------------------------------------------------------

@target(erlang)
pub fn timeout_pulls_every_element_in_one_process_test() {
  // A resource whose handle belongs to the process that opened it (an
  // ETS table, a port, a file) must keep being pulled by that process.
  let pids = process.new_subject()
  let outcome =
    watchdog.run(within: 3000, running: fn() {
      source.resource(
        open: fn() { 0 },
        next: fn(i) {
          process.send(pids, process.self())
          case i < 3 {
            True -> datastream.Next(i, i + 1)
            False -> datastream.Done
          }
        },
        close: fn(_) { Nil },
      )
      |> esource.timeout(within: 500)
      |> fold.to_list
    })
  outcome |> should.equal(Finished([Ok(0), Ok(1), Ok(2)]))
  let seen = drain_pids(pids, [])
  list.length(seen) |> should.equal(4)
  list.unique(seen) |> list.length |> should.equal(1)
}

@target(erlang)
fn drain_pids(
  pids: process.Subject(process.Pid),
  acc: List(process.Pid),
) -> List(process.Pid) {
  case process.receive(from: pids, within: 0) {
    Ok(pid) -> drain_pids(pids, [pid, ..acc])
    Error(_) -> acc
  }
}

@target(erlang)
pub fn timeout_closes_the_upstream_when_the_deadline_trips_test() {
  let log = event_log.new_log()
  let outcome =
    watchdog.run(within: 3000, running: fn() {
      event_log.counted_resource([1, 2, 3], named: "slow", log: log)
      |> stream.tap(fn(x) {
        case x {
          2 -> process.sleep(200)
          _ -> Nil
        }
      })
      |> esource.timeout(within: 50)
      |> fold.to_list
    })
  outcome |> should.equal(Finished([Ok(1), Error(Nil)]))
  event_log.drain(log, within: 500)
  |> event_log.count_closes
  |> should.equal(1)
}

// --- a subject stream pulled in a worker fails loudly ----------------------

@target(erlang)
fn two_message_subject_stream() -> Stream(Int) {
  let subject = process.new_subject()
  process.send(subject, 1)
  process.send(subject, 2)
  let remaining = process.new_subject()
  process.send(remaining, 2)
  esource.from_subject(from: subject, while: fn() {
    case process.receive(from: remaining, within: 0) {
      Ok(n) if n > 0 -> {
        process.send(remaining, n - 1)
        True
      }
      _ -> False
    }
  })
}

@target(erlang)
pub fn throttle_of_a_subject_stream_panics_instead_of_hanging_test() {
  watchdog.run(within: 3000, running: fn() {
    two_message_subject_stream() |> time.throttle(every: 1) |> fold.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn merge_of_a_subject_stream_panics_instead_of_hanging_test() {
  watchdog.run(within: 3000, running: fn() {
    par.merge([two_message_subject_stream()]) |> fold.to_list
  })
  |> should.equal(Crashed)
}
