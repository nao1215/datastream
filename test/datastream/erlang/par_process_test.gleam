//// BEAM-only tests for how `datastream/erlang/par` treats processes,
//// monitors and the caller's mailbox: worker crashes, upstream panics,
//// re-running a stream and early exit.

@target(erlang)
import datastream

@target(erlang)
import datastream/erlang/internal/event_log

@target(erlang)
import datastream/erlang/internal/watchdog.{Crashed, Finished}

@target(erlang)
import gleam/erlang/process

@target(erlang)
import gleam/int

@target(erlang)
import gleam/list

@target(erlang)
import gleeunit/should

@target(javascript)
pub const beam_only_marker: String =
  "datastream/erlang/par_process_test is BEAM-only"

@target(erlang)
fn numbers(count: Int) -> List(Int) {
  int.range(from: count - 1, to: -1, with: [], run: fn(acc, i) { [i, ..acc] })
}

// --- the caller's own monitors are left alone ----------------------------

@target(erlang)
fn keeps_callers_down_message(
  run: fn(fn(Int) -> Int) -> List(Int),
) -> watchdog.Outcome(#(List(Int), Bool)) {
  use <- watchdog.run(within: 3000)
  let pid =
    process.spawn_unlinked(fn() {
      process.sleep(20)
      panic as "the caller's own process"
    })
  let monitor = process.monitor(pid)
  let result =
    run(fn(x) {
      process.sleep(50)
      x
    })
  let own_down =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
    |> process.selector_receive(within: 1000)
  #(list.sort(result, int.compare), own_down == Ok(Nil))
}

@target(erlang)
pub fn map_unordered_leaves_callers_down_messages_alone_test() {
  keeps_callers_down_message(fn(f) {
    datastream.range(from: 0, to: 10)
    |> datastream.map_unordered(with: f, max_workers: 4, max_buffer: 16)
    |> should.be_ok
    |> datastream.to_list
  })
  |> should.equal(Finished(#([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], True)))
}

@target(erlang)
pub fn map_ordered_leaves_callers_down_messages_alone_test() {
  keeps_callers_down_message(fn(f) {
    datastream.range(from: 0, to: 10)
    |> datastream.map_ordered(with: f, max_workers: 4, max_buffer: 16)
    |> should.be_ok
    |> datastream.to_list
  })
  |> should.equal(Finished(#([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], True)))
}

// --- a panicking f is reported, not hidden -------------------------------

@target(erlang)
fn crash_first_sleep_rest(x: Int) -> Int {
  case x {
    0 -> panic as "f(0)"
    _ -> {
      process.sleep(5)
      x
    }
  }
}

@target(erlang)
pub fn map_unordered_with_crash_next_to_a_fast_result_does_not_hang_test() {
  watchdog.run(within: 3000, running: fn() {
    datastream.from_list([0, 1])
    |> datastream.map_unordered(
      with: crash_first_sleep_rest,
      max_workers: 2,
      max_buffer: 2,
    )
    |> should.be_ok
    |> datastream.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn map_ordered_with_crash_next_to_a_fast_result_does_not_hang_test() {
  watchdog.run(within: 3000, running: fn() {
    datastream.from_list([0, 1])
    |> datastream.map_ordered(
      with: crash_first_sleep_rest,
      max_workers: 2,
      max_buffer: 2,
    )
    |> should.be_ok
    |> datastream.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn map_ordered_with_closes_upstream_when_f_panics_test() {
  let log = event_log.new_log()
  let outcome =
    watchdog.run(within: 3000, running: fn() {
      event_log.counted_resource(numbers(10), named: "src", log: log)
      |> datastream.map_ordered(
        with: fn(x) {
          case x {
            2 -> panic as "f(2)"
            _ -> x
          }
        },
        max_workers: 1,
        max_buffer: 1,
      )
      |> should.be_ok
      |> datastream.to_list
    })
  outcome |> should.equal(Crashed)
  event_log.drain(log, within: 100)
  |> event_log.count_closes
  |> should.equal(1)
}

// --- each run gets its own workers and messages --------------------------

@target(erlang)
pub fn map_unordered_rerun_after_early_exit_sees_only_its_own_results_test() {
  watchdog.run(within: 3000, running: fn() {
    let s =
      datastream.range(from: 0, to: 10)
      |> datastream.map_unordered(
        with: fn(x) {
          process.sleep(x * 5)
          x
        },
        max_workers: 4,
        max_buffer: 16,
      )
      |> should.be_ok
    let first = s |> datastream.take(up_to: 1) |> datastream.to_list
    let second = datastream.to_list(s) |> list.sort(int.compare)
    process.sleep(100)
    #(first, second, watchdog.mailbox_is_empty())
  })
  |> should.equal(Finished(#([0], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], True)))
}

@target(erlang)
pub fn map_ordered_runs_in_a_process_other_than_the_builder_test() {
  let s =
    datastream.range(from: 0, to: 5)
    |> datastream.map_ordered(
      with: fn(x) { x * 2 },
      max_workers: 4,
      max_buffer: 16,
    )
    |> should.be_ok
  watchdog.run(within: 3000, running: fn() { datastream.to_list(s) })
  |> should.equal(Finished([0, 2, 4, 6, 8]))
}

// --- merge and race ------------------------------------------------------

@target(erlang)
fn panicking_source() {
  datastream.unfold(from: Nil, with: fn(_) { panic as "upstream" })
}

@target(erlang)
pub fn merge_reports_a_panicking_upstream_test() {
  watchdog.run(within: 3000, running: fn() {
    datastream.merge(
      [datastream.from_list([1]), panicking_source()],
      max_buffer: 16,
    )
    |> should.be_ok
    |> datastream.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn merge_closes_the_other_upstreams_when_one_panics_test() {
  let log = event_log.new_log()
  let outcome =
    watchdog.run(within: 3000, running: fn() {
      datastream.merge(
        [
          event_log.counted_resource(numbers(1000), named: "a", log: log)
            |> datastream.tap(fn(_) { process.sleep(1) }),
          panicking_source(),
        ],
        max_buffer: 16,
      )
      |> should.be_ok
      |> datastream.to_list
    })
  outcome |> should.equal(Crashed)
  event_log.drain(log, within: 200)
  |> event_log.count_closes
  |> should.equal(1)
}

@target(erlang)
pub fn race_reports_a_panicking_upstream_test() {
  watchdog.run(within: 3000, running: fn() {
    datastream.race([datastream.empty(), panicking_source()])
    |> datastream.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn merge_does_not_pull_before_a_terminal_runs_test() {
  let seen = process.new_subject()
  let _merged =
    datastream.merge(
      [datastream.from_list([1, 2]) |> datastream.tap(process.send(seen, _))],
      max_buffer: 16,
    )
    |> should.be_ok
  process.receive(from: seen, within: 100) |> should.equal(Error(Nil))
}

@target(erlang)
pub fn race_does_not_pull_before_a_terminal_runs_test() {
  let seen = process.new_subject()
  let _raced =
    datastream.race([
      datastream.from_list([1, 2]) |> datastream.tap(process.send(seen, _)),
    ])
  process.receive(from: seen, within: 100) |> should.equal(Error(Nil))
}

@target(erlang)
pub fn merge_can_run_twice_test() {
  watchdog.run(within: 3000, running: fn() {
    let s =
      datastream.merge(
        [datastream.from_list([1, 2]), datastream.from_list([3])],
        max_buffer: 16,
      )
      |> should.be_ok
    let first = datastream.to_list(s) |> list.sort(int.compare)
    let second = datastream.to_list(s) |> list.sort(int.compare)
    #(first, second)
  })
  |> should.equal(Finished(#([1, 2, 3], [1, 2, 3])))
}

@target(erlang)
pub fn race_can_run_twice_test() {
  watchdog.run(within: 3000, running: fn() {
    let s = datastream.race([datastream.from_list([1, 2])])
    #(datastream.to_list(s), datastream.to_list(s))
  })
  |> should.equal(Finished(#([1, 2], [1, 2])))
}

@target(erlang)
pub fn merge_leaves_no_messages_after_early_exit_test() {
  watchdog.run(within: 3000, running: fn() {
    let taken =
      datastream.merge(
        [datastream.repeat(1), datastream.repeat(2)],
        max_buffer: 16,
      )
      |> should.be_ok
      |> datastream.take(up_to: 5)
      |> datastream.count
    process.sleep(100)
    #(taken, watchdog.mailbox_is_empty())
  })
  |> should.equal(Finished(#(5, True)))
}

// --- workers stop when the run ends abnormally -----------------------------

@target(erlang)
@external(erlang, "datastream_test_rescue_ffi", "rescue")
fn rescue(thunk: fn() -> a) -> Result(a, Nil)

@target(erlang)
fn panics_after_four() {
  datastream.unfold(from: 0, with: fn(i) {
    case i < 4 {
      True -> datastream.Next(i, i + 1)
      False -> panic as "upstream"
    }
  })
}

@target(erlang)
fn slow_by_index(x: Int) -> Int {
  process.sleep(50 + x * 100)
  x
}

@target(erlang)
pub fn map_unordered_stops_its_tasks_when_the_upstream_panics_test() {
  watchdog.run(within: 3000, running: fn() {
    let outcome =
      rescue(fn() {
        panics_after_four()
        |> datastream.map_unordered(
          with: slow_by_index,
          max_workers: 4,
          max_buffer: 4,
        )
        |> should.be_ok
        |> datastream.to_list
      })
    process.sleep(500)
    #(outcome, watchdog.mailbox_is_empty())
  })
  |> should.equal(Finished(#(Error(Nil), True)))
}

@target(erlang)
pub fn each_unordered_stops_its_effects_when_the_upstream_panics_test() {
  let log = process.new_subject()
  let outcome =
    watchdog.run(within: 3000, running: fn() {
      panics_after_four()
      |> datastream.each_unordered(
        with: fn(x) { process.send(log, slow_by_index(x)) },
        max_workers: 4,
        max_buffer: 4,
      )
      |> should.be_ok
    })
  outcome |> should.equal(Crashed)
  process.sleep(500)
  drain_ints(log, []) |> should.equal([0])
}

@target(erlang)
pub fn map_tasks_stop_when_the_consuming_process_dies_test() {
  let log = process.new_subject()
  let consumer =
    process.spawn_unlinked(fn() {
      datastream.range(from: 0, to: 4)
      |> datastream.each_unordered(
        with: fn(x) {
          process.sleep(200)
          process.send(log, x)
        },
        max_workers: 4,
        max_buffer: 16,
      )
      |> should.be_ok
    })
  process.sleep(50)
  process.kill(consumer)
  process.sleep(400)
  drain_ints(log, []) |> should.equal([])
}

@target(erlang)
fn drain_ints(log: process.Subject(Int), acc: List(Int)) -> List(Int) {
  case process.receive(from: log, within: 0) {
    Ok(x) -> drain_ints(log, [x, ..acc])
    Error(_) -> list.reverse(acc)
  }
}
