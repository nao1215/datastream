//// BEAM-only tests for how `datastream/erlang/par` treats processes,
//// monitors and the caller's mailbox: worker crashes, upstream panics,
//// re-running a stream and early exit.

@target(erlang)
import datastream/erlang/internal/event_log

@target(erlang)
import datastream/erlang/internal/watchdog.{Crashed, Finished}

@target(erlang)
import datastream/erlang/par

@target(erlang)
import datastream/fold

@target(erlang)
import datastream/source

@target(erlang)
import datastream/stream

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
    source.range(from: 0, to: 10)
    |> par.map_unordered(with: f)
    |> fold.to_list
  })
  |> should.equal(Finished(#([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], True)))
}

@target(erlang)
pub fn map_ordered_leaves_callers_down_messages_alone_test() {
  keeps_callers_down_message(fn(f) {
    source.range(from: 0, to: 10)
    |> par.map_ordered(with: f)
    |> fold.to_list
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
    source.from_list([0, 1])
    |> par.map_unordered_with(
      with: crash_first_sleep_rest,
      max_workers: 2,
      max_buffer: 2,
    )
    |> fold.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn map_ordered_with_crash_next_to_a_fast_result_does_not_hang_test() {
  watchdog.run(within: 3000, running: fn() {
    source.from_list([0, 1])
    |> par.map_ordered_with(
      with: crash_first_sleep_rest,
      max_workers: 2,
      max_buffer: 2,
    )
    |> fold.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn map_ordered_with_closes_upstream_when_f_panics_test() {
  let log = event_log.new_log()
  let outcome =
    watchdog.run(within: 3000, running: fn() {
      event_log.counted_resource(numbers(10), named: "src", log: log)
      |> par.map_ordered_with(
        with: fn(x) {
          case x {
            2 -> panic as "f(2)"
            _ -> x
          }
        },
        max_workers: 1,
        max_buffer: 1,
      )
      |> fold.to_list
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
      source.range(from: 0, to: 10)
      |> par.map_unordered(with: fn(x) {
        process.sleep(x * 5)
        x
      })
    let first = s |> stream.take(up_to: 1) |> fold.to_list
    let second = fold.to_list(s) |> list.sort(int.compare)
    process.sleep(100)
    #(first, second, watchdog.mailbox_is_empty())
  })
  |> should.equal(Finished(#([0], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], True)))
}

@target(erlang)
pub fn map_ordered_runs_in_a_process_other_than_the_builder_test() {
  let s = source.range(from: 0, to: 5) |> par.map_ordered(with: fn(x) { x * 2 })
  watchdog.run(within: 3000, running: fn() { fold.to_list(s) })
  |> should.equal(Finished([0, 2, 4, 6, 8]))
}

// --- merge and race ------------------------------------------------------

@target(erlang)
fn panicking_source() {
  source.unfold(from: Nil, with: fn(_) { panic as "upstream" })
}

@target(erlang)
pub fn merge_reports_a_panicking_upstream_test() {
  watchdog.run(within: 3000, running: fn() {
    par.merge([source.from_list([1]), panicking_source()]) |> fold.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn merge_closes_the_other_upstreams_when_one_panics_test() {
  let log = event_log.new_log()
  let outcome =
    watchdog.run(within: 3000, running: fn() {
      par.merge([
        event_log.counted_resource(numbers(1000), named: "a", log: log)
          |> stream.tap(fn(_) { process.sleep(1) }),
        panicking_source(),
      ])
      |> fold.to_list
    })
  outcome |> should.equal(Crashed)
  event_log.drain(log, within: 200)
  |> event_log.count_closes
  |> should.equal(1)
}

@target(erlang)
pub fn race_reports_a_panicking_upstream_test() {
  watchdog.run(within: 3000, running: fn() {
    par.race([source.empty(), panicking_source()]) |> fold.to_list
  })
  |> should.equal(Crashed)
}

@target(erlang)
pub fn merge_does_not_pull_before_a_terminal_runs_test() {
  let seen = process.new_subject()
  let _merged =
    par.merge([source.from_list([1, 2]) |> stream.tap(process.send(seen, _))])
  process.receive(from: seen, within: 100) |> should.equal(Error(Nil))
}

@target(erlang)
pub fn race_does_not_pull_before_a_terminal_runs_test() {
  let seen = process.new_subject()
  let _raced =
    par.race([source.from_list([1, 2]) |> stream.tap(process.send(seen, _))])
  process.receive(from: seen, within: 100) |> should.equal(Error(Nil))
}

@target(erlang)
pub fn merge_can_run_twice_test() {
  watchdog.run(within: 3000, running: fn() {
    let s = par.merge([source.from_list([1, 2]), source.from_list([3])])
    let first = fold.to_list(s) |> list.sort(int.compare)
    let second = fold.to_list(s) |> list.sort(int.compare)
    #(first, second)
  })
  |> should.equal(Finished(#([1, 2, 3], [1, 2, 3])))
}

@target(erlang)
pub fn race_can_run_twice_test() {
  watchdog.run(within: 3000, running: fn() {
    let s = par.race([source.from_list([1, 2])])
    #(fold.to_list(s), fold.to_list(s))
  })
  |> should.equal(Finished(#([1, 2], [1, 2])))
}

@target(erlang)
pub fn merge_leaves_no_messages_after_early_exit_test() {
  watchdog.run(within: 3000, running: fn() {
    let taken =
      par.merge([source.repeat(1), source.repeat(2)])
      |> stream.take(up_to: 5)
      |> fold.count
    process.sleep(100)
    #(taken, watchdog.mailbox_is_empty())
  })
  |> should.equal(Finished(#(5, True)))
}
