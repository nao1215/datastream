@target(erlang)
import datastream.{type Stream, Next}

@target(erlang)
import datastream/erlang/internal/event_log

@target(erlang)
import datastream/erlang/internal/watchdog.{Finished}

@target(erlang)
import gleam/erlang/process.{type Pid, type Subject}

@target(erlang)
import gleam/int

@target(erlang)
import gleam/list

@target(erlang)
import gleam/result

@target(erlang)
import gleeunit/should

@target(javascript)
pub const beam_only_marker: String =
  "datastream_erlang_redesign_test is BEAM-only"

@target(erlang)
pub fn duration_constructors_validate_boundaries_without_opening_sources_test() {
  let constructors = [
    #("ticks", False, fn(_, milliseconds) {
      datastream.ticks(every: milliseconds) |> result.map(fn(_) { Nil })
    }),
    #("interval", False, fn(_, milliseconds) {
      datastream.interval(every: milliseconds) |> result.map(fn(_) { Nil })
    }),
    #("timeout", True, fn(source, milliseconds) {
      datastream.timeout(source, within: milliseconds)
      |> result.map(fn(_) { Nil })
    }),
    #("debounce", False, fn(source, milliseconds) {
      datastream.debounce(source, quiet_for: milliseconds)
      |> result.map(fn(_) { Nil })
    }),
    #("throttle", False, fn(source, milliseconds) {
      datastream.throttle(source, every: milliseconds)
      |> result.map(fn(_) { Nil })
    }),
    #("sample", False, fn(source, milliseconds) {
      datastream.sample(source, every: milliseconds)
      |> result.map(fn(_) { Nil })
    }),
    #("window_time", False, fn(source, milliseconds) {
      datastream.window_time(source, span: milliseconds)
      |> result.map(fn(_) { Nil })
    }),
  ]
  use constructor <- list.each(constructors)
  let #(name, allows_zero, construct) = constructor
  use milliseconds <- list.each([-1, 0, 1, 4_294_967_295, 4_294_967_296])
  let log = event_log.new_log()
  let source = event_log.counted_resource([1], named: name, log: log)
  let valid =
    milliseconds <= 4_294_967_295
    && { milliseconds > 0 || { allows_zero && milliseconds == 0 } }
  let expected = case valid {
    True -> Ok(Nil)
    False -> Error(datastream.InvalidDuration(name, milliseconds))
  }
  construct(source, milliseconds) |> should.equal(expected)
  event_log.drain(log, within: 0) |> should.equal([])
}

@target(erlang)
pub fn rate_limit_validates_count_before_duration_without_source_effects_test() {
  use count <- list.each([-1, 0, 1])
  use milliseconds <- list.each([-1, 0, 1, 4_294_967_295, 4_294_967_296])
  let log = event_log.new_log()
  let source = event_log.counted_resource([1], named: "rate_limit", log: log)
  let expected = case
    count > 0,
    milliseconds > 0 && milliseconds <= 4_294_967_295
  {
    False, _ -> Error(datastream.NotPositive("rate_limit", "up_to", count))
    True, False -> Error(datastream.InvalidDuration("rate_limit", milliseconds))
    True, True -> Ok(Nil)
  }
  datastream.rate_limit(source, up_to: count, every: milliseconds)
  |> result.map(fn(_) { Nil })
  |> should.equal(expected)
  event_log.drain(log, within: 0) |> should.equal([])
}

@target(erlang)
pub fn parallel_argument_failures_neither_pull_nor_run_callbacks_test() {
  let constructors = [
    #("map_ordered", fn(source, workers, buffer, effect) {
      datastream.map_ordered(
        source,
        max_workers: workers,
        max_buffer: buffer,
        with: fn(value) {
          effect(value)
          value
        },
      )
      |> result.map(fn(_) { Nil })
    }),
    #("map_unordered", fn(source, workers, buffer, effect) {
      datastream.map_unordered(
        source,
        max_workers: workers,
        max_buffer: buffer,
        with: fn(value) {
          effect(value)
          value
        },
      )
      |> result.map(fn(_) { Nil })
    }),
    #("each_unordered", fn(source, workers, buffer, effect) {
      datastream.each_unordered(
        source,
        max_workers: workers,
        max_buffer: buffer,
        with: effect,
      )
    }),
  ]
  use constructor <- list.each(constructors)
  let #(name, construct) = constructor
  use sizes <- list.each([#(-1, 1), #(0, 0), #(1, -1), #(1, 0), #(2, 1)])
  let #(workers, buffer) = sizes
  let log = event_log.new_log()
  let source = event_log.counted_resource([1], named: name, log: log)
  let expected = case workers <= 0, buffer <= 0 {
    True, _ -> datastream.NotPositive(name, "max_workers", workers)
    False, True -> datastream.NotPositive(name, "max_buffer", buffer)
    False, False -> datastream.BufferLessThanWorkers(workers, buffer)
  }
  construct(source, workers, buffer, fn(_) {
    event_log.record_enter(log, named: "callback")
  })
  |> should.equal(Error(expected))
  event_log.drain(log, within: 0) |> should.equal([])
}

@target(erlang)
pub fn merge_buffer_validation_has_no_resource_effects_test() {
  use buffer <- list.each([-1, 0, 1])
  let log = event_log.new_log()
  let source = event_log.counted_resource([1], named: "merge", log: log)
  let expected = case buffer > 0 {
    True -> Ok(Nil)
    False -> Error(datastream.NotPositive("merge", "max_buffer", buffer))
  }
  datastream.merge([source], max_buffer: buffer)
  |> result.map(fn(_) { Nil })
  |> should.equal(expected)
  event_log.drain(log, within: 0) |> should.equal([])
}

@target(erlang)
pub fn timeout_zero_polls_without_waiting_for_a_gated_pull_test() {
  watchdog.run(within: 4000, running: fn() {
    let started = process.new_subject()
    let closed = process.new_subject()
    let source =
      datastream.resource(
        open: fn() { 0 },
        next: fn(state) {
          let gate = process.new_subject()
          process.send(started, #(gate, process.self()))
          let assert Ok(Nil) = process.receive(gate, within: 1000)
          Next(state, state + 1)
        },
        close: fn(state) { process.send(closed, state) },
      )
    let assert Ok(timed) = datastream.timeout(source, within: 0)
    let values = datastream.to_list(timed)
    let assert Ok(#(gate, worker)) = process.receive(started, within: 1000)
    let monitor = process.monitor(worker)
    process.send(gate, Nil)
    let assert Ok(close_state) = process.receive(closed, within: 1000)
    let assert Ok(Nil) =
      process.new_selector()
      |> process.select_specific_monitor(monitor, fn(_) { Nil })
      |> process.selector_receive(within: 1000)
    #(values, close_state, process.receive(closed, within: 0))
  })
  |> should.equal(Finished(#([Error(Nil)], 1, Error(Nil))))
}

@target(erlang)
fn reverse_completion_gate() -> #(
  Subject(Subject(Nil)),
  Subject(#(Pid, Subject(Nil))),
) {
  let handshake = process.new_subject()
  let _coordinator =
    process.spawn_unlinked(fn() {
      let first_waiting = process.new_subject()
      let second_worker = process.new_subject()
      process.send(handshake, #(first_waiting, second_worker))
      let assert Ok(release_first) =
        process.receive(first_waiting, within: 1000)
      let assert Ok(#(worker, observed)) =
        process.receive(second_worker, within: 1000)
      // This gate observes the current one-task-per-process scheduler. If workers
      // become reusable, replace the synchronization without changing the oracle.
      let monitor = process.monitor(worker)
      process.send(observed, Nil)
      let assert Ok(process.ProcessDown(_, _, process.Normal)) =
        process.new_selector()
        |> process.select_specific_monitor(monitor, fn(down) { down })
        |> process.selector_receive(within: 1000)
      process.send(release_first, Nil)
    })
  let assert Ok(gates) = process.receive(handshake, within: 1000)
  gates
}

@target(erlang)
fn compute_in_reverse_order(
  value: Int,
  first_waiting: Subject(Subject(Nil)),
  second_worker: Subject(#(Pid, Subject(Nil))),
  completed: Subject(Int),
) -> Int {
  case value {
    1 -> {
      let gate = process.new_subject()
      process.send(first_waiting, gate)
      let assert Ok(Nil) = process.receive(gate, within: 1000)
      process.send(completed, 1)
      10
    }
    _ -> {
      process.send(completed, 2)
      let observed = process.new_subject()
      process.send(second_worker, #(process.self(), observed))
      let assert Ok(Nil) = process.receive(observed, within: 1000)
      20
    }
  }
}

@target(erlang)
fn already_received_two(subject: Subject(a)) -> List(a) {
  let assert Ok(first) = process.receive(subject, within: 0)
  let assert Ok(second) = process.receive(subject, within: 0)
  let assert Error(Nil) = process.receive(subject, within: 0)
  [first, second]
}

@target(erlang)
pub fn ordered_computation_then_sequential_each_preserves_effect_order_test() {
  watchdog.run(within: 4000, running: fn() {
    let #(first_waiting, second_worker) = reverse_completion_gate()
    let completed = process.new_subject()
    let effects = process.new_subject()
    let assert Ok(mapped) =
      datastream.from_list([1, 2])
      |> datastream.map_ordered(max_workers: 2, max_buffer: 2, with: fn(value) {
        compute_in_reverse_order(value, first_waiting, second_worker, completed)
      })
    datastream.each(mapped, with: fn(value) { process.send(effects, value) })
    #(already_received_two(completed), already_received_two(effects))
  })
  |> should.equal(Finished(#([2, 1], [10, 20])))
}

@target(erlang)
pub fn each_unordered_waits_for_both_effects_even_when_the_second_finishes_first_test() {
  watchdog.run(within: 4000, running: fn() {
    let #(first_waiting, second_worker) = reverse_completion_gate()
    let completed = process.new_subject()
    let result =
      datastream.from_list([1, 2])
      |> datastream.each_unordered(
        max_workers: 2,
        max_buffer: 2,
        with: fn(value) {
          let _value =
            compute_in_reverse_order(
              value,
              first_waiting,
              second_worker,
              completed,
            )
          Nil
        },
      )
    #(result, already_received_two(completed))
  })
  |> should.equal(Finished(#(Ok(Nil), [2, 1])))
}

@target(erlang)
fn two_messages() -> Stream(Int) {
  let subject = process.new_subject()
  process.send(subject, 1)
  process.send(subject, 2)
  datastream.from_subject(subject, while: fn() { True })
  |> datastream.take(up_to: 2)
}

@target(erlang)
pub fn subject_streams_can_feed_both_parallel_maps_in_the_owner_process_test() {
  let constructors = [datastream.map_ordered, datastream.map_unordered]
  use construct <- list.each(constructors)
  watchdog.run(within: 4000, running: fn() {
    let assert Ok(mapped) =
      construct(two_messages(), 2, 2, fn(value) { value * 10 })
    mapped |> datastream.to_list |> list.sort(int.compare)
  })
  |> should.equal(Finished([10, 20]))
}

@target(erlang)
pub fn subject_streams_can_feed_parallel_each_in_the_owner_process_test() {
  watchdog.run(within: 4000, running: fn() {
    let effects = process.new_subject()
    let result =
      datastream.each_unordered(
        two_messages(),
        max_workers: 2,
        max_buffer: 2,
        with: fn(value) { process.send(effects, value) },
      )
    #(result, already_received_two(effects) |> list.sort(int.compare))
  })
  |> should.equal(Finished(#(Ok(Nil), [1, 2])))
}
