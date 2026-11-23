//// Worker cleanup when a callback raises, on Erlang. Each test runs in
//// its own process, which survives the raise, so a worker or source
//// left running would stay alive and keep its resource open.

@target(erlang)
import datastream.{type Stream, Next}

@target(erlang)
import datastream/erlang/internal/event_log.{type Event, EnterFn}

@target(erlang)
import datastream/erlang/internal/watchdog.{Finished}

@target(erlang)
import gleam/erlang/process.{type Subject}

@target(erlang)
import gleam/list

@target(javascript)
pub const beam_only_marker: String = "exception_cleanup_erlang_test is BEAM-only"

@target(erlang)
@external(erlang, "exception_cleanup_ffi", "rescue")
fn rescue(thunk: fn() -> a) -> Result(a, Nil)

@target(erlang)
fn numbers() -> List(Int) {
  datastream.range(from: 0, to: 100) |> datastream.to_list
}

@target(erlang)
fn explode_at(stream: Stream(a), trigger: a) -> Stream(a) {
  datastream.map(stream, fn(value) {
    case value == trigger {
      True -> panic as "explode"
      False -> value
    }
  })
}

@target(erlang)
fn opens_and_closes(log: Subject(Event)) -> #(Int, Int) {
  let events = event_log.drain(log, within: 300)
  #(event_log.count_opens(events), event_log.count_closes(events))
}

@target(erlang)
pub fn merge_raise_downstream_stops_workers_and_closes_sources_test() {
  let outcome = {
    use <- watchdog.run(within: 3000)
    let log = event_log.new_log()
    let assert Ok(merged) =
      datastream.merge(
        [
          event_log.counted_resource(numbers(), named: "a", log:),
          event_log.counted_resource(numbers(), named: "b", log:),
        ],
        max_buffer: 2,
      )
    let result = rescue(fn() { merged |> explode_at(3) |> datastream.to_list })
    #(result, opens_and_closes(log))
  }
  assert outcome == Finished(#(Error(Nil), #(2, 2)))
}

@target(erlang)
pub fn timeout_raise_downstream_closes_source_test() {
  let outcome = {
    use <- watchdog.run(within: 3000)
    let log = event_log.new_log()
    let assert Ok(timed) =
      datastream.timeout(
        event_log.counted_resource(numbers(), named: "a", log:),
        within: 1000,
      )
    let result = rescue(fn() { timed |> explode_at(Ok(3)) |> datastream.drain })
    #(result, opens_and_closes(log))
  }
  assert outcome == Finished(#(Error(Nil), #(1, 1)))
}

@target(erlang)
pub fn race_raise_downstream_closes_winner_test() {
  let outcome = {
    use <- watchdog.run(within: 3000)
    let log = event_log.new_log()
    let raced =
      datastream.race([event_log.counted_resource(numbers(), named: "a", log:)])
    let result = rescue(fn() { raced |> explode_at(3) |> datastream.drain })
    #(result, opens_and_closes(log))
  }
  assert outcome == Finished(#(Error(Nil), #(1, 1)))
}

@target(erlang)
pub fn source_raise_in_worker_closes_source_test() {
  let outcome = {
    use <- watchdog.run(within: 3000)
    let log = event_log.new_log()
    let failing =
      datastream.resource(
        open: fn() {
          process.send(log, event_log.ResourceOpen("a"))
          0
        },
        next: fn(index) {
          case index {
            2 -> panic as "read failed"
            _ -> Next(index, index + 1)
          }
        },
        close: fn(_) { process.send(log, event_log.ResourceClose("a")) },
      )
    let assert Ok(merged) = datastream.merge([failing], max_buffer: 1)
    let result = rescue(fn() { datastream.to_list(merged) })
    #(result, opens_and_closes(log))
  }
  assert outcome == Finished(#(Error(Nil), #(1, 1)))
}

@target(erlang)
pub fn map_ordered_raise_downstream_stops_running_tasks_test() {
  let outcome = {
    use <- watchdog.run(within: 3000)
    let log = event_log.new_log()
    let assert Ok(mapped) =
      datastream.range(from: 0, to: 8)
      |> datastream.map_ordered(max_workers: 4, max_buffer: 4, with: fn(x) {
        case x {
          0 -> process.sleep(10)
          _ -> process.sleep(300)
        }
        process.send(log, EnterFn("done"))
        x
      })
    let result = rescue(fn() { mapped |> explode_at(0) |> datastream.drain })
    let finished =
      event_log.drain(log, within: 600)
      |> list.count(fn(event) { event == EnterFn("done") })
    #(result, finished)
  }
  assert outcome == Finished(#(Error(Nil), 1))
}
