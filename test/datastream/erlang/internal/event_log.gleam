//// Test helper for tracking resource open/close events across BEAM
//// worker processes.
////
//// The cross-target `process dictionary` counter pattern used in
//// `test/datastream/resource_test.gleam` does not work here because
//// the resource's `open` and `close` callbacks fire inside the worker
//// processes spawned by `datastream/erlang/par` and
//// `datastream/erlang/time`, and each process has its own dictionary.
//// Instead, every event is sent to a shared `Subject` owned by the
//// test process; the test drains it after the pipeline is done.

@target(javascript)
pub const beam_only_marker: String = "test/datastream/erlang/internal/event_log is BEAM-only"

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import datastream/source

@target(erlang)
import gleam/erlang/process.{type Subject}

@target(erlang)
pub type Event {
  ResourceOpen(name: String)
  ResourceClose(name: String)
  EnterFn(name: String)
  LeaveFn(name: String)
}

@target(erlang)
pub fn new_log() -> Subject(Event) {
  process.new_subject()
}

@target(erlang)
pub fn counted_resource(
  elements: List(a),
  named name: String,
  log log: Subject(Event),
) -> Stream(a) {
  source.resource(
    open: fn() {
      process.send(log, ResourceOpen(name))
      elements
    },
    next: fn(state) {
      case state {
        [] -> Done
        [head, ..tail] -> Next(element: head, state: tail)
      }
    },
    close: fn(_) {
      process.send(log, ResourceClose(name))
      Nil
    },
  )
}

@target(erlang)
pub fn drain(log: Subject(Event), within ms: Int) -> List(Event) {
  drain_loop(log, ms, [])
  |> reverse
}

@target(erlang)
fn drain_loop(log: Subject(Event), ms: Int, acc: List(Event)) -> List(Event) {
  case process.receive(from: log, within: ms) {
    Ok(event) -> drain_loop(log, ms, [event, ..acc])
    Error(_) -> acc
  }
}

@target(erlang)
fn reverse(list: List(a)) -> List(a) {
  reverse_loop(list, [])
}

@target(erlang)
fn reverse_loop(list: List(a), acc: List(a)) -> List(a) {
  case list {
    [] -> acc
    [head, ..tail] -> reverse_loop(tail, [head, ..acc])
  }
}

@target(erlang)
pub fn count_opens(events: List(Event)) -> Int {
  count_matching(events, is_open, 0)
}

@target(erlang)
pub fn count_closes(events: List(Event)) -> Int {
  count_matching(events, is_close, 0)
}

@target(erlang)
fn is_open(event: Event) -> Bool {
  case event {
    ResourceOpen(_) -> True
    _ -> False
  }
}

@target(erlang)
fn is_close(event: Event) -> Bool {
  case event {
    ResourceClose(_) -> True
    _ -> False
  }
}

@target(erlang)
pub fn record_enter(log: Subject(Event), named name: String) -> Nil {
  process.send(log, EnterFn(name))
}

@target(erlang)
pub fn record_leave(log: Subject(Event), named name: String) -> Nil {
  process.send(log, LeaveFn(name))
}

@target(erlang)
/// Walk the events in arrival order, tracking running concurrent
/// `EnterFn`/`LeaveFn` count for `name`, and return the peak.
pub fn peak_concurrent(events: List(Event), named name: String) -> Int {
  peak_loop(events, name, 0, 0)
}

@target(erlang)
fn peak_loop(events: List(Event), name: String, running: Int, peak: Int) -> Int {
  case events {
    [] -> peak
    [EnterFn(n), ..rest] if n == name -> {
      let new_running = running + 1
      let new_peak = case new_running > peak {
        True -> new_running
        False -> peak
      }
      peak_loop(rest, name, new_running, new_peak)
    }
    [LeaveFn(n), ..rest] if n == name ->
      peak_loop(rest, name, running - 1, peak)
    [_, ..rest] -> peak_loop(rest, name, running, peak)
  }
}

@target(erlang)
fn count_matching(list: List(Event), pred: fn(Event) -> Bool, acc: Int) -> Int {
  case list {
    [] -> acc
    [head, ..tail] ->
      case pred(head) {
        True -> count_matching(tail, pred, acc + 1)
        False -> count_matching(tail, pred, acc)
      }
  }
}
