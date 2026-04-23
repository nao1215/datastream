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
    ResourceClose(_) -> False
  }
}

@target(erlang)
fn is_close(event: Event) -> Bool {
  case event {
    ResourceClose(_) -> True
    ResourceOpen(_) -> False
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
