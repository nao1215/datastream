//// Internal helpers for the BEAM extension modules that pull an
//// upstream in another process.
////
//// A pump is a process that owns an upstream `Stream(a)`. It pulls one
//// element each time it receives a request and sends the result to a
//// mailbox of the process that started it, so it never runs ahead of
//// its consumer. A stop request makes it close the upstream and exit;
//// it does the same if the consumer process exits. A pump blocked
//// inside `datastream.pull` only sees a stop after that pull returns.
////
//// The mailbox is a process alias. Closing it deactivates the alias,
//// so messages a worker sends afterwards are dropped by the runtime
//// instead of piling up in the consumer's mailbox, and re-running a
//// stream starts with a fresh mailbox.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only.
pub const beam_only_marker: String =
  "datastream/erlang/internal/pump is BEAM-only"

@target(erlang)
import datastream.{type Step, type Stream, Done, Next}

@target(erlang)
import gleam/dict.{type Dict}

@target(erlang)
import gleam/dynamic.{type Dynamic}

@target(erlang)
import gleam/erlang/atom.{type Atom}

@target(erlang)
import gleam/erlang/process.{type Monitor, type Pid, type Subject}

// --- mailbox ----------------------------------------------------------------

@target(erlang)
/// An alias of the process that created it, receiving `message`s.
pub type Mailbox(message)

@target(erlang)
/// What a wait on a mailbox returned.
pub type Event(message, id) {
  Received(message)
  /// A watched worker exited before its result was received.
  WorkerDown(id: id, reason: Dynamic)
  TimedOut
}

@target(erlang)
@external(erlang, "datastream_mailbox_ffi", "new")
pub fn new_mailbox() -> Mailbox(message)

@target(erlang)
@external(erlang, "datastream_mailbox_ffi", "send")
fn send(to mailbox: Mailbox(message), message message: message) -> Nil

@target(erlang)
/// Wait up to `ms` milliseconds for a message, or for the exit of a
/// worker whose monitor is a key of `watching`.
@external(erlang, "datastream_mailbox_ffi", "receive_event")
fn receive(
  from mailbox: Mailbox(message),
  watching watching: Dict(Monitor, id),
  within ms: Int,
) -> Event(message, id)

@target(erlang)
@external(erlang, "datastream_mailbox_ffi", "receive_event_forever")
pub fn receive_forever(
  from mailbox: Mailbox(message),
  watching watching: Dict(Monitor, id),
) -> Event(message, id)

@target(erlang)
/// Deactivate `mailbox` and drop the messages already sent to it.
@external(erlang, "datastream_mailbox_ffi", "close")
pub fn close_mailbox(mailbox: Mailbox(message)) -> Nil

@target(erlang)
/// Panic in the calling process with a message naming `context` and
/// the crashed worker's exit `reason`.
@external(erlang, "datastream_mailbox_ffi", "raise_worker_crash")
pub fn raise_worker_crash(context: String, reason: Dynamic) -> a

// --- pump workers -----------------------------------------------------------

@target(erlang)
/// What a pump sends for one request.
pub type Reply(a) {
  Element(a)
  Exhausted
}

@target(erlang)
type Control {
  Request
  Stop
}

@target(erlang)
/// The consumer's handle on one pump process.
pub opaque type Worker {
  Worker(monitor: Monitor, control: Subject(Control))
}

@target(erlang)
/// Start a pump for `stream` that answers each request with a
/// `#(id, reply)` message to `mailbox`. The calling process monitors
/// the pump; the pump monitors the calling process.
pub fn spawn_pump(
  over stream: Stream(a),
  into mailbox: Mailbox(#(id, Reply(a))),
  tagged id: id,
) -> Worker {
  let consumer = process.self()
  let handshake = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      let control = process.new_subject()
      process.send(handshake, control)
      let consumer_monitor = process.monitor(consumer)
      let selector =
        process.new_selector()
        |> process.select(control)
        |> process.select_specific_monitor(consumer_monitor, fn(_) { Stop })
      pump_loop(stream, mailbox, id, selector)
    })
  let monitor = process.monitor(pid)
  Worker(monitor:, control: process.receive_forever(from: handshake))
}

@target(erlang)
fn pump_loop(
  stream: Stream(a),
  mailbox: Mailbox(#(id, Reply(a))),
  id: id,
  selector: process.Selector(Control),
) -> Nil {
  case process.selector_receive_forever(from: selector) {
    Stop -> datastream.close(stream)
    Request ->
      case datastream.pull(stream) {
        Next(element, rest) -> {
          send(to: mailbox, message: #(id, Element(element)))
          pump_loop(rest, mailbox, id, selector)
        }
        Done -> send(to: mailbox, message: #(id, Exhausted))
      }
  }
}

@target(erlang)
pub fn monitor_of(worker: Worker) -> Monitor {
  worker.monitor
}

@target(erlang)
/// Ask the pump for one more element.
pub fn request(worker: Worker) -> Nil {
  process.send(worker.control, Request)
}

@target(erlang)
/// Tell the pump to close its upstream and exit, and stop watching it.
pub fn stop(worker: Worker) -> Nil {
  process.send(worker.control, Stop)
  process.demonitor_process(worker.monitor)
}

@target(erlang)
/// Stop watching a pump that has replied `Exhausted` and is exiting.
pub fn forget(worker: Worker) -> Nil {
  process.demonitor_process(worker.monitor)
}

// --- tasks ------------------------------------------------------------------

@target(erlang)
/// Start a process that kills the tasks linked to it when the calling
/// process exits or when `stop_reaper` is called.
@external(erlang, "datastream_mailbox_ffi", "start_reaper")
pub fn start_reaper() -> Pid

@target(erlang)
@external(erlang, "datastream_mailbox_ffi", "stop_reaper")
pub fn stop_reaper(reaper: Pid) -> Nil

@target(erlang)
/// Run `body`; if it panics or throws, run `on_crash` first and then let
/// the same error continue.
@external(erlang, "datastream_mailbox_ffi", "guard")
pub fn guard(body: fn() -> a, on_crash on_crash: fn() -> Nil) -> a

@target(erlang)
/// Run `work` in a new monitored process that sends `#(id, result)` to
/// `mailbox`. The process links to `reaper`, so it is killed when the
/// consumer exits even while `work` is still running.
pub fn spawn_task(
  into mailbox: Mailbox(#(id, b)),
  tagged id: id,
  linked_to reaper: Pid,
  running work: fn() -> b,
) -> #(Pid, Monitor) {
  let pid =
    process.spawn_unlinked(fn() {
      process.link(reaper)
      send(to: mailbox, message: #(id, work()))
    })
  #(pid, process.monitor(pid))
}

// --- a single pump ------------------------------------------------------------

@target(erlang)
/// One pump together with its mailbox, for combinators that consume a
/// single upstream. `requested` is True while a request is unanswered.
pub opaque type Pump(a) {
  Pump(
    worker: Worker,
    mailbox: Mailbox(#(Nil, Reply(a))),
    watching: Dict(Monitor, Nil),
    requested: Bool,
    context: String,
  )
}

@target(erlang)
/// Build a stream that starts a pump for `stream` on its first pull and
/// hands it to `begin` together with the current monotonic time.
/// Nothing is spawned or pulled before that first pull, and every run
/// gets its own pump.
pub fn on_first_pull(
  over stream: Stream(a),
  context context: String,
  begin begin: fn(Pump(a), Int) -> Step(b, Stream(b)),
) -> Stream(b) {
  datastream.make(
    pull: fn() {
      let mailbox = new_mailbox()
      let worker = spawn_pump(over: stream, into: mailbox, tagged: Nil)
      let watching = dict.from_list([#(worker.monitor, Nil)])
      begin(
        Pump(worker:, mailbox:, watching:, requested: False, context:),
        now_ms(),
      )
    },
    close: fn() { datastream.close(stream) },
  )
}

@target(erlang)
/// Wait up to `ms` milliseconds for the next reply, requesting one
/// first unless a request is still unanswered. After `Exhausted` the
/// pump is finished and must not be used again. A crashed upstream is
/// re-raised as a panic here.
pub fn next(
  pump: Pump(a),
  within ms: Int,
) -> #(Pump(a), Result(Reply(a), Nil)) {
  let pump = ensure_requested(pump)
  handle_event(
    pump,
    receive(from: pump.mailbox, watching: pump.watching, within: ms),
  )
}

@target(erlang)
/// Like `next`, without a deadline.
pub fn next_forever(pump: Pump(a)) -> #(Pump(a), Reply(a)) {
  let pump = ensure_requested(pump)
  case
    handle_event(
      pump,
      receive_forever(from: pump.mailbox, watching: pump.watching),
    )
  {
    #(pump, Ok(reply)) -> #(pump, reply)
    #(pump, Error(_)) -> next_forever(pump)
  }
}

@target(erlang)
fn ensure_requested(pump: Pump(a)) -> Pump(a) {
  case pump.requested {
    True -> pump
    False -> {
      request(pump.worker)
      Pump(..pump, requested: True)
    }
  }
}

@target(erlang)
fn handle_event(
  pump: Pump(a),
  event: Event(#(Nil, Reply(a)), Nil),
) -> #(Pump(a), Result(Reply(a), Nil)) {
  case event {
    Received(#(_, Exhausted)) -> {
      forget(pump.worker)
      close_mailbox(pump.mailbox)
      #(pump, Ok(Exhausted))
    }
    Received(#(_, reply)) -> #(Pump(..pump, requested: False), Ok(reply))
    TimedOut -> #(pump, Error(Nil))
    WorkerDown(reason:, ..) -> {
      close_mailbox(pump.mailbox)
      raise_worker_crash(pump.context, reason)
    }
  }
}

@target(erlang)
/// Stop a pump that has not replied `Exhausted`: it closes its upstream
/// once any pull in progress returns.
pub fn stop_pump(pump: Pump(a)) -> Nil {
  stop(pump.worker)
  close_mailbox(pump.mailbox)
}

// --- clock --------------------------------------------------------------------

@target(erlang)
@external(erlang, "erlang", "monotonic_time")
fn erlang_monotonic_time(unit: Atom) -> Int

@target(erlang)
/// Monotonic time in milliseconds.
pub fn now_ms() -> Int {
  erlang_monotonic_time(atom.create("millisecond"))
}
