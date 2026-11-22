//// Test helper that runs a thunk in its own process with a deadline,
//// so a test that would hang reports `Hung` instead of blocking the
//// suite, and a panic reports `Crashed` instead of failing the test
//// process.

@target(javascript)
pub const beam_only_marker: String =
  "test/datastream/erlang/internal/watchdog is BEAM-only"

@target(erlang)
import gleam/erlang/process

@target(erlang)
pub type Outcome(a) {
  Finished(a)
  Crashed
  Hung
}

@target(erlang)
/// Run `thunk` in a fresh unlinked process and wait at most `ms`
/// milliseconds for it. A process that is still running at the deadline
/// is killed.
pub fn run(within ms: Int, running thunk: fn() -> a) -> Outcome(a) {
  let reply = process.new_subject()
  let pid = process.spawn_unlinked(fn() { process.send(reply, thunk()) })
  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_map(reply, Finished)
    |> process.select_specific_monitor(monitor, fn(_) { Crashed })
  case process.selector_receive(from: selector, within: ms) {
    Ok(Finished(value)) -> {
      process.demonitor_process(monitor)
      Finished(value)
    }
    Ok(_) -> Crashed
    Error(_) -> {
      process.demonitor_process(monitor)
      process.kill(pid)
      Hung
    }
  }
}

@target(erlang)
/// Whether the calling process has any message left in its mailbox.
pub fn mailbox_is_empty() -> Bool {
  let selector = process.new_selector() |> process.select_other(fn(_) { Nil })
  case process.selector_receive(from: selector, within: 0) {
    Ok(_) -> False
    Error(_) -> True
  }
}
