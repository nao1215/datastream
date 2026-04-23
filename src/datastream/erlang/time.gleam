//// BEAM-only time-based combinators.
////
//// Every combinator here observes wall-clock behaviour: bursts get
//// debounced / throttled / sampled / windowed against BEAM monotonic
//// time, never against the wall clock that can jump backwards.
////
//// Implementation strategy: each combinator spawns one worker
//// process that pumps the upstream into a shared subject. The main
//// process drives the consuming `Stream` and uses
//// `process.receive(within: ms)` to wait simultaneously for the next
//// element and for a timer deadline.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only.
pub const beam_only_marker: String = "datastream/erlang/time is BEAM-only"

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import datastream/chunk.{type Chunk}

@target(erlang)
import datastream/source

@target(erlang)
import gleam/erlang/atom.{type Atom}

@target(erlang)
import gleam/erlang/process.{type Subject}

@target(erlang)
import gleam/option.{type Option, None, Some}

// --- worker pump (shared) ------------------------------------------------

@target(erlang)
type Pump(a) {
  PumpElement(a)
  PumpDone
}

@target(erlang)
fn spawn_pump(stream: Stream(a)) -> Subject(Pump(a)) {
  let subj = process.new_subject()
  let _pid = process.spawn_unlinked(fn() { pump_loop(stream, subj) })
  subj
}

@target(erlang)
fn pump_loop(stream: Stream(a), subj: Subject(Pump(a))) -> Nil {
  case datastream.pull(stream) {
    Next(element, rest) -> {
      process.send(subj, PumpElement(element))
      pump_loop(rest, subj)
    }
    Done -> process.send(subj, PumpDone)
  }
}

@target(erlang)
@external(erlang, "erlang", "monotonic_time")
fn erlang_monotonic_time(unit: Atom) -> Int

@target(erlang)
fn now_ms() -> Int {
  erlang_monotonic_time(atom.create("millisecond"))
}

// --- debounce ------------------------------------------------------------

@target(erlang)
type DebounceState(a) {
  DebounceState(
    subj: Subject(Pump(a)),
    latest: Option(a),
    deadline: Int,
    upstream_done: Bool,
  )
}

@target(erlang)
/// Emit only after the upstream has been silent for at least
/// `quiet_for` ms following the last seen element. The element
/// emitted is the LAST element seen before the silence.
///
/// Useful for collapsing UI events (typing, resize) into a single
/// "settled" notification.
pub fn debounce(over stream: Stream(a), quiet_for ms: Int) -> Stream(a) {
  let subj = spawn_pump(stream)
  source.unfold(
    from: DebounceState(
      subj: subj,
      latest: None,
      deadline: 0,
      upstream_done: False,
    ),
    with: fn(state) { debounce_step(state, ms) },
  )
}

@target(erlang)
fn debounce_step(
  state: DebounceState(a),
  ms: Int,
) -> datastream.Step(a, DebounceState(a)) {
  case state.upstream_done {
    True -> Done
    False -> debounce_wait(state, ms)
  }
}

@target(erlang)
fn debounce_wait(
  state: DebounceState(a),
  ms: Int,
) -> datastream.Step(a, DebounceState(a)) {
  case state.latest {
    None ->
      case process.receive_forever(from: state.subj) {
        PumpElement(element) ->
          debounce_wait(
            DebounceState(
              ..state,
              latest: Some(element),
              deadline: now_ms() + ms,
            ),
            ms,
          )
        PumpDone -> Done
      }
    Some(value) -> {
      let now = now_ms()
      let wait = case state.deadline - now {
        delta if delta > 0 -> delta
        _ -> 0
      }
      case process.receive(from: state.subj, within: wait) {
        Ok(PumpElement(element)) ->
          debounce_wait(
            DebounceState(
              ..state,
              latest: Some(element),
              deadline: now_ms() + ms,
            ),
            ms,
          )
        Ok(PumpDone) ->
          Next(value, DebounceState(..state, latest: None, upstream_done: True))
        Error(_) -> Next(value, DebounceState(..state, latest: None))
      }
    }
  }
}

// --- throttle ------------------------------------------------------------

@target(erlang)
type ThrottleState {
  ThrottleState(next_window_start: Option(Int))
}

@target(erlang)
/// Emit the FIRST element of each `ms`-wide window; drop the rest.
///
/// Windows start at the moment the first element is emitted.
pub fn throttle(over stream: Stream(a), every ms: Int) -> Stream(a) {
  let subj = spawn_pump(stream)
  source.unfold(
    from: #(subj, ThrottleState(next_window_start: None)),
    with: fn(state) {
      let #(subj, throttle_state) = state
      throttle_step(subj, throttle_state, ms)
    },
  )
}

@target(erlang)
fn throttle_step(
  subj: Subject(Pump(a)),
  state: ThrottleState,
  ms: Int,
) -> datastream.Step(a, #(Subject(Pump(a)), ThrottleState)) {
  case process.receive_forever(from: subj) {
    PumpDone -> Done
    PumpElement(element) -> {
      let now = now_ms()
      case state.next_window_start {
        None ->
          Next(element, #(
            subj,
            ThrottleState(next_window_start: Some(now + ms)),
          ))
        Some(start) ->
          case now >= start {
            True ->
              Next(element, #(
                subj,
                ThrottleState(next_window_start: Some(now + ms)),
              ))
            False -> throttle_step(subj, state, ms)
          }
      }
    }
  }
}

// --- sample --------------------------------------------------------------

@target(erlang)
type SampleState(a) {
  SampleState(
    subj: Subject(Pump(a)),
    window_start: Int,
    latest: Option(a),
    upstream_done: Bool,
  )
}

@target(erlang)
/// At each `ms`-wide window boundary, emit the most recent element
/// seen during the window. If no element arrived during the window,
/// the boundary emits nothing and the next window begins.
pub fn sample(over stream: Stream(a), every ms: Int) -> Stream(a) {
  let subj = spawn_pump(stream)
  source.unfold(
    from: SampleState(
      subj: subj,
      window_start: now_ms(),
      latest: None,
      upstream_done: False,
    ),
    with: fn(state) { sample_step(state, ms) },
  )
}

@target(erlang)
fn sample_step(
  state: SampleState(a),
  ms: Int,
) -> datastream.Step(a, SampleState(a)) {
  case state.upstream_done {
    True -> Done
    False -> sample_wait(state, ms)
  }
}

@target(erlang)
fn sample_wait(
  state: SampleState(a),
  ms: Int,
) -> datastream.Step(a, SampleState(a)) {
  let now = now_ms()
  let deadline = state.window_start + ms
  let wait = case deadline - now {
    delta if delta > 0 -> delta
    _ -> 0
  }
  case process.receive(from: state.subj, within: wait) {
    Ok(PumpElement(element)) ->
      sample_wait(SampleState(..state, latest: Some(element)), ms)
    Ok(PumpDone) ->
      case state.latest {
        Some(value) ->
          Next(value, SampleState(..state, latest: None, upstream_done: True))
        None -> Done
      }
    Error(_) ->
      case state.latest {
        Some(value) ->
          Next(
            value,
            SampleState(..state, latest: None, window_start: deadline),
          )
        None -> sample_wait(SampleState(..state, window_start: deadline), ms)
      }
  }
}

// --- rate_limit ----------------------------------------------------------

@target(erlang)
type RateState {
  RateState(window_start: Int, count_in_window: Int)
}

@target(erlang)
/// Emit elements no faster than `count` per `ms`-wide window.
/// Excess elements are delayed (sleeping the calling pull), NOT
/// dropped. Loss-free.
pub fn rate_limit(
  over stream: Stream(a),
  max_per_window count: Int,
  window_ms ms: Int,
) -> Stream(a) {
  let subj = spawn_pump(stream)
  source.unfold(
    from: #(subj, RateState(window_start: now_ms(), count_in_window: 0)),
    with: fn(state) {
      let #(subj, rate_state) = state
      rate_limit_step(subj, rate_state, count, ms)
    },
  )
}

@target(erlang)
fn rate_limit_step(
  subj: Subject(Pump(a)),
  state: RateState,
  count: Int,
  ms: Int,
) -> datastream.Step(a, #(Subject(Pump(a)), RateState)) {
  case process.receive_forever(from: subj) {
    PumpDone -> Done
    PumpElement(element) -> rate_limit_handle(element, subj, state, count, ms)
  }
}

@target(erlang)
fn rate_limit_handle(
  element: a,
  subj: Subject(Pump(a)),
  state: RateState,
  count: Int,
  ms: Int,
) -> datastream.Step(a, #(Subject(Pump(a)), RateState)) {
  let now = now_ms()
  let state = case now >= state.window_start + ms {
    True -> RateState(window_start: now, count_in_window: 0)
    False -> state
  }
  case state.count_in_window < count {
    True ->
      Next(element, #(
        subj,
        RateState(..state, count_in_window: state.count_in_window + 1),
      ))
    False -> rate_limit_delay(element, subj, state, ms, now)
  }
}

@target(erlang)
fn rate_limit_delay(
  element: a,
  subj: Subject(Pump(a)),
  state: RateState,
  ms: Int,
  now: Int,
) -> datastream.Step(a, #(Subject(Pump(a)), RateState)) {
  let sleep_ms = case state.window_start + ms - now {
    delta if delta > 0 -> delta
    _ -> 0
  }
  process.sleep(sleep_ms)
  let new_now = now_ms()
  Next(element, #(subj, RateState(window_start: new_now, count_in_window: 1)))
}

// --- window_time ---------------------------------------------------------

@target(erlang)
type WindowState(a) {
  WindowState(
    subj: Subject(Pump(a)),
    buffer: List(a),
    window_start: Int,
    upstream_done: Bool,
  )
}

@target(erlang)
/// Divide time into `ms`-wide windows. At each boundary, emit a
/// `Chunk(a)` containing every element that arrived during that
/// window. Empty windows emit empty chunks.
pub fn window_time(over stream: Stream(a), span ms: Int) -> Stream(Chunk(a)) {
  let subj = spawn_pump(stream)
  source.unfold(
    from: WindowState(
      subj: subj,
      buffer: [],
      window_start: now_ms(),
      upstream_done: False,
    ),
    with: fn(state) { window_step(state, ms) },
  )
}

@target(erlang)
fn window_step(
  state: WindowState(a),
  ms: Int,
) -> datastream.Step(Chunk(a), WindowState(a)) {
  case state.upstream_done {
    True ->
      case state.buffer {
        [] -> Done
        _ ->
          Next(
            chunk_from_reverse(state.buffer),
            WindowState(..state, buffer: []),
          )
      }
    False -> window_wait(state, ms)
  }
}

@target(erlang)
fn window_wait(
  state: WindowState(a),
  ms: Int,
) -> datastream.Step(Chunk(a), WindowState(a)) {
  let now = now_ms()
  let deadline = state.window_start + ms
  let wait = case deadline - now {
    delta if delta > 0 -> delta
    _ -> 0
  }
  case process.receive(from: state.subj, within: wait) {
    Ok(PumpElement(element)) ->
      window_wait(WindowState(..state, buffer: [element, ..state.buffer]), ms)
    Ok(PumpDone) -> window_step(WindowState(..state, upstream_done: True), ms)
    Error(_) ->
      Next(
        chunk_from_reverse(state.buffer),
        WindowState(..state, buffer: [], window_start: deadline),
      )
  }
}

@target(erlang)
fn chunk_from_reverse(buffer: List(a)) -> Chunk(a) {
  chunk.from_list(reverse_list(buffer))
}

@target(erlang)
fn reverse_list(list: List(a)) -> List(a) {
  reverse_loop(list, [])
}

@target(erlang)
fn reverse_loop(list: List(a), acc: List(a)) -> List(a) {
  case list {
    [] -> acc
    [head, ..tail] -> reverse_loop(tail, [head, ..acc])
  }
}
