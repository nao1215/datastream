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
import datastream/erlang/internal/pump.{
  type Pump, type Stop, PumpDone, PumpElement,
}

@target(erlang)
import gleam/erlang/atom.{type Atom}

@target(erlang)
import gleam/erlang/process.{type Subject}

@target(erlang)
import gleam/list

@target(erlang)
import gleam/option.{type Option, None, Some}

@target(erlang)
@external(erlang, "erlang", "monotonic_time")
fn erlang_monotonic_time(unit: Atom) -> Int

@target(erlang)
fn now_ms() -> Int {
  erlang_monotonic_time(atom.create("millisecond"))
}

// --- shared close handler ------------------------------------------------

@target(erlang)
fn maybe_stop(stop_subj: Subject(Stop), upstream_done: Bool) -> Nil {
  case upstream_done {
    True -> Nil
    False -> pump.stop(stop_subj)
  }
}

// --- debounce ------------------------------------------------------------

@target(erlang)
type DebounceState(a) {
  DebounceState(
    result_subj: Subject(Pump(a)),
    stop_subj: Subject(Stop),
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
  let result_subj = process.new_subject()
  let stop_subj = pump.spawn_pump(stream, into: result_subj)
  build_debounce_stream(
    DebounceState(
      result_subj: result_subj,
      stop_subj: stop_subj,
      latest: None,
      deadline: 0,
      upstream_done: False,
    ),
    ms,
  )
}

@target(erlang)
fn build_debounce_stream(state: DebounceState(a), ms: Int) -> Stream(a) {
  datastream.make(pull: fn() { debounce_step(state, ms) }, close: fn() {
    maybe_stop(state.stop_subj, state.upstream_done)
  })
}

@target(erlang)
fn debounce_step(
  state: DebounceState(a),
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  case state.upstream_done {
    True -> Done
    False -> debounce_wait(state, ms)
  }
}

@target(erlang)
fn debounce_wait(
  state: DebounceState(a),
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  case state.latest {
    None ->
      case process.receive_forever(from: state.result_subj) {
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
      case process.receive(from: state.result_subj, within: wait) {
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
          Next(
            value,
            build_debounce_stream(
              DebounceState(..state, latest: None, upstream_done: True),
              ms,
            ),
          )
        Error(_) ->
          Next(
            value,
            build_debounce_stream(DebounceState(..state, latest: None), ms),
          )
      }
    }
  }
}

// --- throttle ------------------------------------------------------------

@target(erlang)
type ThrottleState(a) {
  ThrottleState(
    result_subj: Subject(Pump(a)),
    stop_subj: Subject(Stop),
    next_window_start: Option(Int),
    upstream_done: Bool,
  )
}

@target(erlang)
/// Emit the FIRST element of each `ms`-wide window; drop the rest.
///
/// Windows start at the moment the first element is emitted.
pub fn throttle(over stream: Stream(a), every ms: Int) -> Stream(a) {
  let result_subj = process.new_subject()
  let stop_subj = pump.spawn_pump(stream, into: result_subj)
  build_throttle_stream(
    ThrottleState(
      result_subj: result_subj,
      stop_subj: stop_subj,
      next_window_start: None,
      upstream_done: False,
    ),
    ms,
  )
}

@target(erlang)
fn build_throttle_stream(state: ThrottleState(a), ms: Int) -> Stream(a) {
  datastream.make(pull: fn() { throttle_step(state, ms) }, close: fn() {
    maybe_stop(state.stop_subj, state.upstream_done)
  })
}

@target(erlang)
fn throttle_step(
  state: ThrottleState(a),
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  case state.upstream_done {
    True -> Done
    False ->
      case process.receive_forever(from: state.result_subj) {
        PumpDone -> Done
        PumpElement(element) -> throttle_handle(element, state, ms)
      }
  }
}

@target(erlang)
fn throttle_handle(
  element: a,
  state: ThrottleState(a),
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  let now = now_ms()
  case state.next_window_start {
    None -> throttle_emit(element, state, now, ms)
    Some(start) ->
      case now >= start {
        True -> throttle_emit(element, state, now, ms)
        False -> throttle_step(state, ms)
      }
  }
}

@target(erlang)
fn throttle_emit(
  element: a,
  state: ThrottleState(a),
  now: Int,
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  Next(
    element,
    build_throttle_stream(
      ThrottleState(..state, next_window_start: Some(now + ms)),
      ms,
    ),
  )
}

// --- sample --------------------------------------------------------------

@target(erlang)
type SampleState(a) {
  SampleState(
    result_subj: Subject(Pump(a)),
    stop_subj: Subject(Stop),
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
  let result_subj = process.new_subject()
  let stop_subj = pump.spawn_pump(stream, into: result_subj)
  build_sample_stream(
    SampleState(
      result_subj: result_subj,
      stop_subj: stop_subj,
      window_start: now_ms(),
      latest: None,
      upstream_done: False,
    ),
    ms,
  )
}

@target(erlang)
fn build_sample_stream(state: SampleState(a), ms: Int) -> Stream(a) {
  datastream.make(pull: fn() { sample_step(state, ms) }, close: fn() {
    maybe_stop(state.stop_subj, state.upstream_done)
  })
}

@target(erlang)
fn sample_step(state: SampleState(a), ms: Int) -> datastream.Step(a, Stream(a)) {
  case state.upstream_done {
    True -> Done
    False -> sample_wait(state, ms)
  }
}

@target(erlang)
fn sample_wait(state: SampleState(a), ms: Int) -> datastream.Step(a, Stream(a)) {
  let now = now_ms()
  let deadline = state.window_start + ms
  let wait = case deadline - now {
    delta if delta > 0 -> delta
    _ -> 0
  }
  case process.receive(from: state.result_subj, within: wait) {
    Ok(PumpElement(element)) ->
      sample_wait(SampleState(..state, latest: Some(element)), ms)
    Ok(PumpDone) ->
      case state.latest {
        Some(value) ->
          Next(
            value,
            build_sample_stream(
              SampleState(..state, latest: None, upstream_done: True),
              ms,
            ),
          )
        None -> Done
      }
    Error(_) ->
      case state.latest {
        Some(value) ->
          Next(
            value,
            build_sample_stream(
              SampleState(..state, latest: None, window_start: deadline),
              ms,
            ),
          )
        None -> sample_wait(SampleState(..state, window_start: deadline), ms)
      }
  }
}

// --- rate_limit ----------------------------------------------------------

@target(erlang)
type RateState(a) {
  RateState(
    result_subj: Subject(Pump(a)),
    stop_subj: Subject(Stop),
    window_start: Int,
    count_in_window: Int,
    upstream_done: Bool,
  )
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
  let result_subj = process.new_subject()
  let stop_subj = pump.spawn_pump(stream, into: result_subj)
  build_rate_limit_stream(
    RateState(
      result_subj: result_subj,
      stop_subj: stop_subj,
      window_start: now_ms(),
      count_in_window: 0,
      upstream_done: False,
    ),
    count,
    ms,
  )
}

@target(erlang)
fn build_rate_limit_stream(
  state: RateState(a),
  count: Int,
  ms: Int,
) -> Stream(a) {
  datastream.make(pull: fn() { rate_limit_step(state, count, ms) }, close: fn() {
    maybe_stop(state.stop_subj, state.upstream_done)
  })
}

@target(erlang)
fn rate_limit_step(
  state: RateState(a),
  count: Int,
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  case state.upstream_done {
    True -> Done
    False ->
      case process.receive_forever(from: state.result_subj) {
        PumpDone -> Done
        PumpElement(element) -> rate_limit_handle(element, state, count, ms)
      }
  }
}

@target(erlang)
fn rate_limit_handle(
  element: a,
  state: RateState(a),
  count: Int,
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  let now = now_ms()
  let state = case now >= state.window_start + ms {
    True -> RateState(..state, window_start: now, count_in_window: 0)
    False -> state
  }
  case state.count_in_window < count {
    True ->
      Next(
        element,
        build_rate_limit_stream(
          RateState(..state, count_in_window: state.count_in_window + 1),
          count,
          ms,
        ),
      )
    False -> rate_limit_delay(element, state, count, ms, now)
  }
}

@target(erlang)
fn rate_limit_delay(
  element: a,
  state: RateState(a),
  count: Int,
  ms: Int,
  now: Int,
) -> datastream.Step(a, Stream(a)) {
  let sleep_ms = case state.window_start + ms - now {
    delta if delta > 0 -> delta
    _ -> 0
  }
  process.sleep(sleep_ms)
  let new_now = now_ms()
  Next(
    element,
    build_rate_limit_stream(
      RateState(..state, window_start: new_now, count_in_window: 1),
      count,
      ms,
    ),
  )
}

// --- window_time ---------------------------------------------------------

@target(erlang)
type WindowState(a) {
  WindowState(
    result_subj: Subject(Pump(a)),
    stop_subj: Subject(Stop),
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
  let result_subj = process.new_subject()
  let stop_subj = pump.spawn_pump(stream, into: result_subj)
  build_window_stream(
    WindowState(
      result_subj: result_subj,
      stop_subj: stop_subj,
      buffer: [],
      window_start: now_ms(),
      upstream_done: False,
    ),
    ms,
  )
}

@target(erlang)
fn build_window_stream(state: WindowState(a), ms: Int) -> Stream(Chunk(a)) {
  datastream.make(pull: fn() { window_step(state, ms) }, close: fn() {
    maybe_stop(state.stop_subj, state.upstream_done)
  })
}

@target(erlang)
fn window_step(
  state: WindowState(a),
  ms: Int,
) -> datastream.Step(Chunk(a), Stream(Chunk(a))) {
  case state.upstream_done {
    True ->
      case state.buffer {
        [] -> Done
        _ ->
          Next(
            chunk.from_list(list.reverse(state.buffer)),
            build_window_stream(WindowState(..state, buffer: []), ms),
          )
      }
    False -> window_wait(state, ms)
  }
}

@target(erlang)
fn window_wait(
  state: WindowState(a),
  ms: Int,
) -> datastream.Step(Chunk(a), Stream(Chunk(a))) {
  let now = now_ms()
  let deadline = state.window_start + ms
  let wait = case deadline - now {
    delta if delta > 0 -> delta
    _ -> 0
  }
  case process.receive(from: state.result_subj, within: wait) {
    Ok(PumpElement(element)) ->
      window_wait(WindowState(..state, buffer: [element, ..state.buffer]), ms)
    Ok(PumpDone) -> window_step(WindowState(..state, upstream_done: True), ms)
    Error(_) ->
      Next(
        chunk.from_list(list.reverse(state.buffer)),
        build_window_stream(
          WindowState(..state, buffer: [], window_start: deadline),
          ms,
        ),
      )
  }
}
