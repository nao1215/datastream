//// BEAM-only time-based combinators.
////
//// Every combinator here observes wall-clock behaviour: bursts get
//// debounced / throttled / sampled / windowed against BEAM monotonic
//// time, never against the wall clock that can jump backwards.
////
//// Implementation strategy: on its first pull each combinator spawns
//// one worker process that pulls the upstream one element per request.
//// The consuming process waits for the next element and for a timer
//// deadline at the same time.
////
//// Every public function in this module is gated on
//// `@target(erlang)`. On the JavaScript target the module still
//// compiles, so `import datastream/erlang/time` itself does not fail,
//// but calling any function fails at the call site. The
//// `beam_only_marker` constant exists solely to keep the module
//// non-empty on the JavaScript target.
////
//// ## Limitation: incompatible with `from_subject`
////
//// Because every combinator pulls inside a spawned worker process,
//// streams built from `datastream/erlang/source.from_subject` cannot
//// be passed through this layer — an Erlang `Subject` can only be
//// received by its owning process and the worker would `panic` on
//// the first pull. The same constraint applies to
//// `datastream/erlang/par`.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only.
pub const beam_only_marker: String = "datastream/erlang/time is BEAM-only"

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import datastream/chunk.{type Chunk}

@target(erlang)
import datastream/erlang/internal/pump.{type Pump, Element, Exhausted}

@target(erlang)
import gleam/erlang/process

@target(erlang)
import gleam/list

@target(erlang)
import gleam/option.{type Option, None, Some}

@target(erlang)
/// Milliseconds remaining until `deadline`, clamped to a minimum of
/// 0. Used as the `within` argument for `process.receive`.
fn ms_until(deadline: Int) -> Int {
  case deadline - pump.now_ms() {
    delta if delta > 0 -> delta
    _ -> 0
  }
}

// --- shared close handler ------------------------------------------------

@target(erlang)
fn maybe_stop(upstream: Pump(a), upstream_done: Bool) -> Nil {
  case upstream_done {
    True -> Nil
    False -> pump.stop_pump(upstream)
  }
}

// --- debounce ------------------------------------------------------------

@target(erlang)
type DebounceState(a) {
  DebounceState(
    upstream: Pump(a),
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
  use upstream, _now <- pump.on_first_pull(
    stream,
    context: "datastream/erlang/time.debounce",
  )
  debounce_step(
    DebounceState(upstream:, latest: None, deadline: 0, upstream_done: False),
    ms,
  )
}

@target(erlang)
fn build_debounce_stream(state: DebounceState(a), ms: Int) -> Stream(a) {
  datastream.make(pull: fn() { debounce_step(state, ms) }, close: fn() {
    maybe_stop(state.upstream, state.upstream_done)
  })
}

@target(erlang)
fn debounce_step(
  state: DebounceState(a),
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  case state.upstream_done, state.latest {
    True, _ -> Done
    False, None ->
      case pump.next_forever(state.upstream) {
        #(upstream, Element(element)) ->
          debounce_step(restart_quiet_period(state, upstream, element, ms), ms)
        #(_, Exhausted) -> Done
      }
    False, Some(value) ->
      case pump.next(state.upstream, within: ms_until(state.deadline)) {
        #(upstream, Ok(Element(element))) ->
          debounce_step(restart_quiet_period(state, upstream, element, ms), ms)
        #(upstream, Ok(Exhausted)) ->
          Next(
            value,
            build_debounce_stream(
              DebounceState(
                ..state,
                upstream:,
                latest: None,
                upstream_done: True,
              ),
              ms,
            ),
          )
        #(upstream, Error(_)) ->
          Next(
            value,
            build_debounce_stream(
              DebounceState(..state, upstream:, latest: None),
              ms,
            ),
          )
      }
  }
}

@target(erlang)
/// A new element arrived: it becomes the candidate and the quiet period
/// starts again.
fn restart_quiet_period(
  state: DebounceState(a),
  upstream: Pump(a),
  element: a,
  ms: Int,
) -> DebounceState(a) {
  DebounceState(
    ..state,
    upstream:,
    latest: Some(element),
    deadline: pump.now_ms() + ms,
  )
}

// --- throttle ------------------------------------------------------------

@target(erlang)
type ThrottleState(a) {
  ThrottleState(upstream: Pump(a), next_window_start: Option(Int))
}

@target(erlang)
/// Emit the FIRST element of each `ms`-wide window; drop the rest.
///
/// Windows start at the moment the first element is emitted.
pub fn throttle(over stream: Stream(a), every ms: Int) -> Stream(a) {
  use upstream, _now <- pump.on_first_pull(
    stream,
    context: "datastream/erlang/time.throttle",
  )
  throttle_step(ThrottleState(upstream:, next_window_start: None), ms)
}

@target(erlang)
fn build_throttle_stream(state: ThrottleState(a), ms: Int) -> Stream(a) {
  datastream.make(pull: fn() { throttle_step(state, ms) }, close: fn() {
    pump.stop_pump(state.upstream)
  })
}

@target(erlang)
fn throttle_step(
  state: ThrottleState(a),
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  case pump.next_forever(state.upstream) {
    #(_, Exhausted) -> Done
    #(upstream, Element(element)) -> {
      let state = ThrottleState(..state, upstream:)
      let now = pump.now_ms()
      case state.next_window_start {
        Some(start) if now < start -> throttle_step(state, ms)
        _ ->
          Next(
            element,
            build_throttle_stream(
              ThrottleState(..state, next_window_start: Some(now + ms)),
              ms,
            ),
          )
      }
    }
  }
}

// --- sample --------------------------------------------------------------

@target(erlang)
type SampleState(a) {
  SampleState(
    upstream: Pump(a),
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
  use upstream, now <- pump.on_first_pull(
    stream,
    context: "datastream/erlang/time.sample",
  )
  sample_step(
    SampleState(
      upstream:,
      window_start: now,
      latest: None,
      upstream_done: False,
    ),
    ms,
  )
}

@target(erlang)
fn build_sample_stream(state: SampleState(a), ms: Int) -> Stream(a) {
  datastream.make(pull: fn() { sample_step(state, ms) }, close: fn() {
    maybe_stop(state.upstream, state.upstream_done)
  })
}

@target(erlang)
fn sample_step(
  state: SampleState(a),
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  case state.upstream_done {
    True -> Done
    False -> sample_wait(state, ms)
  }
}

@target(erlang)
fn sample_wait(
  state: SampleState(a),
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  let deadline = state.window_start + ms
  case pump.next(state.upstream, within: ms_until(deadline)), state.latest {
    #(upstream, Ok(Element(element))), _ ->
      sample_wait(SampleState(..state, upstream:, latest: Some(element)), ms)
    #(_, Ok(Exhausted)), None -> Done
    #(upstream, Ok(Exhausted)), Some(value) ->
      Next(
        value,
        build_sample_stream(
          SampleState(..state, upstream:, latest: None, upstream_done: True),
          ms,
        ),
      )
    #(upstream, Error(_)), None ->
      sample_wait(SampleState(..state, upstream:, window_start: deadline), ms)
    #(upstream, Error(_)), Some(value) ->
      Next(
        value,
        build_sample_stream(
          SampleState(..state, upstream:, window_start: deadline, latest: None),
          ms,
        ),
      )
  }
}

// --- rate_limit ----------------------------------------------------------

@target(erlang)
type RateState(a) {
  RateState(upstream: Pump(a), window_start: Int, count_in_window: Int)
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
  use upstream, now <- pump.on_first_pull(
    stream,
    context: "datastream/erlang/time.rate_limit",
  )
  rate_limit_step(
    RateState(upstream:, window_start: now, count_in_window: 0),
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
    pump.stop_pump(state.upstream)
  })
}

@target(erlang)
fn rate_limit_step(
  state: RateState(a),
  count: Int,
  ms: Int,
) -> datastream.Step(a, Stream(a)) {
  case pump.next_forever(state.upstream) {
    #(_, Exhausted) -> Done
    #(upstream, Element(element)) -> {
      let now = pump.now_ms()
      let state = case now >= state.window_start + ms {
        True -> RateState(upstream:, window_start: now, count_in_window: 0)
        False -> RateState(..state, upstream:)
      }
      let state = case state.count_in_window < count {
        True -> RateState(..state, count_in_window: state.count_in_window + 1)
        False -> {
          // Over the limit: wait for the window to end, then start a new
          // one with this element in it.
          process.sleep(ms_until(state.window_start + ms))
          RateState(..state, window_start: pump.now_ms(), count_in_window: 1)
        }
      }
      Next(element, build_rate_limit_stream(state, count, ms))
    }
  }
}

// --- window_time ---------------------------------------------------------

@target(erlang)
type WindowState(a) {
  WindowState(
    upstream: Pump(a),
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
  use upstream, now <- pump.on_first_pull(
    stream,
    context: "datastream/erlang/time.window_time",
  )
  window_step(
    WindowState(upstream:, buffer: [], window_start: now, upstream_done: False),
    ms,
  )
}

@target(erlang)
fn build_window_stream(state: WindowState(a), ms: Int) -> Stream(Chunk(a)) {
  datastream.make(pull: fn() { window_step(state, ms) }, close: fn() {
    maybe_stop(state.upstream, state.upstream_done)
  })
}

@target(erlang)
fn window_step(
  state: WindowState(a),
  ms: Int,
) -> datastream.Step(Chunk(a), Stream(Chunk(a))) {
  case state.upstream_done, state.buffer {
    True, [] -> Done
    True, buffer ->
      Next(
        chunk.from_list(list.reverse(buffer)),
        build_window_stream(WindowState(..state, buffer: []), ms),
      )
    False, _ -> {
      let deadline = state.window_start + ms
      case pump.next(state.upstream, within: ms_until(deadline)) {
        #(upstream, Ok(Element(element))) ->
          window_step(
            WindowState(..state, upstream:, buffer: [element, ..state.buffer]),
            ms,
          )
        #(upstream, Ok(Exhausted)) ->
          window_step(WindowState(..state, upstream:, upstream_done: True), ms)
        #(upstream, Error(_)) ->
          Next(
            chunk.from_list(list.reverse(state.buffer)),
            build_window_stream(
              WindowState(
                ..state,
                upstream:,
                buffer: [],
                window_start: deadline,
              ),
              ms,
            ),
          )
      }
    }
  }
}
