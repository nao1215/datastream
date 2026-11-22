//// BEAM-only bounded-concurrency combinators.
////
//// This module is the single place where multiple processes get
//// involved, so callers can tell at the import site whether a
//// pipeline runs in parallel.
////
//// Every public function in this module is gated on
//// `@target(erlang)`. On the JavaScript target the module still
//// compiles, so `import datastream/erlang/par` itself does not fail,
//// but calling any function fails at the call site. The
//// `beam_only_marker` constant exists solely to keep the module
//// non-empty on the JavaScript target.
////
//// ## Concurrency knobs
////
//// - `max_workers`: degree of parallelism. The maximum number of BEAM
////   processes that run the user function at once. Only `map_*` and
////   `each_*` accept this knob; `merge` is fixed at one worker per
////   input stream.
//// - `max_buffer`: in-flight ceiling. The maximum number of elements
////   pulled from the upstream(s) but not yet emitted to the downstream.
////   Workers in flight (mid-`f(x)`) count toward this ceiling.
////
//// `max_buffer >= max_workers` is required where both apply; otherwise
//// the buffer would fill before parallelism is reached.
////
//// ## What happens when `max_buffer` is reached
////
//// | function | behaviour |
//// |----------|-----------|
//// | `map_unordered` | bound collapses to `max_workers` (results emit on receive); new dispatch is paused |
//// | `map_ordered`   | new dispatch is paused; pending dict + workers cap |
//// | `merge`         | per-worker `Continue` ack is withheld; workers wait |
////
//// ## Defaults
////
//// `default_max_workers = 4` and `default_max_buffer = 16` are the
//// values used by the no-argument `merge`, `map_unordered`,
//// `map_ordered`, `each_unordered`, `each_ordered`. They aim at the
//// common case (mixed CPU + IO work, modest cardinality). Tune via
//// the `_with` variants when:
////
//// - workload is IO-bound (consider `max_workers` of 16-128 and a
////   correspondingly larger `max_buffer`)
//// - workload is CPU-bound (consider `max_workers` equal to
////   `erlang:system_info(schedulers_online)`)
//// - upstream is rate-limited or memory-pressured (lower
////   `max_buffer` to throttle pull rate)
////
//// ## Notes
////
//// - A `f(x)` that `panic`s, or a `merge` / `race` source that
////   panics, closes the upstream, stops the other workers and
////   re-raises the crash as a panic in the process pulling the stream.
//// - `merge` and `race` workers exit and close their source when the
////   process pulling the stream exits. If that process stays alive
////   but drops the stream without reaching `close`, the workers stay
////   resident. Always drive the stream to a terminal.
//// - Unbounded concurrency is intentionally not offered.
////
//// ## Limitation: incompatible with `from_subject`
////
//// Every combinator in this module pulls from its upstream inside a
//// spawned worker process. Streams built from
//// `datastream/erlang/source.from_subject` cannot survive that, since
//// an Erlang `Subject` can only be received by its owning process —
//// the worker would `panic` on the first pull. Wrap subject streams
//// outside the parallel layer (e.g. materialise to a list first) or
//// route them through a non-`par.*` pipeline.

@target(javascript)
/// Sentinel value documenting that this module is BEAM-only.
pub const beam_only_marker: String = "datastream/erlang/par is BEAM-only"

@target(erlang)
/// Default `max_workers` for the no-argument variants of `map_*` and
/// `each_*`.
pub const default_max_workers: Int = 4

@target(erlang)
/// Default `max_buffer` for the no-argument variants of `merge`,
/// `map_*`, and `each_*`.
pub const default_max_buffer: Int = 16

@target(erlang)
import datastream.{type Stream, Done, Next}

@target(erlang)
import datastream/erlang/internal/pump.{
  type Mailbox, type Reply, type Worker, Element, Exhausted, Received, TimedOut,
  WorkerDown,
}

@target(erlang)
import datastream/fold

@target(erlang)
import datastream/source

@target(erlang)
import gleam/dict.{type Dict}

@target(erlang)
import gleam/erlang/process.{type Monitor, type Pid}

@target(erlang)
import gleam/list

@target(erlang)
import gleam/option.{type Option, None, Some}

@target(erlang)
import gleam/result

// --- common argument validation --------------------------------------------

@target(erlang)
/// Why a checked `par.*` constructor refused its arguments.
///
/// Returned by `map_unordered_with_checked`,
/// `map_ordered_with_checked`, `each_unordered_with_checked`,
/// `each_ordered_with_checked`, and `merge_with_checked`. Lets the
/// caller surface argument-validation failures through `Result`
/// instead of crashing the process — useful when the numeric
/// arguments come from CLI flags, config files, or request
/// parameters.
///
/// `function` names the constructor that rejected the value
/// (`"map_unordered_with"`, `"map_ordered_with"`,
/// `"each_unordered_with"`, `"each_ordered_with"`, `"merge_with"`)
/// so a caller routing many checked constructors through the same
/// handler can produce a meaningful error message.
pub type ParArgError {
  /// `max_workers` was less than 1. `given` is the value the caller
  /// passed.
  InvalidMaxWorkers(function: String, given: Int)
  /// `max_buffer` was less than `max_workers` for `map_*_with` /
  /// `each_*_with`. Both values are reported so the caller can fix
  /// either side.
  BufferLessThanWorkers(function: String, max_workers: Int, max_buffer: Int)
  /// `max_buffer` was less than 1 for `merge_with`. `given` is the
  /// value the caller passed.
  InvalidMaxBuffer(function: String, given: Int)
}

@target(erlang)
/// The panicking constructors are the checked ones plus this.
fn or_panic(checked: Result(a, ParArgError)) -> a {
  case checked {
    Ok(value) -> value
    Error(InvalidMaxWorkers(..)) ->
      panic as "datastream/erlang/par: max_workers must be >= 1"
    Error(BufferLessThanWorkers(..)) ->
      panic as "datastream/erlang/par: max_buffer must be >= max_workers"
    Error(InvalidMaxBuffer(..)) ->
      panic as "datastream/erlang/par.merge: max_buffer must be >= 1"
  }
}

@target(erlang)
fn check_par_args(
  function: String,
  max_workers: Int,
  max_buffer: Int,
) -> Result(Nil, ParArgError) {
  case max_workers >= 1 {
    False -> Error(InvalidMaxWorkers(function: function, given: max_workers))
    True ->
      case max_buffer >= max_workers {
        False ->
          Error(BufferLessThanWorkers(
            function: function,
            max_workers: max_workers,
            max_buffer: max_buffer,
          ))
        True -> Ok(Nil)
      }
  }
}

@target(erlang)
fn check_buffer(function: String, max_buffer: Int) -> Result(Nil, ParArgError) {
  case max_buffer >= 1 {
    True -> Ok(Nil)
    False -> Error(InvalidMaxBuffer(function: function, given: max_buffer))
  }
}

// --- map_unordered / map_ordered ---------------------------------------------

@target(erlang)
/// Whether `map_*` emits results in input order or as they finish.
type Order {
  InputOrder
  CompletionOrder
}

@target(erlang)
type MapSettings(a, b) {
  MapSettings(f: fn(a) -> b, max_workers: Int, max_buffer: Int, order: Order)
}

@target(erlang)
/// One run of a `map_*` stream. `running` maps each busy worker's
/// dispatch index to its process and monitor, `watching` maps the
/// monitors back to the index, and `finished` holds results that wait
/// for an earlier one under `InputOrder`.
type MapRun(a, b) {
  MapRun(
    settings: MapSettings(a, b),
    source: Stream(a),
    source_drained: Bool,
    mailbox: Mailbox(#(Int, b)),
    running: Dict(Int, #(Pid, Monitor)),
    watching: Dict(Monitor, Int),
    finished: Dict(Int, b),
    next_dispatch_index: Int,
    next_emit_index: Int,
  )
}

@target(erlang)
/// Run `f` in parallel using `default_max_workers` BEAM processes,
/// emitting results in the order they finish.
///
/// See `map_unordered_with` for tunable concurrency.
pub fn map_unordered(over stream: Stream(a), with f: fn(a) -> b) -> Stream(b) {
  map_unordered_with(
    over: stream,
    with: f,
    max_workers: default_max_workers,
    max_buffer: default_max_buffer,
  )
}

@target(erlang)
/// Run `f` in parallel across at most `max_workers` BEAM processes.
/// Results are emitted in the order they finish, NOT input order.
///
/// `max_workers >= 1` and `max_buffer >= max_workers` are required;
/// violations `panic` at construction. The `max_buffer >= max_workers`
/// rule is enforced uniformly across `par.*` so callers can swap
/// between `map_unordered_with`, `map_ordered_with`, and `merge_with`
/// without re-tuning. Note however that `map_unordered` emits each
/// result the moment it arrives, so `in_flight = workers_busy` and
/// `max_buffer` adds no extra ceiling beyond `max_workers` here. See
/// `default_max_workers` and `default_max_buffer` for sensible
/// starting values.
pub fn map_unordered_with(
  over stream: Stream(a),
  with f: fn(a) -> b,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Stream(b) {
  map_unordered_with_checked(over: stream, with: f, max_workers:, max_buffer:)
  |> or_panic
}

@target(erlang)
/// Like `map_unordered_with`, but returns the argument-validation
/// failure as a `Result` instead of panicking. Use this when
/// `max_workers` or `max_buffer` come from dynamic input (CLI,
/// config, request parameters); the panicking `map_unordered_with`
/// remains the right tool for trusted constants.
///
/// On success the stream behaves identically to
/// `map_unordered_with(over: stream, with: f, max_workers: ...,
/// max_buffer: ...)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error`.
pub fn map_unordered_with_checked(
  over stream: Stream(a),
  with f: fn(a) -> b,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Result(Stream(b), ParArgError) {
  use Nil <- result.map(check_par_args(
    "map_unordered_with",
    max_workers,
    max_buffer,
  ))
  map_stream(
    stream,
    MapSettings(f:, max_workers:, max_buffer:, order: CompletionOrder),
  )
}

@target(erlang)
/// Run `f` in parallel using `default_max_workers` BEAM processes,
/// emitting results in input order.
///
/// See `map_ordered_with` for tunable concurrency.
pub fn map_ordered(over stream: Stream(a), with f: fn(a) -> b) -> Stream(b) {
  map_ordered_with(
    over: stream,
    with: f,
    max_workers: default_max_workers,
    max_buffer: default_max_buffer,
  )
}

@target(erlang)
/// Run `f` in parallel across at most `max_workers` BEAM processes,
/// emitting results in input order.
///
/// `max_buffer >= max_workers` is REQUIRED: if `max_buffer` were
/// smaller, the buffer would fill while waiting for an early result
/// and ordering could not be enforced without stalling. Violation
/// panics at construction. `in_flight = workers_busy + pending dict
/// size`; dispatch is paused once it reaches `max_buffer`.
pub fn map_ordered_with(
  over stream: Stream(a),
  with f: fn(a) -> b,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Stream(b) {
  map_ordered_with_checked(over: stream, with: f, max_workers:, max_buffer:)
  |> or_panic
}

@target(erlang)
/// Like `map_ordered_with`, but returns the argument-validation
/// failure as a `Result` instead of panicking. Use this when
/// `max_workers` or `max_buffer` come from dynamic input (CLI,
/// config, request parameters); the panicking `map_ordered_with`
/// remains the right tool for trusted constants.
///
/// On success the stream behaves identically to
/// `map_ordered_with(over: stream, with: f, max_workers: ...,
/// max_buffer: ...)`.
///
/// The caller is responsible for closing `stream` if the
/// constructor returns `Error`.
pub fn map_ordered_with_checked(
  over stream: Stream(a),
  with f: fn(a) -> b,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Result(Stream(b), ParArgError) {
  use Nil <- result.map(check_par_args(
    "map_ordered_with",
    max_workers,
    max_buffer,
  ))
  map_stream(
    stream,
    MapSettings(f:, max_workers:, max_buffer:, order: InputOrder),
  )
}

@target(erlang)
/// The mailbox and workers are created on the first pull, in the
/// process that pulls, so every run starts clean.
fn map_stream(stream: Stream(a), settings: MapSettings(a, b)) -> Stream(b) {
  datastream.make(
    pull: fn() {
      map_step(MapRun(
        settings:,
        source: stream,
        source_drained: False,
        mailbox: pump.new_mailbox(),
        running: dict.new(),
        watching: dict.new(),
        finished: dict.new(),
        next_dispatch_index: 0,
        next_emit_index: 0,
      ))
    },
    close: fn() { datastream.close(stream) },
  )
}

@target(erlang)
fn map_active(run: MapRun(a, b)) -> Stream(b) {
  datastream.make(pull: fn() { map_step(run) }, close: fn() { map_abort(run) })
}

@target(erlang)
/// Close the upstream unless it is drained, kill the busy workers and
/// drop whatever they still send.
fn map_abort(run: MapRun(a, b)) -> Nil {
  case run.source_drained {
    True -> Nil
    False -> datastream.close(run.source)
  }
  dict.each(run.running, fn(_, worker) {
    let #(pid, monitor) = worker
    process.demonitor_process(monitor)
    process.kill(pid)
  })
  pump.close_mailbox(run.mailbox)
}

@target(erlang)
fn map_step(run: MapRun(a, b)) -> datastream.Step(b, Stream(b)) {
  let run = map_dispatch(run)
  case dict.get(run.finished, run.next_emit_index), dict.is_empty(run.running) {
    Ok(value), _ ->
      Next(
        value,
        map_active(
          MapRun(
            ..run,
            finished: dict.delete(run.finished, run.next_emit_index),
            next_emit_index: run.next_emit_index + 1,
          ),
        ),
      )
    Error(_), True -> {
      pump.close_mailbox(run.mailbox)
      Done
    }
    Error(_), False ->
      case pump.receive_forever(from: run.mailbox, watching: run.watching) {
        Received(#(index, value)) -> {
          let run = map_settle(run, index)
          case run.settings.order {
            CompletionOrder -> Next(value, map_active(run))
            InputOrder ->
              map_step(
                MapRun(..run, finished: dict.insert(run.finished, index, value)),
              )
          }
        }
        WorkerDown(reason:, ..) -> {
          map_abort(run)
          pump.raise_worker_crash("datastream/erlang/par", reason)
        }
        TimedOut -> map_step(run)
      }
  }
}

@target(erlang)
/// Forget the worker for `index` once its result has arrived.
fn map_settle(run: MapRun(a, b), index: Int) -> MapRun(a, b) {
  case dict.get(run.running, index) {
    Ok(#(_, monitor)) -> {
      process.demonitor_process(monitor)
      MapRun(
        ..run,
        running: dict.delete(run.running, index),
        watching: dict.delete(run.watching, monitor),
      )
    }
    Error(_) -> run
  }
}

@target(erlang)
fn map_dispatch(run: MapRun(a, b)) -> MapRun(a, b) {
  let busy = dict.size(run.running)
  let in_flight = busy + dict.size(run.finished)
  case
    run.source_drained
    || busy >= run.settings.max_workers
    || in_flight >= run.settings.max_buffer
  {
    True -> run
    False ->
      case datastream.pull(run.source) {
        Done -> MapRun(..run, source_drained: True)
        Next(element, rest) -> {
          let index = run.next_dispatch_index
          let function = run.settings.f
          let #(pid, monitor) =
            pump.spawn_task(into: run.mailbox, tagged: index, running: fn() {
              function(element)
            })
          map_dispatch(
            MapRun(
              ..run,
              source: rest,
              running: dict.insert(run.running, index, #(pid, monitor)),
              watching: dict.insert(run.watching, monitor, index),
              next_dispatch_index: index + 1,
            ),
          )
        }
      }
  }
}

// --- each_ordered / each_unordered ----------------------------------------

@target(erlang)
/// Side-effect-only variant of `map_ordered`. Uses
/// `default_max_workers` and `default_max_buffer`.
pub fn each_ordered(over stream: Stream(a), with effect: fn(a) -> Nil) -> Nil {
  each_ordered_with(
    over: stream,
    with: effect,
    max_workers: default_max_workers,
    max_buffer: default_max_buffer,
  )
}

@target(erlang)
/// Side-effect-only variant of `map_ordered_with`.
pub fn each_ordered_with(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Nil {
  each_ordered_with_checked(
    over: stream,
    with: effect,
    max_workers:,
    max_buffer:,
  )
  |> or_panic
}

@target(erlang)
/// Like `each_ordered_with`, but returns the argument-validation
/// failure as a `Result` instead of panicking. The effect runs only
/// when the arguments validate; on `Error` no upstream pull happens
/// and the caller is responsible for closing `stream`.
pub fn each_ordered_with_checked(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Result(Nil, ParArgError) {
  map_ordered_with_checked(
    over: stream,
    with: effect,
    max_workers:,
    max_buffer:,
  )
  |> result.map(fold.drain)
  |> result.map_error(rename_function(_, "each_ordered_with"))
}

@target(erlang)
/// Side-effect-only variant of `map_unordered`. Uses
/// `default_max_workers` and `default_max_buffer`.
pub fn each_unordered(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
) -> Nil {
  each_unordered_with(
    over: stream,
    with: effect,
    max_workers: default_max_workers,
    max_buffer: default_max_buffer,
  )
}

@target(erlang)
/// Side-effect-only variant of `map_unordered_with`.
pub fn each_unordered_with(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Nil {
  each_unordered_with_checked(
    over: stream,
    with: effect,
    max_workers:,
    max_buffer:,
  )
  |> or_panic
}

@target(erlang)
/// Like `each_unordered_with`, but returns the argument-validation
/// failure as a `Result` instead of panicking. The effect runs only
/// when the arguments validate; on `Error` no upstream pull happens
/// and the caller is responsible for closing `stream`.
pub fn each_unordered_with_checked(
  over stream: Stream(a),
  with effect: fn(a) -> Nil,
  max_workers max_workers: Int,
  max_buffer max_buffer: Int,
) -> Result(Nil, ParArgError) {
  map_unordered_with_checked(
    over: stream,
    with: effect,
    max_workers:,
    max_buffer:,
  )
  |> result.map(fold.drain)
  |> result.map_error(rename_function(_, "each_unordered_with"))
}

@target(erlang)
fn rename_function(error: ParArgError, function: String) -> ParArgError {
  case error {
    InvalidMaxWorkers(..) -> InvalidMaxWorkers(..error, function:)
    BufferLessThanWorkers(..) -> BufferLessThanWorkers(..error, function:)
    InvalidMaxBuffer(..) -> InvalidMaxBuffer(..error, function:)
  }
}

// --- merge ---------------------------------------------------------------

@target(erlang)
/// One run of a `merge` stream. `workers` holds the pumps whose source
/// has not ended, by index. `waiting_front` / `waiting_back` form a
/// two-list queue of pumps waiting for a request, so that at most
/// `max_buffer` requests are outstanding. `last_emitted` is the pump
/// whose element was handed downstream last; it rejoins the queue on
/// the next pull.
type MergeRun(a) {
  MergeRun(
    mailbox: Mailbox(#(Int, Reply(a))),
    workers: Dict(Int, Worker),
    watching: Dict(Monitor, Int),
    waiting_front: List(Int),
    waiting_back: List(Int),
    last_emitted: Option(Int),
  )
}

@target(erlang)
/// Interleave elements from every input stream in arrival order. Uses
/// `default_max_buffer` as the in-flight ceiling.
///
/// See `merge_with` for tunable buffering.
pub fn merge(streams streams: List(Stream(a))) -> Stream(a) {
  merge_with(streams: streams, max_buffer: default_max_buffer)
}

@target(erlang)
/// Interleave elements from every input stream in arrival order.
///
/// Order from each individual source is preserved relative to that
/// source; cross-source order follows whichever worker process
/// happens to deliver first.
///
/// `max_buffer` bounds the number of in-flight elements: pulled by
/// any worker but not yet emitted to the downstream. Implementation:
/// at startup at most `max_buffer` workers are issued a `Continue`
/// signal; the rest wait. Each worker pulls one element, sends it,
/// and waits for the next `Continue`. The coordinator hands a
/// `Continue` to the next waiting worker every time it emits an
/// element downstream. If `max_buffer < len(streams)`, the late-
/// starting workers wait until earlier ones drain.
///
/// `max_buffer >= 1` is required.
pub fn merge_with(
  streams streams: List(Stream(a)),
  max_buffer max_buffer: Int,
) -> Stream(a) {
  merge_with_checked(streams:, max_buffer:) |> or_panic
}

@target(erlang)
/// Like `merge_with`, but returns the argument-validation failure
/// as a `Result` instead of panicking. Use this when `max_buffer`
/// comes from dynamic input (CLI, config, request parameters); the
/// panicking `merge_with` remains the right tool for trusted
/// constants.
///
/// On success the stream behaves identically to
/// `merge_with(streams: streams, max_buffer: max_buffer)`.
///
/// The caller is responsible for closing each stream in `streams`
/// if the constructor returns `Error`.
pub fn merge_with_checked(
  streams streams: List(Stream(a)),
  max_buffer max_buffer: Int,
) -> Result(Stream(a), ParArgError) {
  use Nil <- result.map(check_buffer("merge_with", max_buffer))
  datastream.make(
    pull: fn() { merge_step(start_merge(streams, max_buffer)) },
    close: fn() { list.each(streams, datastream.close) },
  )
}

@target(erlang)
fn start_merge(streams: List(Stream(a)), max_buffer: Int) -> MergeRun(a) {
  let mailbox = pump.new_mailbox()
  let workers =
    list.index_map(streams, fn(stream, index) {
      #(index, pump.spawn_pump(over: stream, into: mailbox, tagged: index))
    })
  let #(started, waiting) = list.split(workers, max_buffer)
  list.each(started, fn(entry) { pump.request(entry.1) })
  MergeRun(
    mailbox:,
    workers: dict.from_list(workers),
    watching: watch_all(workers),
    waiting_front: list.map(waiting, fn(entry) { entry.0 }),
    waiting_back: [],
    last_emitted: None,
  )
}

@target(erlang)
fn watch_all(workers: List(#(Int, Worker))) -> Dict(Monitor, Int) {
  list.fold(workers, dict.new(), fn(watching, entry) {
    dict.insert(watching, pump.monitor_of(entry.1), entry.0)
  })
}

@target(erlang)
fn merge_active(run: MergeRun(a)) -> Stream(a) {
  datastream.make(pull: fn() { merge_step(run) }, close: fn() {
    stop_all(run.workers, run.mailbox)
  })
}

@target(erlang)
fn stop_all(workers: Dict(Int, Worker), mailbox: Mailbox(message)) -> Nil {
  dict.each(workers, fn(_, worker) { pump.stop(worker) })
  pump.close_mailbox(mailbox)
}

@target(erlang)
fn merge_step(run: MergeRun(a)) -> datastream.Step(a, Stream(a)) {
  let run = case run.last_emitted {
    Some(index) ->
      request_next(
        MergeRun(
          ..run,
          waiting_back: [index, ..run.waiting_back],
          last_emitted: None,
        ),
      )
    None -> run
  }
  case dict.is_empty(run.workers) {
    True -> {
      pump.close_mailbox(run.mailbox)
      Done
    }
    False ->
      case pump.receive_forever(from: run.mailbox, watching: run.watching) {
        Received(#(index, Element(element))) ->
          Next(
            element,
            merge_active(MergeRun(..run, last_emitted: Some(index))),
          )
        Received(#(index, Exhausted)) ->
          merge_step(request_next(merge_forget(run, index)))
        WorkerDown(reason:, ..) -> {
          stop_all(run.workers, run.mailbox)
          pump.raise_worker_crash("datastream/erlang/par.merge", reason)
        }
        TimedOut -> merge_step(run)
      }
  }
}

@target(erlang)
fn merge_forget(run: MergeRun(a), index: Int) -> MergeRun(a) {
  case dict.get(run.workers, index) {
    Ok(worker) -> {
      pump.forget(worker)
      MergeRun(
        ..run,
        workers: dict.delete(run.workers, index),
        watching: dict.delete(run.watching, pump.monitor_of(worker)),
      )
    }
    Error(_) -> run
  }
}

@target(erlang)
/// Hand the freed request to the pump that has waited longest.
fn request_next(run: MergeRun(a)) -> MergeRun(a) {
  let #(front, back) = case run.waiting_front {
    [] -> #(list.reverse(run.waiting_back), [])
    front -> #(front, run.waiting_back)
  }
  case front {
    [index, ..front] -> {
      case dict.get(run.workers, index) {
        Ok(worker) -> pump.request(worker)
        Error(_) -> Nil
      }
      MergeRun(..run, waiting_front: front, waiting_back: back)
    }
    [] -> MergeRun(..run, waiting_front: [], waiting_back: [])
  }
}

// --- race ----------------------------------------------------------------

@target(erlang)
type RaceState(a) {
  /// No source has produced an element yet; `workers` holds the
  /// sources that have not ended.
  RaceWaiting(
    mailbox: Mailbox(#(Int, Reply(a))),
    workers: Dict(Int, Worker),
    watching: Dict(Monitor, Int),
  )
  /// Source `index` won and is the only one still pulled.
  RaceWinning(
    mailbox: Mailbox(#(Int, Reply(a))),
    index: Int,
    winner: Worker,
    watching: Dict(Monitor, Int),
  )
}

@target(erlang)
/// Wait for the first source to emit; signal the other workers to
/// stop, then continue with the winning source until it halts.
///
/// Implementation: on the first pull, spawn one worker per source and
/// ask each for one element. The first element to arrive wins; every
/// other worker is told to stop and closes its source once its pull in
/// progress returns. The winning worker keeps pulling its source one
/// element per downstream pull.
pub fn race(streams streams: List(Stream(a))) -> Stream(a) {
  case streams {
    [] -> source.empty()
    _ ->
      datastream.make(
        pull: fn() { race_step(start_race(streams)) },
        close: fn() { list.each(streams, datastream.close) },
      )
  }
}

@target(erlang)
fn start_race(streams: List(Stream(a))) -> RaceState(a) {
  let mailbox = pump.new_mailbox()
  let workers =
    list.index_map(streams, fn(stream, index) {
      let worker = pump.spawn_pump(over: stream, into: mailbox, tagged: index)
      pump.request(worker)
      #(index, worker)
    })
  RaceWaiting(
    mailbox:,
    workers: dict.from_list(workers),
    watching: watch_all(workers),
  )
}

@target(erlang)
fn race_active(state: RaceState(a)) -> Stream(a) {
  datastream.make(pull: fn() { race_step(state) }, close: fn() {
    case state {
      RaceWaiting(mailbox:, workers:, ..) -> stop_all(workers, mailbox)
      RaceWinning(mailbox:, winner:, ..) -> {
        pump.stop(winner)
        pump.close_mailbox(mailbox)
      }
    }
  })
}

@target(erlang)
fn race_step(state: RaceState(a)) -> datastream.Step(a, Stream(a)) {
  case state {
    RaceWaiting(mailbox:, workers:, watching:) ->
      case dict.is_empty(workers) {
        True -> {
          pump.close_mailbox(mailbox)
          Done
        }
        False -> race_wait(mailbox, workers, watching)
      }
    RaceWinning(mailbox:, index:, winner:, watching:) -> {
      pump.request(winner)
      race_follow(mailbox, index, winner, watching)
    }
  }
}

@target(erlang)
fn race_wait(
  mailbox: Mailbox(#(Int, Reply(a))),
  workers: Dict(Int, Worker),
  watching: Dict(Monitor, Int),
) -> datastream.Step(a, Stream(a)) {
  case pump.receive_forever(from: mailbox, watching:) {
    Received(#(index, Element(element))) -> {
      let #(winner, losers) = case dict.get(workers, index) {
        Ok(winner) -> #(Ok(winner), dict.delete(workers, index))
        Error(_) -> #(Error(Nil), workers)
      }
      dict.each(losers, fn(_, loser) { pump.stop(loser) })
      case winner {
        Ok(winner) ->
          Next(
            element,
            race_active(RaceWinning(
              mailbox:,
              index:,
              winner:,
              watching: dict.from_list([#(pump.monitor_of(winner), index)]),
            )),
          )
        Error(_) ->
          race_step(RaceWaiting(
            mailbox:,
            workers: dict.new(),
            watching: dict.new(),
          ))
      }
    }
    Received(#(index, Exhausted)) -> {
      case dict.get(workers, index) {
        Ok(worker) -> pump.forget(worker)
        Error(_) -> Nil
      }
      race_step(RaceWaiting(
        mailbox:,
        workers: dict.delete(workers, index),
        watching:,
      ))
    }
    WorkerDown(reason:, ..) -> {
      stop_all(workers, mailbox)
      pump.raise_worker_crash("datastream/erlang/par.race", reason)
    }
    TimedOut -> race_wait(mailbox, workers, watching)
  }
}

@target(erlang)
fn race_follow(
  mailbox: Mailbox(#(Int, Reply(a))),
  index: Int,
  winner: Worker,
  watching: Dict(Monitor, Int),
) -> datastream.Step(a, Stream(a)) {
  case pump.receive_forever(from: mailbox, watching:) {
    // A stopped loser may still deliver the element it was pulling.
    Received(#(from, _)) if from != index ->
      race_follow(mailbox, index, winner, watching)
    Received(#(_, Element(element))) ->
      Next(
        element,
        race_active(RaceWinning(mailbox:, index:, winner:, watching:)),
      )
    Received(#(_, Exhausted)) -> {
      pump.forget(winner)
      pump.close_mailbox(mailbox)
      Done
    }
    WorkerDown(reason:, ..) -> {
      pump.close_mailbox(mailbox)
      pump.raise_worker_crash("datastream/erlang/par.race", reason)
    }
    TimedOut -> race_follow(mailbox, index, winner, watching)
  }
}
