//// Single-stream middle and composition combinators over `Stream(a)`.
////
//// All combinators are lazy and order-preserving (unless documented
//// otherwise). Building a pipeline with these combinators triggers no
//// user callbacks; callbacks fire only when a terminal in `fold` or
//// `sink` pulls the result.
////
//// Early-exit combinators (`take`, `take_while`) stop pulling upstream
//// the moment their result is determined, which is what makes them
//// safe on infinite sources.
////
//// Every combinator here forwards the upstream's `close` callback so
//// resource-backed streams (`source.resource`) are released on every
//// termination path. Combinators that observe a `Done` from the
//// upstream do NOT call `close` again — the source closed itself when
//// it returned `Done`. Combinators that hold multiple upstreams open
//// at once close them in the order specified in the spec
//// (`flat_map`: inner before outer; `zip`: right before left).

import datastream.{type Step, type Stream, Done, Next}
import datastream/chunk.{type Chunk}
import gleam/list
import gleam/option.{type Option, None, Some}

fn noop() -> Nil {
  Nil
}

// --- single-stream combinators ----------------------------------------------

/// Apply `f` to every element. Cardinality and order are preserved.
pub fn map(over stream: Stream(a), with f: fn(a) -> b) -> Stream(b) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) -> Next(f(element), map(over: rest, with: f))
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Keep only the elements for which `predicate` returns `True`.
///
/// Relative order of the survivors is preserved.
pub fn filter(
  over stream: Stream(a),
  keeping predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.make(pull: fn() { filter_pull(stream, predicate) }, close: fn() {
    datastream.close(stream)
  })
}

fn filter_pull(
  stream: Stream(a),
  predicate: fn(a) -> Bool,
) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) ->
      case predicate(element) {
        True -> Next(element, filter(over: rest, keeping: predicate))
        False -> filter_pull(rest, predicate)
      }
    Done -> Done
  }
}

/// Yield at most the first `n` elements; `n <= 0` yields the empty stream.
///
/// Stops pulling upstream the moment the `n`th element has been
/// emitted, and closes the upstream on that early exit. This is what
/// makes `take` safe on infinite resource-backed sources.
pub fn take(from stream: Stream(a), up_to n: Int) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case n <= 0 {
        True -> {
          datastream.close(stream)
          Done
        }
        False ->
          case datastream.pull(stream) {
            Next(element, rest) -> Next(element, take(from: rest, up_to: n - 1))
            Done -> Done
          }
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Discard the first `n` elements; `n <= 0` is the identity.
pub fn drop(from stream: Stream(a), up_to n: Int) -> Stream(a) {
  datastream.make(pull: fn() { drop_pull(stream, n) }, close: fn() {
    datastream.close(stream)
  })
}

fn drop_pull(stream: Stream(a), n: Int) -> Step(a, Stream(a)) {
  case n <= 0 {
    True ->
      case datastream.pull(stream) {
        Next(element, rest) -> Next(element, drop(from: rest, up_to: 0))
        Done -> Done
      }
    False ->
      case datastream.pull(stream) {
        Next(_, rest) -> drop_pull(rest, n - 1)
        Done -> Done
      }
  }
}

/// Yield the longest prefix where `predicate` holds, then stop.
///
/// Stops pulling upstream and closes it as soon as `predicate` returns
/// `False`, so `take_while` terminates on infinite resource-backed
/// sources whose prefix eventually fails the predicate.
pub fn take_while(
  in stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) ->
          case predicate(element) {
            True -> Next(element, take_while(in: rest, satisfying: predicate))
            False -> {
              datastream.close(rest)
              Done
            }
          }
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Discard the longest prefix where `predicate` holds, then yield the rest.
pub fn drop_while(
  in stream: Stream(a),
  satisfying predicate: fn(a) -> Bool,
) -> Stream(a) {
  datastream.make(
    pull: fn() { drop_while_pull(stream, predicate) },
    close: fn() { datastream.close(stream) },
  )
}

fn drop_while_pull(
  stream: Stream(a),
  predicate: fn(a) -> Bool,
) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) ->
      case predicate(element) {
        True -> drop_while_pull(rest, predicate)
        False -> Next(element, rest)
      }
    Done -> Done
  }
}

/// Yield all of `first`, then all of `second`. If `first` is infinite,
/// `second` is never opened.
///
/// Honours the spec close ordering: `second` is opened only after
/// `first` returns `Done` (and so has already closed itself); on early
/// exit before `first` finishes, `second` is never opened.
pub fn append(first: Stream(a), second: Stream(a)) -> Stream(a) {
  datastream.make(pull: fn() { append_pull(first, second) }, close: fn() {
    datastream.close(first)
  })
}

fn append_pull(first: Stream(a), second: Stream(a)) -> Step(a, Stream(a)) {
  case datastream.pull(first) {
    Next(element, rest) -> Next(element, append(rest, second))
    Done -> append_pull_second(second)
  }
}

fn append_pull_second(stream: Stream(a)) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Next(element, rest) -> Next(element, rest)
    Done -> Done
  }
}

// --- composition combinators ------------------------------------------------

/// Apply `f` to each element and keep only the `Some(x)` results.
pub fn filter_map(
  over stream: Stream(a),
  with f: fn(a) -> Option(b),
) -> Stream(b) {
  datastream.make(pull: fn() { filter_map_pull(stream, f) }, close: fn() {
    datastream.close(stream)
  })
}

fn filter_map_pull(
  stream: Stream(a),
  f: fn(a) -> Option(b),
) -> Step(b, Stream(b)) {
  case datastream.pull(stream) {
    Next(element, rest) ->
      case f(element) {
        Some(value) -> Next(value, filter_map(over: rest, with: f))
        None -> filter_map_pull(rest, f)
      }
    Done -> Done
  }
}

/// Apply `f` to each element and concatenate the inner streams it
/// produces.
///
/// Pulls one inner stream at a time; the next inner stream is not
/// constructed until the previous one is exhausted. On early exit
/// before the outer is `Done`, the active inner is closed first, then
/// the outer.
pub fn flat_map(over stream: Stream(a), with f: fn(a) -> Stream(b)) -> Stream(b) {
  flat_map_outer(stream, f)
}

fn flat_map_outer(outer: Stream(a), f: fn(a) -> Stream(b)) -> Stream(b) {
  datastream.make(pull: fn() { flat_map_outer_pull(outer, f) }, close: fn() {
    datastream.close(outer)
  })
}

fn flat_map_outer_pull(
  outer: Stream(a),
  f: fn(a) -> Stream(b),
) -> Step(b, Stream(b)) {
  case datastream.pull(outer) {
    Done -> Done
    Next(element, outer_rest) -> flat_map_inner_pull(f(element), outer_rest, f)
  }
}

fn flat_map_inner(
  inner: Stream(b),
  outer: Stream(a),
  f: fn(a) -> Stream(b),
) -> Stream(b) {
  datastream.make(
    pull: fn() { flat_map_inner_pull(inner, outer, f) },
    close: fn() {
      datastream.close(inner)
      datastream.close(outer)
    },
  )
}

fn flat_map_inner_pull(
  inner: Stream(b),
  outer: Stream(a),
  f: fn(a) -> Stream(b),
) -> Step(b, Stream(b)) {
  case datastream.pull(inner) {
    Next(value, inner_rest) -> Next(value, flat_map_inner(inner_rest, outer, f))
    Done -> flat_map_outer_pull(outer, f)
  }
}

/// Flatten a stream of streams into a single stream.
///
/// Equivalent to `flat_map(s, fn(x) { x })`; the close ordering is the
/// same.
pub fn flatten(streams: Stream(Stream(a))) -> Stream(a) {
  flat_map(over: streams, with: fn(inner) { inner })
}

/// Walk a `List(Stream(a))` in list order, yielding every element of
/// every stream.
///
/// Each inner stream is closed (by reaching its own `Done`) before the
/// next one is opened. On early exit, the active stream is closed.
pub fn concat(streams: List(Stream(a))) -> Stream(a) {
  case streams {
    [] -> empty_stream()
    [first, ..rest] -> concat_active(first, rest)
  }
}

fn concat_active(active: Stream(a), remaining: List(Stream(a))) -> Stream(a) {
  datastream.make(pull: fn() { concat_pull(active, remaining) }, close: fn() {
    datastream.close(active)
  })
}

fn concat_pull(
  active: Stream(a),
  remaining: List(Stream(a)),
) -> Step(a, Stream(a)) {
  case datastream.pull(active) {
    Next(element, rest) -> Next(element, concat_active(rest, remaining))
    Done ->
      case remaining {
        [] -> Done
        [next, ..rest] -> concat_pull(next, rest)
      }
  }
}

fn empty_stream() -> Stream(a) {
  datastream.make(pull: fn() { Done }, close: noop)
}

/// Left-fold the stream, emitting one output per input.
///
/// The seed itself is NOT emitted, so output cardinality equals input
/// cardinality. On empty input the output is empty.
pub fn scan(
  over stream: Stream(a),
  from initial: b,
  with step: fn(b, a) -> b,
) -> Stream(b) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) -> {
          let new_acc = step(initial, element)
          Next(new_acc, scan(over: rest, from: new_acc, with: step))
        }
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Thread a state through the stream while emitting elements of a
/// possibly different type.
pub fn map_accum(
  over stream: Stream(a),
  from initial: state,
  with step: fn(state, a) -> #(state, b),
) -> Stream(b) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) -> {
          let #(new_acc, output) = step(initial, element)
          Next(output, map_accum(over: rest, from: new_acc, with: step))
        }
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Pair-wise zip two streams; halts the moment either source halts.
///
/// Both upstreams may be open at once. On halt, closes `right` first,
/// then `left`, matching the spec.
pub fn zip(left: Stream(a), right: Stream(b)) -> Stream(#(a, b)) {
  zip_with(left, right, with: fn(l, r) { #(l, r) })
}

/// Combine two streams element-wise with `combiner`; halts the moment
/// either source halts.
///
/// Same close ordering as `zip`: right then left.
pub fn zip_with(
  left: Stream(a),
  right: Stream(b),
  with combiner: fn(a, b) -> c,
) -> Stream(c) {
  datastream.make(pull: fn() { zip_pull(left, right, combiner) }, close: fn() {
    datastream.close(right)
    datastream.close(left)
  })
}

fn zip_pull(
  left: Stream(a),
  right: Stream(b),
  combiner: fn(a, b) -> c,
) -> Step(c, Stream(c)) {
  case datastream.pull(left) {
    Done -> {
      datastream.close(right)
      Done
    }
    Next(left_element, left_rest) ->
      case datastream.pull(right) {
        Done -> {
          datastream.close(left_rest)
          Done
        }
        Next(right_element, right_rest) ->
          Next(
            combiner(left_element, right_element),
            zip_with(left_rest, right_rest, with: combiner),
          )
      }
  }
}

/// Insert `separator` between adjacent elements.
///
/// Empty and single-element streams are unchanged.
pub fn intersperse(over stream: Stream(a), with separator: a) -> Stream(a) {
  intersperse_initial(stream, separator)
}

fn intersperse_initial(stream: Stream(a), separator: a) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Done -> Done
        Next(first, rest) ->
          Next(first, intersperse_awaiting_sep(rest, separator))
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

fn intersperse_awaiting_sep(stream: Stream(a), separator: a) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Done -> Done
        Next(next, rest) ->
          Next(separator, intersperse_pending(next, rest, separator))
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

fn intersperse_pending(pending: a, stream: Stream(a), separator: a) -> Stream(a) {
  datastream.make(
    pull: fn() { Next(pending, intersperse_awaiting_sep(stream, separator)) },
    close: fn() { datastream.close(stream) },
  )
}

/// Call `effect` once per element and re-emit the element unchanged.
pub fn tap(over stream: Stream(a), with effect: fn(a) -> Nil) -> Stream(a) {
  datastream.make(
    pull: fn() {
      case datastream.pull(stream) {
        Next(element, rest) -> {
          effect(element)
          Next(element, tap(over: rest, with: effect))
        }
        Done -> Done
      }
    },
    close: fn() { datastream.close(stream) },
  )
}

/// Collapse runs of `==`-equal adjacent values to a single occurrence.
pub fn dedupe_adjacent(stream: Stream(a)) -> Stream(a) {
  dedupe_active(stream, None)
}

fn dedupe_active(stream: Stream(a), last: Option(a)) -> Stream(a) {
  datastream.make(pull: fn() { dedupe_pull(stream, last) }, close: fn() {
    datastream.close(stream)
  })
}

fn dedupe_pull(stream: Stream(a), last: Option(a)) -> Step(a, Stream(a)) {
  case datastream.pull(stream) {
    Done -> Done
    Next(element, rest) ->
      case last {
        Some(prev) if prev == element -> dedupe_pull(rest, Some(prev))
        _ -> Next(element, dedupe_active(rest, Some(element)))
      }
  }
}

// --- chunked operations -----------------------------------------------------

/// Group adjacent elements into fixed-size chunks.
///
/// `size < 1` is normalised to `1`. The trailing chunk may be smaller
/// than `size` when the source length is not divisible.
pub fn chunks_of(over stream: Stream(a), into size: Int) -> Stream(Chunk(a)) {
  let normalised = case size < 1 {
    True -> 1
    False -> size
  }
  chunks_active(stream, [], 0, normalised)
}

fn chunks_active(
  source: Stream(a),
  buffer: List(a),
  count: Int,
  size: Int,
) -> Stream(Chunk(a)) {
  datastream.make(
    pull: fn() { chunks_pull(source, buffer, count, size) },
    close: fn() { datastream.close(source) },
  )
}

fn chunks_pull(
  source: Stream(a),
  buffer: List(a),
  count: Int,
  size: Int,
) -> Step(Chunk(a), Stream(Chunk(a))) {
  case datastream.pull(source) {
    Done ->
      case buffer {
        [] -> Done
        _ -> Next(chunk.from_list(list.reverse(buffer)), empty_stream())
      }
    Next(element, rest) -> {
      let new_count = count + 1
      let new_buffer = [element, ..buffer]
      case new_count >= size {
        True ->
          Next(
            chunk.from_list(list.reverse(new_buffer)),
            chunks_active(rest, [], 0, size),
          )
        False -> chunks_pull(rest, new_buffer, new_count, size)
      }
    }
  }
}

/// Group consecutive elements that share `key(element)`.
pub fn group_adjacent(
  over stream: Stream(a),
  by key: fn(a) -> k,
) -> Stream(#(k, Chunk(a))) {
  group_active(stream, None, [], key)
}

fn group_active(
  source: Stream(a),
  current_key: Option(k),
  buffer: List(a),
  key: fn(a) -> k,
) -> Stream(#(k, Chunk(a))) {
  datastream.make(
    pull: fn() { group_pull(source, current_key, buffer, key) },
    close: fn() { datastream.close(source) },
  )
}

fn group_pull(
  source: Stream(a),
  current_key: Option(k),
  buffer: List(a),
  key: fn(a) -> k,
) -> Step(#(k, Chunk(a)), Stream(#(k, Chunk(a)))) {
  case datastream.pull(source) {
    Done -> group_flush(current_key, buffer)
    Next(element, rest) ->
      group_advance(current_key, buffer, element, rest, key)
  }
}

fn group_flush(
  current_key: Option(k),
  buffer: List(a),
) -> Step(#(k, Chunk(a)), Stream(#(k, Chunk(a)))) {
  case current_key {
    None -> Done
    Some(k) -> Next(#(k, chunk.from_list(list.reverse(buffer))), empty_stream())
  }
}

fn group_advance(
  current_key: Option(k),
  buffer: List(a),
  element: a,
  rest: Stream(a),
  key: fn(a) -> k,
) -> Step(#(k, Chunk(a)), Stream(#(k, Chunk(a)))) {
  let element_key = key(element)
  case current_key {
    None -> group_pull(rest, Some(element_key), [element], key)
    Some(prev_key) if prev_key == element_key ->
      group_pull(rest, Some(element_key), [element, ..buffer], key)
    Some(prev_key) ->
      Next(
        #(prev_key, chunk.from_list(list.reverse(buffer))),
        group_active(rest, Some(element_key), [element], key),
      )
  }
}
