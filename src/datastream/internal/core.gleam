//// Small building blocks shared by the combinator modules.

import datastream.{type Stream, Done}
import gleam/list

/// A stream that is already finished and holds nothing to close.
pub fn empty() -> Stream(a) {
  datastream.make(pull: fn() { Done }, close: fn() { Nil })
}

/// Close `source` unless it already returned `Done`, in which case it
/// closed itself and must not be closed again.
pub fn close_unless_drained(source: Stream(a), drained drained: Bool) -> Nil {
  case drained {
    True -> Nil
    False -> datastream.close(source)
  }
}

/// Split newest-first `chunks` into the newest ones that together hold
/// at least `wanted` bytes, as measured by `size`, and the older rest.
/// Used by the delimiter searches to keep only the input a delimiter
/// could still start in.
pub fn keep_newest(
  chunks: List(chunk),
  holding wanted: Int,
  measured_by size: fn(chunk) -> Int,
) -> #(List(chunk), List(chunk)) {
  do_keep_newest(chunks, wanted, size, [])
}

fn do_keep_newest(
  chunks: List(chunk),
  wanted: Int,
  size: fn(chunk) -> Int,
  kept_rev: List(chunk),
) -> #(List(chunk), List(chunk)) {
  case chunks, wanted > 0 {
    [chunk, ..older], True ->
      do_keep_newest(older, wanted - size(chunk), size, [chunk, ..kept_rev])
    _, _ -> #(list.reverse(kept_rev), chunks)
  }
}
