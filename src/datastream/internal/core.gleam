//// Small building blocks shared by the combinator modules.

import datastream.{type Stream, Done}

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
