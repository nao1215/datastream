//// `datastream` is a compositional, resource-safe stream library for
//// Gleam. The cross-target core (`datastream/source`, `datastream/stream`,
//// `datastream/fold`, `datastream/sink`, `datastream/chunk`,
//// `datastream/text`, `datastream/binary`, `datastream/dataprep`) runs on
//// both the Erlang and JavaScript targets. The `datastream/erlang/*`
//// extension modules are quarantined to the Erlang target.
////
//// See `doc/reference/spec.md` for the full specification.

/// Library version. Replaced by the actual implementation surface as
/// the Phase 1 issues land (see GitHub issue #2).
pub fn version() -> String {
  "0.1.0"
}
