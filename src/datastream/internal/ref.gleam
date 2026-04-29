//// Cross-target mutable reference cell, internal to the library.
////
//// Almost every combinator in `datastream/stream` is pure and needs no
//// shared state. The exceptions are `broadcast` and `unzip`, where two
//// or more consumer streams must share access to a single upstream and
//// per-consumer queues — and where pulling from one consumer must
//// observably advance the state seen by the others. Pure-functional
//// pull-state cannot express that observation across separate stream
//// values, so we route the shared bits through a tiny `Ref(a)`.
////
//// `Ref` is **not** part of the public API — `datastream` stays a
//// pull-based stream library, not a general-purpose mutable-state
//// library. The `@internal` annotation hides every export from the
//// generated docs, and `internal_modules` in `gleam.toml` prevents
//// downstream packages from importing this module directly.
////
//// On Erlang the cell is keyed off an `:erlang.unique_integer/1`
//// process-dictionary slot, which keeps the cost flat and confines
//// state to the calling process (no extra spawned processes,
//// no `:ets` tables, no atomics). On JavaScript it is a one-field
//// mutable object. Both implementations are call-site-local; a `Ref`
//// outlives only the surrounding pipeline.

pub opaque type Ref(a) {
  Ref(handle: Handle(a))
}

/// Phantom-typed handle to the FFI-side mutable cell. The `a`
/// parameter is purely advisory — neither the BEAM process dict nor
/// the JavaScript object enforces it — but it lets the public surface
/// keep `Ref(a)` typed.
type Handle(a)

@external(erlang, "datastream_ref_ffi", "new_ref")
@external(javascript, "./ref_ffi.mjs", "new_ref")
fn do_new(value: a) -> Handle(a)

@external(erlang, "datastream_ref_ffi", "get_ref")
@external(javascript, "./ref_ffi.mjs", "get_ref")
fn do_get(handle: Handle(a)) -> a

@external(erlang, "datastream_ref_ffi", "set_ref")
@external(javascript, "./ref_ffi.mjs", "set_ref")
fn do_set(handle: Handle(a), value: a) -> Nil

/// Allocate a new mutable cell holding `value`.
@internal
pub fn new(value: a) -> Ref(a) {
  Ref(handle: do_new(value))
}

/// Read the current value of `ref`.
@internal
pub fn get(ref: Ref(a)) -> a {
  let Ref(handle) = ref
  do_get(handle)
}

/// Overwrite `ref` with `value`. Returns `Nil`.
@internal
pub fn set(ref: Ref(a), value: a) -> Nil {
  let Ref(handle) = ref
  do_set(handle, value)
}
