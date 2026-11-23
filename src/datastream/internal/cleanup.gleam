//// Cleanup that still runs when a callback raises.
////
//// A `Cell` holds the cleanup of one open resource or worker group.
//// `guard` runs a terminal operation in a frame; a cell created, set,
//// or touched while that frame is innermost is registered in it. If
//// the operation raises, the frame runs every cell still registered,
//// newest first, discards exceptions from those cleanups, and lets the
//// original exception continue. A cell runs at most once.
////
//// On a normal return, cells that are still open move to the
//// enclosing frame. `guard_in` uses a frame that outlives the call, for
//// state shared between several terminal operations.
////
//// On Erlang the state lives in the process dictionary of the process
//// that created it. Every run of a stream stays in the process that
//// opened it: workers pull their sources for the whole run, and shared
//// consumers must be pulled in their creating process. Taking a cell in
//// another process runs the cleanup passed by that caller instead.

/// A cleanup that runs at most once.
pub type Cell

/// A frame that keeps its cells between calls to `guard_in`.
pub type Frame

@external(erlang, "datastream_cleanup_ffi", "new_cell")
@external(javascript, "./cleanup_ffi.mjs", "new_cell")
@internal
pub fn new(cleanup: fn() -> Nil) -> Cell

/// Replace the cleanup and register the cell in the innermost frame.
@external(erlang, "datastream_cleanup_ffi", "set_cell")
@external(javascript, "./cleanup_ffi.mjs", "set_cell")
@internal
pub fn set(cell: Cell, cleanup: fn() -> Nil) -> Nil

/// Register the cell in the innermost frame without changing it.
@external(erlang, "datastream_cleanup_ffi", "touch_cell")
@external(javascript, "./cleanup_ffi.mjs", "touch_cell")
@internal
pub fn touch(cell: Cell) -> Nil

/// Run the cleanup unless it has already run or been discarded.
@external(erlang, "datastream_cleanup_ffi", "take_cell")
@external(javascript, "./cleanup_ffi.mjs", "take_cell")
@internal
pub fn take(cell: Cell, or fallback: fn() -> Nil) -> Nil

/// Forget the cleanup without running it.
@external(erlang, "datastream_cleanup_ffi", "discard_cell")
@external(javascript, "./cleanup_ffi.mjs", "discard_cell")
@internal
pub fn discard(cell: Cell) -> Nil

@external(erlang, "datastream_cleanup_ffi", "guard")
@external(javascript, "./cleanup_ffi.mjs", "guard")
@internal
pub fn guard(body: fn() -> a) -> a

@external(erlang, "datastream_cleanup_ffi", "new_frame")
@external(javascript, "./cleanup_ffi.mjs", "new_frame")
@internal
pub fn new_frame() -> Frame

@external(erlang, "datastream_cleanup_ffi", "guard_in")
@external(javascript, "./cleanup_ffi.mjs", "guard_in")
@internal
pub fn guard_in(frame: Frame, body: fn() -> a, on_raise: fn() -> Nil) -> a

/// Raise an exception with `message`, like `panic`.
@external(erlang, "datastream_cleanup_ffi", "raise")
@external(javascript, "./cleanup_ffi.mjs", "raise")
@internal
pub fn raise(message: String) -> a
