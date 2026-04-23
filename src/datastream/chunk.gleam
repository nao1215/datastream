//// `Chunk(a)` is an opaque, finite, immutable batch of elements.
////
//// Chunks are the element type of stream operations that produce
//// batches (`stream.chunks_of`, `stream.group_adjacent`,
//// `erlang/time.window_time`, …). The type is intentionally opaque so
//// the internal representation can change later (for example a
//// contiguous byte buffer for `Chunk(Int)`) without breaking callers.
////
//// The accessor surface is intentionally narrow: `to_list`, `size`,
//// `is_empty`. Anything else is expressed by going through `to_list`,
//// keeping `Chunk` from re-mirroring the entire list API.
////
//// Chunks are finite by design. Infinite-ness is the responsibility
//// of `Stream`, not of a self-contained value.

import gleam/list

/// A finite, immutable batch of `a` values.
///
/// The internal representation is hidden so the library is free to
/// change it (currently a list, possibly a contiguous buffer later)
/// without a breaking change. Observational equality goes through
/// `to_list`.
pub opaque type Chunk(a) {
  Chunk(elements: List(a))
}

/// The zero-element chunk.
pub fn empty() -> Chunk(a) {
  Chunk([])
}

/// The one-element chunk containing `value`.
pub fn singleton(value: a) -> Chunk(a) {
  Chunk([value])
}

/// Build a chunk from a list of elements.
///
/// Round-trips with `to_list`: `to_list(from_list(xs)) == xs`.
pub fn from_list(elements: List(a)) -> Chunk(a) {
  Chunk(elements)
}

/// Materialise a chunk back into a list, preserving order.
pub fn to_list(chunk: Chunk(a)) -> List(a) {
  chunk.elements
}

/// Number of elements in `chunk`.
pub fn size(chunk: Chunk(a)) -> Int {
  list.length(chunk.elements)
}

/// `True` iff `chunk` carries zero elements.
pub fn is_empty(chunk: Chunk(a)) -> Bool {
  case chunk.elements {
    [] -> True
    _ -> False
  }
}

/// Apply `f` to every element. The result has the same size as the
/// input; order is preserved.
pub fn map(over chunk: Chunk(a), with f: fn(a) -> b) -> Chunk(b) {
  Chunk(chunk.elements |> list.map(f))
}

/// Keep only the elements for which `predicate` returns `True`. Order
/// is preserved.
pub fn filter(
  over chunk: Chunk(a),
  keeping predicate: fn(a) -> Bool,
) -> Chunk(a) {
  Chunk(chunk.elements |> list.filter(predicate))
}

/// Concatenate chunks in list order.
pub fn concat(chunks: List(Chunk(a))) -> Chunk(a) {
  Chunk(chunks |> list.map(fn(c) { c.elements }) |> list.flatten)
}
