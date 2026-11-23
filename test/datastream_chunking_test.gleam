//// Metamorphic relations for the chunk-boundary-aware parsers in
//// `datastream/text` and `datastream/binary`.
////
//// The exact output of `datastream.lines` or `datastream.delimited` for a random
//// input is not something a test can write down without re-implementing
//// the parser, but one relation must always hold: the same input cut
//// into chunks at different places yields the same elements. Real reads
//// split text and bytes at arbitrary offsets, so every relation below
//// compares the one-chunk run with a run over a random re-chunking (and
//// one with empty chunks mixed in).

import datastream
import gleeunit/should

import gleam/bit_array
import gleam/list
import gleam/string
import metamon
import metamon/generator.{type Generator}
import metamon/generator/range

// ---------- helpers ----------

/// Group `items` into consecutive runs whose lengths cycle through
/// `sizes` (each size is at least 1). The concatenation of the runs is
/// always `items`.
fn group(items: List(a), sizes: List(Int)) -> List(List(a)) {
  case sizes {
    [] -> [items]
    _ -> group_loop(items, sizes, sizes, [])
  }
}

fn group_loop(
  items: List(a),
  sizes: List(Int),
  all_sizes: List(Int),
  acc: List(List(a)),
) -> List(List(a)) {
  case items, sizes {
    [], _ -> list.reverse(acc)
    _, [] -> group_loop(items, all_sizes, all_sizes, acc)
    _, [size, ..rest] -> {
      let #(head, tail) = list.split(items, size)
      group_loop(tail, rest, all_sizes, [head, ..acc])
    }
  }
}

fn sizes_generator() -> Generator(List(Int)) {
  generator.list_of(generator.int(range.constant(1, 5)), range.constant(0, 6))
}

/// Text built from pieces that stress the boundaries the parsers care
/// about: every line terminator, the delimiters used below, a
/// multi-byte codepoint, an emoji and a combining accent that forms a
/// grapheme with the preceding `e`.
fn text_pieces_generator() -> Generator(List(String)) {
  generator.list_of(
    generator.element_of([
      "a", "b", "e", "\n", "\r", "\r\n", ",", ";", "é", "日", "😀", "\u{0301}",
    ]),
    range.constant(0, 24),
  )
}

/// The same pieces without the combining accent, for comparing with
/// `string.split`. On Erlang `string.split` matches whole graphemes, so
/// it does not split `e` followed by a combining mark, while
/// `datastream.split` matches code points on both targets.
/// `text_chunking_test.gleam` compares that case with a code point
/// oracle.
fn text_pieces_without_combining_generator() -> Generator(List(String)) {
  generator.list_of(
    generator.element_of([
      "a", "b", "e", "\n", "\r", "\r\n", ",", ";", "é", "日", "😀",
    ]),
    range.constant(0, 24),
  )
}

/// The one-chunk run and a re-chunked run of the same text. The cuts
/// fall between pieces, so a `\r\n` piece stays whole while a `\r`
/// piece followed by a `\n` piece is split across chunks.
fn text_chunkings(
  pieces: List(String),
  sizes: List(Int),
) -> #(List(String), List(String)) {
  let whole = [string.concat(pieces)]
  let chunks = group(pieces, sizes) |> list.map(string.concat)
  #(whole, chunks)
}

/// Insert an empty chunk before every chunk.
fn with_empty_chunks(chunks: List(a), empty: a) -> List(a) {
  list.flat_map(chunks, fn(chunk) { [empty, chunk] })
}

fn bytes_generator() -> Generator(BitArray) {
  generator.bit_array(range.constant(0, 40))
}

/// Bytes drawn from a small alphabet so that delimiters and short
/// length prefixes appear often.
fn framed_bytes_generator() -> Generator(BitArray) {
  generator.list_of(
    generator.element_of([0, 1, 2, 3, 10, 13, 255]),
    range.constant(0, 40),
  )
  |> generator.map(fn(values) {
    list.fold(values, <<>>, fn(acc, value) { <<acc:bits, value:8>> })
  })
}

fn byte_list(bytes: BitArray) -> List(BitArray) {
  case bytes {
    <<byte:8, rest:bits>> -> [<<byte:8>>, ..byte_list(rest)]
    _ -> []
  }
}

fn byte_chunkings(
  bytes: BitArray,
  sizes: List(Int),
) -> #(List(BitArray), List(BitArray)) {
  let chunks = group(byte_list(bytes), sizes) |> list.map(bit_array.concat)
  #([bytes], chunks)
}

fn text_relation(run: fn(List(String)) -> b) -> Nil {
  text_relation_over(text_pieces_generator(), run)
}

fn text_relation_over(
  pieces_gen: Generator(List(String)),
  run: fn(List(String)) -> b,
) -> Nil {
  metamon.forall(generator.tuple2(pieces_gen, sizes_generator()), fn(input) {
    let #(pieces, sizes) = input
    let #(whole, chunks) = text_chunkings(pieces, sizes)
    let expected = run(whole)
    // An empty input is left out: an empty stream and a stream of one
    // empty string are different inputs to `split`.
    pieces == []
    || run(chunks) == expected
    && run(with_empty_chunks(chunks, "")) == expected
  })
}

fn byte_relation(
  bytes_gen: Generator(BitArray),
  run: fn(List(BitArray)) -> b,
) -> Nil {
  metamon.forall(generator.tuple2(bytes_gen, sizes_generator()), fn(input) {
    let #(bytes, sizes) = input
    let #(whole, chunks) = byte_chunkings(bytes, sizes)
    let expected = run(whole)
    bytes == <<>>
    || run(chunks) == expected
    && run(with_empty_chunks(chunks, <<>>)) == expected
  })
}

// ---------- text ----------

pub fn lines_is_invariant_under_rechunking_test() -> Nil {
  text_relation(fn(chunks) {
    datastream.to_list(datastream.lines(over: datastream.from_list(chunks)))
  })
}

pub fn records_is_invariant_under_rechunking_test() -> Nil {
  text_relation(fn(chunks) {
    datastream.to_list(
      datastream.records(
        over: datastream.lines(over: datastream.from_list(chunks)),
      ),
    )
  })
}

pub fn split_on_single_char_is_invariant_under_rechunking_test() -> Nil {
  text_relation_over(text_pieces_generator(), fn(chunks) {
    datastream.to_list(datastream.split(
      over: datastream.from_list(chunks),
      on: ",",
    ))
  })
}

pub fn split_on_multi_char_is_invariant_under_rechunking_test() -> Nil {
  text_relation_over(text_pieces_generator(), fn(chunks) {
    datastream.to_list(datastream.split(
      over: datastream.from_list(chunks),
      on: ",;",
    ))
  })
}

pub fn split_into_graphemes_is_invariant_under_rechunking_test() -> Nil {
  text_relation(fn(chunks) {
    datastream.to_list(datastream.split(
      over: datastream.from_list(chunks),
      on: "",
    ))
  })
}

pub fn split_into_graphemes_matches_string_to_graphemes_test() -> Nil {
  metamon.forall(text_pieces_generator(), fn(pieces) {
    let whole = string.concat(pieces)
    datastream.to_list(datastream.split(
      over: datastream.from_list([whole]),
      on: "",
    ))
    == string.to_graphemes(whole)
  })
}

pub fn split_on_delimiter_matches_string_split_test() -> Nil {
  metamon.forall(text_pieces_without_combining_generator(), fn(pieces) {
    let whole = string.concat(pieces)
    datastream.to_list(datastream.split(
      over: datastream.from_list([whole]),
      on: ",",
    ))
    == string.split(whole, ",")
  })
}

/// `utf8_decode` emits each run of valid bytes it has buffered as one
/// `Ok`, so the element boundaries follow the reads. The decoded text
/// and the number of errors must not.
pub fn utf8_decode_is_invariant_under_rechunking_test() -> Nil {
  byte_relation(bytes_generator(), fn(chunks) {
    let #(oks, errors) =
      datastream.partition_result(
        datastream.utf8_decode(over: datastream.from_list(chunks)),
      )
    #(string.concat(oks), list.length(errors))
  })
}

pub fn utf8_decode_of_valid_text_is_invariant_under_rechunking_test() -> Nil {
  byte_relation(generator.bit_array_utf8(range.constant(0, 16)), fn(chunks) {
    datastream.to_string(
      datastream.utf8_decode_lossy(over: datastream.from_list(chunks)),
    )
  })
}

pub fn utf8_encode_then_decode_round_trips_test() -> Nil {
  metamon.forall(
    generator.tuple2(text_pieces_generator(), sizes_generator()),
    fn(input) {
      let #(pieces, sizes) = input
      let #(_, chunks) = text_chunkings(pieces, sizes)
      datastream.from_list(chunks)
      |> datastream.utf8_encode
      |> datastream.utf8_decode_lossy
      |> datastream.to_string
      == string.concat(pieces)
    },
  )
}

// ---------- binary ----------

pub fn bytes_is_invariant_under_rechunking_test() -> Nil {
  byte_relation(bytes_generator(), fn(chunks) {
    datastream.to_list(datastream.bytes(over: datastream.from_list(chunks)))
  })
}

pub fn delimited_single_byte_is_invariant_under_rechunking_test() -> Nil {
  byte_relation(framed_bytes_generator(), fn(chunks) {
    datastream.to_list(
      datastream.delimited(over: datastream.from_list(chunks), on: <<10>>)
      |> should.be_ok,
    )
  })
}

pub fn delimited_multi_byte_is_invariant_under_rechunking_test() -> Nil {
  byte_relation(framed_bytes_generator(), fn(chunks) {
    datastream.to_list(
      datastream.delimited(over: datastream.from_list(chunks), on: <<13, 10>>)
      |> should.be_ok,
    )
  })
}

pub fn fixed_size_is_invariant_under_rechunking_test() -> Nil {
  byte_relation(framed_bytes_generator(), fn(chunks) {
    datastream.to_list(
      datastream.fixed_size(over: datastream.from_list(chunks), size: 3)
      |> should.be_ok,
    )
  })
}

pub fn length_prefixed_is_invariant_under_rechunking_test() -> Nil {
  byte_relation(framed_bytes_generator(), fn(chunks) {
    datastream.to_list(
      datastream.length_prefixed(
        over: datastream.from_list(chunks),
        prefix_size: 1,
        max_frame_size: 16_777_216,
      )
      |> should.be_ok,
    )
  })
}

pub fn length_prefixed_two_byte_prefix_is_invariant_under_rechunking_test() -> Nil {
  byte_relation(framed_bytes_generator(), fn(chunks) {
    datastream.to_list(
      datastream.length_prefixed(
        over: datastream.from_list(chunks),
        prefix_size: 2,
        max_frame_size: 16_777_216,
      )
      |> should.be_ok,
    )
  })
}
