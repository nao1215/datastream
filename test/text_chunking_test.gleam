//// `split` must not depend on the target or on where chunks are cut.

import datastream
import gleam/list
import gleam/string

fn next_seed(seed: Int) -> Int {
  { seed * 48_271 } % 2_147_483_647
}

fn pick(seed: Int, below: Int) -> #(Int, Int) {
  let seed = next_seed(seed)
  #(seed, { seed / 65_536 } % below)
}

fn pick_from(seed: Int, items: List(String)) -> #(Int, String) {
  let #(seed, index) = pick(seed, list.length(items))
  case list.drop(items, index) {
    [item, ..] -> #(seed, item)
    [] -> #(seed, "")
  }
}

fn random_pieces(
  seed: Int,
  alphabet: List(String),
  count: Int,
  pieces: List(String),
) -> #(Int, List(String)) {
  case count {
    0 -> #(seed, pieces)
    _ -> {
      let #(seed, piece) = pick_from(seed, alphabet)
      random_pieces(seed, alphabet, count - 1, [piece, ..pieces])
    }
  }
}

/// Join random runs of `pieces` into chunks.
fn random_chunks(
  seed: Int,
  pieces: List(String),
  chunks: List(String),
) -> #(Int, List(String)) {
  case pieces {
    [] -> #(seed, list.reverse(chunks))
    _ -> {
      let #(seed, size) = pick(seed, 4)
      let #(chunk, rest) = list.split(pieces, size + 1)
      random_chunks(seed, rest, [string.concat(chunk), ..chunks])
    }
  }
}

/// Split by code points, the documented matching rule.
fn split_by_codepoints(text: String, delimiter: String) -> List(String) {
  let delimiter = string.to_utf_codepoints(delimiter)
  do_split(string.to_utf_codepoints(text), delimiter, [], [])
}

fn do_split(text, delimiter, current, pieces) -> List(String) {
  case text, starts_with(text, delimiter) {
    _, True ->
      do_split(list.drop(text, list.length(delimiter)), delimiter, [], [
        string.from_utf_codepoints(list.reverse(current)),
        ..pieces
      ])
    [], False ->
      list.reverse([string.from_utf_codepoints(list.reverse(current)), ..pieces])
    [codepoint, ..rest], False ->
      do_split(rest, delimiter, [codepoint, ..current], pieces)
  }
}

fn starts_with(text, prefix) -> Bool {
  case text, prefix {
    _, [] -> True
    [x, ..text], [y, ..prefix] if x == y -> starts_with(text, prefix)
    _, _ -> False
  }
}

pub fn split_matches_code_points_inside_grapheme_clusters_test() {
  let split = fn(chunks, delimiter) {
    datastream.from_list(chunks)
    |> datastream.split(on: delimiter)
    |> datastream.to_list
  }
  assert split(["a\r\nb"], "\r") == ["a", "\nb"]
  assert split(["a\r", "\nb"], "\r") == ["a", "\nb"]
  assert split(["e\u{301}x"], "e") == ["", "\u{301}x"]
  assert split(["e", "\u{301}x"], "e") == ["", "\u{301}x"]
  assert split(["\u{1F1EF}\u{1F1F5}"], "\u{1F1F5}") == ["\u{1F1EF}", ""]
}

pub fn split_is_independent_of_chunk_boundaries_test() {
  let alphabet = [
    "a", "b", ",", "\r", "\n", "\u{301}", "é", "e", "\u{1F1EF}", "\u{1F1F5}",
    "\u{1F44D}", "\u{200D}", ",,", "\r\n",
  ]
  let delimiters = [",", "\r", "e", ",,", "\u{301}", "\r\n", "\u{200D}"]
  datastream.range(from: 1, to: 501)
  |> datastream.each(fn(case_id) {
    let #(seed, pieces) = random_pieces(case_id * 7919, alphabet, 12, [])
    let #(seed, chunks) = random_chunks(seed, pieces, [])
    let #(_, delimiter) = pick_from(seed, delimiters)
    let expected = split_by_codepoints(string.concat(chunks), delimiter)
    let chunked =
      datastream.from_list(chunks)
      |> datastream.split(on: delimiter)
      |> datastream.to_list
    let whole =
      datastream.once(string.concat(chunks))
      |> datastream.split(on: delimiter)
      |> datastream.to_list
    assert chunked == expected
    assert whole == expected
  })
}

pub fn grapheme_split_is_independent_of_chunk_boundaries_test() {
  // Pieces whose grapheme boundaries depend on earlier code points:
  // emoji ZWJ sequences, regional indicator pairs, Hangul jamo, CRLF,
  // combining and spacing marks, and a prepended concatenation mark.
  let alphabet = [
    "a", "\u{301}", "\u{200D}", "\u{1F469}", "\u{1F466}", "\u{1F1EF}",
    "\u{1F1F5}", "\u{1F3FD}", "\u{1100}", "\u{1161}", "\u{11A8}", "\r", "\n",
    "\u{0915}", "\u{094D}", "\u{093F}", "\u{0600}", "\u{FE0F}", " ",
  ]
  datastream.range(from: 1, to: 501)
  |> datastream.each(fn(case_id) {
    let #(seed, pieces) = random_pieces(case_id * 104_729, alphabet, 40, [])
    let #(_, chunks) = random_chunks(seed, pieces, [])
    let expected = string.to_graphemes(string.concat(chunks))
    let one_code_point_per_chunk =
      datastream.from_list(pieces)
      |> datastream.split(on: "")
      |> datastream.to_list
    let chunked =
      datastream.from_list(chunks)
      |> datastream.split(on: "")
      |> datastream.to_list
    assert chunked == expected
    assert one_code_point_per_chunk
      == string.to_graphemes(string.concat(pieces))
  })
}

pub fn long_grapheme_over_many_chunks_is_linear_test() {
  // One grapheme of 30,001 code points, delivered one code point per
  // chunk, then a second grapheme. Resegmenting the whole unfinished
  // grapheme for each chunk took tens of seconds on Erlang.
  let marks = list.repeat("\u{301}", 30_000)
  let graphemes =
    datastream.from_list(["e", ..list.append(marks, ["x"])])
    |> datastream.split(on: "")
    |> datastream.to_list
  assert list.map(graphemes, string.length) == [1, 1]
  assert graphemes == ["e" <> string.concat(marks), "x"]
}

pub fn long_zwj_sequence_over_many_chunks_is_linear_test() {
  // Each joiner decides whether the next pictograph joins the cluster
  // only by looking back past the marks before it.
  let unit = ["\u{200D}", "\u{1F469}", ..list.repeat("\u{301}", 40)]
  let pieces = list.flatten(list.repeat(unit, 300))
  let whole = string.concat(["\u{1F469}", ..pieces])
  let graphemes =
    datastream.from_list(["\u{1F469}", ..pieces])
    |> datastream.split(on: "")
    |> datastream.to_list
  assert graphemes == string.to_graphemes(whole)
}
