import datastream.{Done, Next}
import gleam/list
import gleam/string
import gleeunit
import gleeunit/should

pub fn main() -> Nil {
  gleeunit.main()
}

fn from_list(list) {
  datastream.unfold(from: list, with: fn(xs) {
    case xs {
      [] -> Done
      [head, ..tail] -> Next(element: head, state: tail)
    }
  })
}

// --- lines -----------------------------------------------------------------

pub fn lines_terminated_by_lf_test() {
  from_list(["hello\nworld\n"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["hello", "world"])
}

pub fn lines_partial_trailing_line_emitted_test() {
  from_list(["hello\nworld"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["hello", "world"])
}

pub fn lines_empty_chunk_yields_no_lines_test() {
  from_list([""]) |> datastream.lines |> datastream.to_list |> should.equal([])
}

pub fn lines_two_consecutive_terminators_yield_two_empty_lines_test() {
  from_list(["\n\n"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["", ""])
}

pub fn lines_crlf_terminator_test() {
  from_list(["a\r\nb\r\n"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["a", "b"])
}

pub fn lines_split_across_chunks_test() {
  from_list(["hel", "lo\nwor", "ld"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["hello", "world"])
}

pub fn lines_crlf_split_across_chunks_test() {
  from_list(["a\r", "\nb"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["a", "b"])
}

pub fn lines_lone_cr_at_chunk_end_treated_as_terminator_test() {
  from_list(["a\r", "b"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["a", "b"])
}

pub fn lines_on_truly_empty_source_yields_no_lines_test() {
  from_list([]) |> datastream.lines |> datastream.to_list |> should.equal([])
}

pub fn lines_cr_at_chunk_end_followed_by_empty_chunks_test() {
  // Issue #157: a CR at end of a chunk followed by empty-string chunks
  // must not loop forever. The empty chunks should be skipped and the
  // CR treated as a line terminator when the source eventually drains.
  from_list(["hello\r", "", "", "world"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["hello", "world"])
}

pub fn lines_cr_then_empty_then_lf_test() {
  // CR at chunk end, empty chunk, then a chunk starting with LF.
  // The \r\n pair should be treated as one terminator.
  from_list(["hello\r", "", "\nworld"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["hello", "world"])
}

pub fn lines_mixed_terminators_inside_one_chunk_test() {
  from_list(["a\rb\r\nc\nd\r\re"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["a", "b", "c", "d", "", "e"])
}

pub fn lines_lone_cr_alone_yields_one_empty_line_test() {
  from_list(["\r"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal([""])
}

pub fn lines_cr_then_cr_lf_across_chunks_test() {
  from_list(["x\r", "\r", "\ny"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["x", "", "y"])
}

pub fn lines_partial_line_across_many_chunks_test() {
  from_list(["ab", "c", "d\re", "f"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["abcd", "ef"])
}

pub fn lines_trailing_lf_after_cr_chunk_test() {
  from_list(["a\r", "\n"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["a"])
}

pub fn lines_lf_after_consumed_cr_lf_is_a_new_line_test() {
  from_list(["a\r", "\n", "\n"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["a", ""])
}

pub fn lines_keeps_non_ascii_text_test() {
  from_list(["日本\r\n語", "🇯🇵\n"])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["日本", "語🇯🇵"])
}

pub fn lines_cr_at_end_followed_by_all_empty_chunks_test() {
  // CR at chunk end, all remaining chunks are empty, then source drains.
  // The CR should be treated as a terminator; "hello" is emitted.
  from_list(["hello\r", "", ""])
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["hello"])
}

// --- records ---------------------------------------------------------------

pub fn records_two_single_line_records_test() {
  from_list(["a", "", "b"])
  |> datastream.records
  |> datastream.to_list
  |> should.equal([["a"], ["b"]])
}

pub fn records_multiline_records_test() {
  from_list(["a", "b", "", "c"])
  |> datastream.records
  |> datastream.to_list
  |> should.equal([["a", "b"], ["c"]])
}

pub fn records_trailing_blank_does_not_emit_empty_record_test() {
  from_list(["a", "", "b", ""])
  |> datastream.records
  |> datastream.to_list
  |> should.equal([["a"], ["b"]])
}

pub fn records_leading_blank_is_ignored_test() {
  from_list(["", "a"])
  |> datastream.records
  |> datastream.to_list
  |> should.equal([["a"]])
}

pub fn records_consecutive_blanks_collapse_test() {
  from_list(["a", "", "", "b"])
  |> datastream.records
  |> datastream.to_list
  |> should.equal([["a"], ["b"]])
}

pub fn records_only_blanks_yields_no_records_test() {
  from_list(["", "", ""])
  |> datastream.records
  |> datastream.to_list
  |> should.equal([])
}

pub fn records_empty_source_yields_no_records_test() {
  from_list([]) |> datastream.records |> datastream.to_list |> should.equal([])
}

pub fn records_single_blank_yields_no_records_test() {
  from_list([""])
  |> datastream.records
  |> datastream.to_list
  |> should.equal([])
}

pub fn records_no_terminating_blank_emits_trailing_record_test() {
  from_list(["a", "b"])
  |> datastream.records
  |> datastream.to_list
  |> should.equal([["a", "b"]])
}

/// Issue #165 reproduction: SSE-style framing where each event is a
/// block of `field: value` lines separated from the next event by a
/// blank line. The previous user-supplied `string.split("\n\n")`
/// workaround required the entire payload buffered up front; with
/// `datastream.records` an SSE consumer composes with `datastream.lines` and
/// streams record-by-record.
pub fn records_sse_style_framing_test() {
  from_list([
    "event: heartbeat\n",
    "data: 1\n",
    "\n",
    "event: post\n",
    "data: hello\n",
    "data: world\n",
  ])
  |> datastream.lines
  |> datastream.records
  |> datastream.to_list
  |> should.equal([
    ["event: heartbeat", "data: 1"],
    ["event: post", "data: hello", "data: world"],
  ])
}

// --- split -----------------------------------------------------------------

pub fn split_on_comma_yields_pieces_test() {
  from_list(["a,b,c"])
  |> datastream.split(on: ",")
  |> datastream.to_list
  |> should.equal(["a", "b", "c"])
}

pub fn split_preserves_empty_between_consecutive_delimiters_test() {
  from_list(["a,,b"])
  |> datastream.split(on: ",")
  |> datastream.to_list
  |> should.equal(["a", "", "b"])
}

pub fn split_preserves_trailing_empty_test() {
  from_list(["a,b,"])
  |> datastream.split(on: ",")
  |> datastream.to_list
  |> should.equal(["a", "b", ""])
}

pub fn split_preserves_leading_empty_test() {
  from_list([",a,b"])
  |> datastream.split(on: ",")
  |> datastream.to_list
  |> should.equal(["", "a", "b"])
}

pub fn split_empty_input_yields_one_empty_test() {
  from_list([""])
  |> datastream.split(on: ",")
  |> datastream.to_list
  |> should.equal([""])
}

pub fn split_truly_empty_source_yields_empty_test() {
  from_list([])
  |> datastream.split(on: ",")
  |> datastream.to_list
  |> should.equal([])
}

pub fn split_delimiter_across_chunk_boundary_test() {
  from_list(["a,b", ",c"])
  |> datastream.split(on: ",")
  |> datastream.to_list
  |> should.equal(["a", "b", "c"])
}

pub fn split_non_delimiter_join_across_chunks_test() {
  from_list(["a,b", "c,d"])
  |> datastream.split(on: ",")
  |> datastream.to_list
  |> should.equal(["a", "bc", "d"])
}

pub fn split_empty_delimiter_emits_each_grapheme_test() {
  from_list(["abc"])
  |> datastream.split(on: "")
  |> datastream.to_list
  |> should.equal(["a", "b", "c"])
}

pub fn split_empty_delimiter_handles_multibyte_grapheme_test() {
  from_list(["a👍b"])
  |> datastream.split(on: "")
  |> datastream.to_list
  |> should.equal(["a", "👍", "b"])
}

pub fn split_empty_delimiter_joins_combining_mark_across_chunks_test() {
  // "e" followed by U+0301 COMBINING ACUTE ACCENT is one grapheme even
  // when the read boundary falls between them.
  from_list(["ae", "\u{0301}b"])
  |> datastream.split(on: "")
  |> datastream.to_list
  |> should.equal(["a", "e\u{0301}", "b"])
}

pub fn split_empty_delimiter_joins_crlf_across_chunks_test() {
  from_list(["a\r", "\nb"])
  |> datastream.split(on: "")
  |> datastream.to_list
  |> should.equal(["a", "\r\n", "b"])
}

pub fn split_empty_delimiter_joins_flag_across_three_chunks_test() {
  // A flag is two regional indicators; a lone indicator in the middle
  // chunk must wait for its pair.
  from_list(["a\u{1F1EF}", "\u{1F1F5}\u{1F1FA}", "\u{1F1F8}", ""])
  |> datastream.split(on: "")
  |> datastream.to_list
  |> should.equal(["a", "\u{1F1EF}\u{1F1F5}", "\u{1F1FA}\u{1F1F8}"])
}

pub fn split_empty_delimiter_matches_whole_input_for_every_cut_test() {
  let input = "x👍🏽e\u{0301}\r\n\u{1F1EF}\u{1F1F5}y"
  let codepoints =
    string.to_utf_codepoints(input)
    |> list.map(fn(cp) { string.from_utf_codepoints([cp]) })
  let expected = string.to_graphemes(input)
  datastream.range(from: 0, to: list.length(codepoints) + 1)
  |> datastream.to_list
  |> list.each(fn(cut) {
    let #(left, right) = list.split(codepoints, cut)
    from_list([string.concat(left), string.concat(right)])
    |> datastream.split(on: "")
    |> datastream.to_list
    |> should.equal(expected)
  })
}

// --- utf8_decode ----------------------------------------------------------

pub fn utf8_decode_simple_ascii_test() {
  from_list([<<104, 105>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("hi")])
}

pub fn utf8_decode_empty_chunk_yields_no_elements_test() {
  from_list([<<>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([])
}

pub fn utf8_decode_empty_source_yields_empty_test() {
  from_list([])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([])
}

pub fn utf8_decode_four_byte_codepoint_in_one_chunk_test() {
  from_list([<<240, 159, 145, 141>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("👍")])
}

pub fn utf8_decode_codepoint_split_across_chunks_2_2_test() {
  from_list([<<240, 159>>, <<145, 141>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("👍")])
}

pub fn utf8_decode_codepoint_split_across_chunks_3_1_test() {
  from_list([<<240, 159, 145>>, <<141>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("👍")])
}

pub fn utf8_decode_per_chunk_decode_test() {
  from_list([<<104>>, <<105>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("h"), Ok("i")])
}

pub fn utf8_decode_invalid_lead_byte_test() {
  from_list([<<255>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Error(Nil)])
}

pub fn utf8_decode_resumes_after_error_test() {
  from_list([<<104, 255, 105>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("h"), Error(Nil), Ok("i")])
}

pub fn utf8_decode_incomplete_trailing_sequence_at_eof_test() {
  from_list([<<240, 159>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Error(Nil)])
}

pub fn utf8_decode_rejects_overlong_two_byte_sequence_test() {
  // 0xC0 0x80 is an overlong encoding of U+0000 and must be rejected
  // (RFC 3629). Here 0xC0 is rejected at the lead-byte classification
  // step (lead < 0xC2), then 0x80 is a lone continuation byte and is
  // also rejected. Two consecutive Error(Nil) — the decoder does not
  // swallow the second byte silently.
  from_list([<<0xC0, 0x80>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Error(Nil), Error(Nil)])
}

pub fn utf8_decode_rejects_invalid_continuation_in_two_byte_sequence_test() {
  // 0xC2 is a valid 2-byte lead; 0x40 is NOT a valid continuation
  // (< 0x80). Decoder emits Error and resumes on 0x40 — which is
  // plain ASCII "@" — so the next output is Ok("@").
  from_list([<<0xC2, 0x40>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Error(Nil), Ok("@")])
}

pub fn utf8_decode_rejects_invalid_continuation_in_three_byte_sequence_test() {
  // 0xE0 is a valid 3-byte lead. 0xA0 is a valid first continuation
  // but 0x40 is not. The decoder must emit Error and resume from
  // 0x40, which is ASCII "@", then consume 0x61 ("a"). The two
  // ASCII bytes coalesce into a single Ok("@a").
  from_list([<<0xE0, 0xA0, 0x40, 0x61>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Error(Nil), Ok("@a")])
}

pub fn utf8_decode_rejects_consecutive_lone_continuation_bytes_test() {
  // Bytes in [0x80, 0xC2) are never valid as a lead byte. Two of
  // them in a row produce two separate Error(Nil) — the decoder
  // does not merge consecutive invalid bytes into a single error.
  from_list([<<0x80, 0x81>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Error(Nil), Error(Nil)])
}

pub fn utf8_decode_keeps_text_around_an_encoded_surrogate_test() {
  // 0xED 0xA0 0x80 has the shape of a 3-byte sequence but encodes the
  // surrogate U+D800, which UTF-8 forbids (RFC 3629): after 0xED the
  // next byte must be in 0x80..0x9F. Each of the three bytes is one
  // error and the text on both sides survives.
  from_list([<<"hello":utf8, 0xED, 0xA0, 0x80, "world":utf8>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("hello"), Error(Nil), Error(Nil), Error(Nil), Ok("world")])
}

pub fn utf8_decode_rejects_overlong_three_byte_sequence_test() {
  // 0xE0 0x80 0xAF is an overlong encoding of "/" (U+002F); after 0xE0
  // the next byte must be in 0xA0..0xBF.
  from_list([<<"a":utf8, 0xE0, 0x80, 0xAF, "b":utf8>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("a"), Error(Nil), Error(Nil), Error(Nil), Ok("b")])
}

pub fn utf8_decode_rejects_overlong_four_byte_sequence_test() {
  // After 0xF0 the next byte must be in 0x90..0xBF.
  from_list([<<"a":utf8, 0xF0, 0x8F, 0xBF, 0xBF, "b":utf8>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([
    Ok("a"),
    Error(Nil),
    Error(Nil),
    Error(Nil),
    Error(Nil),
    Ok("b"),
  ])
}

pub fn utf8_decode_rejects_codepoint_above_u10ffff_test() {
  // 0xF4 0x90 0x80 0x80 would be U+110000; after 0xF4 the next byte
  // must be in 0x80..0x8F.
  from_list([<<"a":utf8, 0xF4, 0x90, 0x80, 0x80, "b":utf8>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([
    Ok("a"),
    Error(Nil),
    Error(Nil),
    Error(Nil),
    Error(Nil),
    Ok("b"),
  ])
}

pub fn utf8_decode_accepts_the_edges_of_each_lead_range_test() {
  // U+0800, U+D7FF, U+E000, U+10000 and U+10FFFF are the first or last
  // code points next to each restricted second-byte range.
  from_list([
    <<0xE0, 0xA0, 0x80, 0xED, 0x9F, 0xBF, 0xEE, 0x80, 0x80>>,
    <<0xF0, 0x90, 0x80, 0x80, 0xF4, 0x8F, 0xBF, 0xBF>>,
  ])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([
    Ok("\u{0800}\u{D7FF}\u{E000}"),
    Ok("\u{10000}\u{10FFFF}"),
  ])
}

pub fn utf8_decode_accepts_the_smallest_two_byte_lead_test() {
  // 0xC2 0xA9 is "©"; 0xC2 is the first valid 2-byte lead.
  from_list([<<0xC2, 0xA9>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Ok("©")])
}

pub fn utf8_decode_rejects_0xf5_as_a_lead_byte_test() {
  // 0xF5 and above can never start a sequence, so each byte here is
  // its own error instead of one truncated four-byte sequence.
  from_list([<<0xF5, 0x80>>])
  |> datastream.utf8_decode
  |> datastream.to_list
  |> should.equal([Error(Nil), Error(Nil)])
}

pub fn utf8_decode_lossy_keeps_text_around_an_encoded_surrogate_test() {
  from_list([<<"hello":utf8, 0xED, 0xA0, 0x80, "world":utf8>>])
  |> datastream.utf8_decode_lossy
  |> datastream.to_string
  |> should.equal("helloworld")
}

// --- utf8_decode_lossy ----------------------------------------------------

pub fn utf8_decode_lossy_simple_ascii_test() {
  from_list([<<104, 105>>])
  |> datastream.utf8_decode_lossy
  |> datastream.to_list
  |> should.equal(["hi"])
}

pub fn utf8_decode_lossy_drops_invalid_lead_byte_test() {
  // 0xFF is not a valid lead byte. The lossy variant drops the error
  // entirely rather than emitting Error(Nil), so the surrounding ASCII
  // bytes pass through as plain strings.
  from_list([<<104, 255, 105>>])
  |> datastream.utf8_decode_lossy
  |> datastream.to_list
  |> should.equal(["h", "i"])
}

pub fn utf8_decode_lossy_drops_incomplete_trailing_sequence_test() {
  // A 4-byte lead with only 2 bytes available at EOF is invalid; the
  // lossy variant drops it without yielding any element.
  from_list([<<240, 159>>])
  |> datastream.utf8_decode_lossy
  |> datastream.to_list
  |> should.equal([])
}

pub fn utf8_decode_lossy_reassembles_split_codepoint_test() {
  // The boundary-spanning multi-byte codepoint still reassembles —
  // the lossy variant only drops actual decode errors, it does not
  // change the chunk-boundary handling of the underlying decoder.
  from_list([<<240, 159>>, <<145, 141>>])
  |> datastream.utf8_decode_lossy
  |> datastream.to_list
  |> should.equal(["👍"])
}

pub fn utf8_decode_lossy_empty_source_test() {
  from_list([])
  |> datastream.utf8_decode_lossy
  |> datastream.to_list
  |> should.equal([])
}

pub fn utf8_decode_lossy_composes_with_lines_test() {
  // The motivating use case from issue #200: BitArray chunks → UTF-8
  // → line framing without a hand-rolled Result-to-Option shim.
  from_list([<<"1 first\n2 sec">>, <<"ond\n3 third\n">>])
  |> datastream.utf8_decode_lossy
  |> datastream.lines
  |> datastream.to_list
  |> should.equal(["1 first", "2 second", "3 third"])
}

// --- utf8_encode ----------------------------------------------------------

pub fn utf8_encode_ascii_test() {
  from_list(["hi"])
  |> datastream.utf8_encode
  |> datastream.to_list
  |> should.equal([<<104, 105>>])
}

pub fn utf8_encode_empty_string_yields_empty_bit_array_test() {
  from_list([""])
  |> datastream.utf8_encode
  |> datastream.to_list
  |> should.equal([<<>>])
}

pub fn utf8_encode_multibyte_codepoint_test() {
  from_list(["👍"])
  |> datastream.utf8_encode
  |> datastream.to_list
  |> should.equal([<<240, 159, 145, 141>>])
}

pub fn utf8_encode_per_chunk_test() {
  from_list(["a", "b", "c"])
  |> datastream.utf8_encode
  |> datastream.to_list
  |> should.equal([<<97>>, <<98>>, <<99>>])
}

pub fn utf8_encode_empty_source_yields_empty_test() {
  from_list([])
  |> datastream.utf8_encode
  |> datastream.to_list
  |> should.equal([])
}

pub fn utf8_encode_then_decode_round_trip_ascii_test() {
  from_list(["hello"])
  |> datastream.utf8_encode
  |> datastream.utf8_decode
  |> datastream.collect_result
  |> should.equal(Ok(["hello"]))
}
