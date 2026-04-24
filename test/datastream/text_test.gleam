import datastream.{Done, Next}
import datastream/fold
import datastream/text
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
  |> text.lines
  |> fold.to_list
  |> should.equal(["hello", "world"])
}

pub fn lines_partial_trailing_line_emitted_test() {
  from_list(["hello\nworld"])
  |> text.lines
  |> fold.to_list
  |> should.equal(["hello", "world"])
}

pub fn lines_empty_chunk_yields_no_lines_test() {
  from_list([""]) |> text.lines |> fold.to_list |> should.equal([])
}

pub fn lines_two_consecutive_terminators_yield_two_empty_lines_test() {
  from_list(["\n\n"]) |> text.lines |> fold.to_list |> should.equal(["", ""])
}

pub fn lines_crlf_terminator_test() {
  from_list(["a\r\nb\r\n"])
  |> text.lines
  |> fold.to_list
  |> should.equal(["a", "b"])
}

pub fn lines_split_across_chunks_test() {
  from_list(["hel", "lo\nwor", "ld"])
  |> text.lines
  |> fold.to_list
  |> should.equal(["hello", "world"])
}

pub fn lines_crlf_split_across_chunks_test() {
  from_list(["a\r", "\nb"])
  |> text.lines
  |> fold.to_list
  |> should.equal(["a", "b"])
}

pub fn lines_lone_cr_at_chunk_end_treated_as_terminator_test() {
  from_list(["a\r", "b"])
  |> text.lines
  |> fold.to_list
  |> should.equal(["a", "b"])
}

pub fn lines_on_truly_empty_source_yields_no_lines_test() {
  from_list([]) |> text.lines |> fold.to_list |> should.equal([])
}

// --- split -----------------------------------------------------------------

pub fn split_on_comma_yields_pieces_test() {
  from_list(["a,b,c"])
  |> text.split(on: ",")
  |> fold.to_list
  |> should.equal(["a", "b", "c"])
}

pub fn split_preserves_empty_between_consecutive_delimiters_test() {
  from_list(["a,,b"])
  |> text.split(on: ",")
  |> fold.to_list
  |> should.equal(["a", "", "b"])
}

pub fn split_preserves_trailing_empty_test() {
  from_list(["a,b,"])
  |> text.split(on: ",")
  |> fold.to_list
  |> should.equal(["a", "b", ""])
}

pub fn split_preserves_leading_empty_test() {
  from_list([",a,b"])
  |> text.split(on: ",")
  |> fold.to_list
  |> should.equal(["", "a", "b"])
}

pub fn split_empty_input_yields_one_empty_test() {
  from_list([""])
  |> text.split(on: ",")
  |> fold.to_list
  |> should.equal([""])
}

pub fn split_truly_empty_source_yields_empty_test() {
  from_list([])
  |> text.split(on: ",")
  |> fold.to_list
  |> should.equal([])
}

pub fn split_delimiter_across_chunk_boundary_test() {
  from_list(["a,b", ",c"])
  |> text.split(on: ",")
  |> fold.to_list
  |> should.equal(["a", "b", "c"])
}

pub fn split_non_delimiter_join_across_chunks_test() {
  from_list(["a,b", "c,d"])
  |> text.split(on: ",")
  |> fold.to_list
  |> should.equal(["a", "bc", "d"])
}

pub fn split_empty_delimiter_emits_each_grapheme_test() {
  from_list(["abc"])
  |> text.split(on: "")
  |> fold.to_list
  |> should.equal(["a", "b", "c"])
}

pub fn split_empty_delimiter_handles_multibyte_grapheme_test() {
  from_list(["a👍b"])
  |> text.split(on: "")
  |> fold.to_list
  |> should.equal(["a", "👍", "b"])
}

// --- utf8_decode ----------------------------------------------------------

pub fn utf8_decode_simple_ascii_test() {
  from_list([<<104, 105>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Ok("hi")])
}

pub fn utf8_decode_empty_chunk_yields_no_elements_test() {
  from_list([<<>>]) |> text.utf8_decode |> fold.to_list |> should.equal([])
}

pub fn utf8_decode_empty_source_yields_empty_test() {
  from_list([]) |> text.utf8_decode |> fold.to_list |> should.equal([])
}

pub fn utf8_decode_four_byte_codepoint_in_one_chunk_test() {
  from_list([<<240, 159, 145, 141>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Ok("👍")])
}

pub fn utf8_decode_codepoint_split_across_chunks_2_2_test() {
  from_list([<<240, 159>>, <<145, 141>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Ok("👍")])
}

pub fn utf8_decode_codepoint_split_across_chunks_3_1_test() {
  from_list([<<240, 159, 145>>, <<141>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Ok("👍")])
}

pub fn utf8_decode_per_chunk_decode_test() {
  from_list([<<104>>, <<105>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Ok("h"), Ok("i")])
}

pub fn utf8_decode_invalid_lead_byte_test() {
  from_list([<<255>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Error(Nil)])
}

pub fn utf8_decode_resumes_after_error_test() {
  from_list([<<104, 255, 105>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Ok("h"), Error(Nil), Ok("i")])
}

pub fn utf8_decode_incomplete_trailing_sequence_at_eof_test() {
  from_list([<<240, 159>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Error(Nil)])
}

pub fn utf8_decode_rejects_overlong_two_byte_sequence_test() {
  // 0xC0 0x80 is an overlong encoding of U+0000 and must be rejected
  // (RFC 3629). Here 0xC0 is rejected at the lead-byte classification
  // step (lead < 0xC2), then 0x80 is a lone continuation byte and is
  // also rejected. Two consecutive Error(Nil) — the decoder does not
  // swallow the second byte silently.
  from_list([<<0xC0, 0x80>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Error(Nil), Error(Nil)])
}

pub fn utf8_decode_rejects_invalid_continuation_in_two_byte_sequence_test() {
  // 0xC2 is a valid 2-byte lead; 0x40 is NOT a valid continuation
  // (< 0x80). Decoder emits Error and resumes on 0x40 — which is
  // plain ASCII "@" — so the next output is Ok("@").
  from_list([<<0xC2, 0x40>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Error(Nil), Ok("@")])
}

pub fn utf8_decode_rejects_invalid_continuation_in_three_byte_sequence_test() {
  // 0xE0 is a valid 3-byte lead. 0xA0 is a valid first continuation
  // but 0x40 is not. The decoder must emit Error and resume from
  // 0x40, which is ASCII "@", then consume 0x61 ("a"). The two
  // ASCII bytes coalesce into a single Ok("@a").
  from_list([<<0xE0, 0xA0, 0x40, 0x61>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Error(Nil), Ok("@a")])
}

pub fn utf8_decode_rejects_consecutive_lone_continuation_bytes_test() {
  // Bytes in [0x80, 0xC2) are never valid as a lead byte. Two of
  // them in a row produce two separate Error(Nil) — the decoder
  // does not merge consecutive invalid bytes into a single error.
  from_list([<<0x80, 0x81>>])
  |> text.utf8_decode
  |> fold.to_list
  |> should.equal([Error(Nil), Error(Nil)])
}

// --- utf8_encode ----------------------------------------------------------

pub fn utf8_encode_ascii_test() {
  from_list(["hi"])
  |> text.utf8_encode
  |> fold.to_list
  |> should.equal([<<104, 105>>])
}

pub fn utf8_encode_empty_string_yields_empty_bit_array_test() {
  from_list([""]) |> text.utf8_encode |> fold.to_list |> should.equal([<<>>])
}

pub fn utf8_encode_multibyte_codepoint_test() {
  from_list(["👍"])
  |> text.utf8_encode
  |> fold.to_list
  |> should.equal([<<240, 159, 145, 141>>])
}

pub fn utf8_encode_per_chunk_test() {
  from_list(["a", "b", "c"])
  |> text.utf8_encode
  |> fold.to_list
  |> should.equal([<<97>>, <<98>>, <<99>>])
}

pub fn utf8_encode_empty_source_yields_empty_test() {
  from_list([]) |> text.utf8_encode |> fold.to_list |> should.equal([])
}

pub fn utf8_encode_then_decode_round_trip_ascii_test() {
  from_list(["hello"])
  |> text.utf8_encode
  |> text.utf8_decode
  |> fold.collect_result
  |> should.equal(Ok(["hello"]))
}
