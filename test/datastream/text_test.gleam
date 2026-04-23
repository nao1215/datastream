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
