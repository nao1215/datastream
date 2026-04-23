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
