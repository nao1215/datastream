import datastream.{Done, Next}
import datastream/binary
import datastream/fold
import gleam/list
import gleeunit
import gleeunit/should

@target(erlang)
import gleam/erlang/process

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

// --- bytes ----------------------------------------------------------------

pub fn bytes_yields_each_byte_in_order_test() {
  from_list([<<1, 2>>, <<3>>])
  |> binary.bytes
  |> fold.to_list
  |> should.equal([1, 2, 3])
}

pub fn bytes_empty_chunk_yields_no_bytes_test() {
  from_list([<<>>]) |> binary.bytes |> fold.to_list |> should.equal([])
}

pub fn bytes_empty_source_yields_empty_test() {
  from_list([]) |> binary.bytes |> fold.to_list |> should.equal([])
}

pub fn bytes_preserves_full_byte_range_test() {
  from_list([<<255, 0, 128>>])
  |> binary.bytes
  |> fold.to_list
  |> should.equal([255, 0, 128])
}

// --- length_prefixed -----------------------------------------------------

pub fn length_prefixed_one_byte_prefix_two_frames_test() {
  from_list([<<2, 65, 66, 1, 67>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([Ok(<<65, 66>>), Ok(<<67>>)])
}

pub fn length_prefixed_chunk_boundary_inside_payload_test() {
  from_list([<<2, 65>>, <<66>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([Ok(<<65, 66>>)])
}

pub fn length_prefixed_chunk_boundary_between_prefix_and_payload_test() {
  from_list([<<2>>, <<65, 66>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([Ok(<<65, 66>>)])
}

pub fn length_prefixed_zero_length_frame_test() {
  from_list([<<0>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([Ok(<<>>)])
}

pub fn length_prefixed_incomplete_payload_emits_error_test() {
  from_list([<<2, 65>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([Error(binary.IncompleteFrame(expected: 2, got: 1))])
}

pub fn length_prefixed_incomplete_prefix_emits_error_test() {
  from_list([<<2>>])
  |> binary.length_prefixed(prefix_size: 2)
  |> fold.to_list
  |> should.equal([Error(binary.IncompleteFrame(expected: 2, got: 1))])
}

pub fn length_prefixed_short_payload_after_full_frame_test() {
  // First frame is well-formed (length 1, payload 65); second prefix
  // declares 200 bytes but only 5 follow, so the truncation surfaces
  // after the well-formed frame.
  from_list([<<1, 65, 200, 1, 2, 3, 4, 5>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([
    Ok(<<65>>),
    Error(binary.IncompleteFrame(expected: 200, got: 5)),
  ])
}

pub fn length_prefixed_zero_byte_payload_at_eof_is_ok_test() {
  // Prefix says "zero bytes follow"; that frame is well-formed and
  // the stream ends cleanly with no trailing IncompleteFrame.
  from_list([<<0>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([Ok(<<>>)])
}

pub fn length_prefixed_four_byte_big_endian_prefix_test() {
  from_list([<<0, 0, 0, 1, 99>>])
  |> binary.length_prefixed(prefix_size: 4)
  |> fold.to_list
  |> should.equal([Ok(<<99>>)])
}

pub fn length_prefixed_two_byte_prefix_decodes_big_endian_test() {
  // Length 257 = 0x0101 = <<1, 1>>; payload is 257 bytes.
  let payload = list.repeat(7, 257)
  let payload_bits = list.fold(payload, <<>>, fn(acc, b) { <<acc:bits, b>> })
  from_list([<<<<1, 1>>:bits, payload_bits:bits>>])
  |> binary.length_prefixed(prefix_size: 2)
  |> fold.to_list
  |> should.equal([Ok(payload_bits)])
}

pub fn length_prefixed_eight_byte_prefix_small_frame_test() {
  from_list([<<0, 0, 0, 0, 0, 0, 0, 2, 65, 66>>])
  |> binary.length_prefixed(prefix_size: 8)
  |> fold.to_list
  |> should.equal([Ok(<<65, 66>>)])
}

pub fn length_prefixed_prefix_split_across_chunks_test() {
  // prefix_size = 4, prefix bytes <<0, 0, 0, 1>> spans chunks
  // [<<0, 0>>, <<0, 1, 99>>]; payload is the trailing 0x63.
  from_list([<<0, 0>>, <<0, 1, 99>>])
  |> binary.length_prefixed(prefix_size: 4)
  |> fold.to_list
  |> should.equal([Ok(<<99>>)])
}

pub fn length_prefixed_multiple_frames_back_to_back_two_byte_prefix_test() {
  // Three frames of length 1, 2, 3 each with a 2-byte big-endian
  // prefix, all in a single chunk.
  from_list([<<0, 1, 65, 0, 2, 66, 67, 0, 3, 68, 69, 70>>])
  |> binary.length_prefixed(prefix_size: 2)
  |> fold.to_list
  |> should.equal([Ok(<<65>>), Ok(<<66, 67>>), Ok(<<68, 69, 70>>)])
}

// --- delimited ----------------------------------------------------------

pub fn delimited_yields_frames_separated_by_delimiter_test() {
  from_list([<<65, 66, 10, 67, 10>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65, 66>>, <<67>>, <<>>])
}

pub fn delimited_preserves_empty_frames_between_consecutive_delimiters_test() {
  from_list([<<65, 10, 10, 66, 10>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65>>, <<>>, <<66>>, <<>>])
}

pub fn delimited_chunk_boundary_after_delimiter_test() {
  from_list([<<65, 66, 10>>, <<67>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65, 66>>, <<67>>])
}

pub fn delimited_delimiter_at_start_of_next_chunk_test() {
  from_list([<<65>>, <<10, 66>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65>>, <<66>>])
}

pub fn delimited_trailing_partial_frame_preserved_test() {
  from_list([<<65, 10, 66>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([<<65>>, <<66>>])
}

pub fn delimited_empty_input_yields_no_frames_test() {
  from_list([<<>>])
  |> binary.delimited(on: <<10>>)
  |> fold.to_list
  |> should.equal([])
}

// --- fixed_size --------------------------------------------------------

pub fn fixed_size_evenly_divisible_test() {
  from_list([<<1, 2, 3, 4>>])
  |> binary.fixed_size(size: 2)
  |> fold.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>])
}

pub fn fixed_size_drops_trailing_partial_test() {
  from_list([<<1, 2, 3, 4, 5>>])
  |> binary.fixed_size(size: 2)
  |> fold.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>])
}

pub fn fixed_size_frame_straddles_chunks_test() {
  from_list([<<1, 2, 3>>, <<4, 5, 6>>])
  |> binary.fixed_size(size: 2)
  |> fold.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>, <<5, 6>>])
}

pub fn fixed_size_empty_input_yields_no_frames_test() {
  from_list([<<>>])
  |> binary.fixed_size(size: 2)
  |> fold.to_list
  |> should.equal([])
}

// --- construction-time panics (Erlang target) ----------------------------
//
// `length_prefixed` and `fixed_size` reject malformed knobs at
// construction time with a panic. These tests pin that contract so a
// future refactor of the validation path has to update them
// deliberately. Erlang-only because the panic-detection helper spawns a
// process; JavaScript has no matching primitive. The validation logic
// itself is cross-target.

@target(erlang)
fn panicked(thunk: fn() -> Nil) -> Bool {
  let done = process.new_subject()
  let _pid =
    process.spawn_unlinked(fn() {
      thunk()
      process.send(done, Nil)
    })
  case process.receive(from: done, within: 100) {
    Ok(_) -> False
    Error(_) -> True
  }
}

@target(erlang)
pub fn length_prefixed_panics_on_prefix_size_three_test() {
  let did_panic =
    panicked(fn() {
      let _result = binary.length_prefixed(from_list([<<>>]), prefix_size: 3)
      Nil
    })
  did_panic |> should.be_true
}

@target(erlang)
pub fn length_prefixed_panics_on_prefix_size_sixteen_test() {
  let did_panic =
    panicked(fn() {
      let _result = binary.length_prefixed(from_list([<<>>]), prefix_size: 16)
      Nil
    })
  did_panic |> should.be_true
}

@target(erlang)
pub fn fixed_size_panics_on_zero_size_test() {
  let did_panic =
    panicked(fn() {
      let _result = binary.fixed_size(from_list([<<>>]), size: 0)
      Nil
    })
  did_panic |> should.be_true
}

@target(erlang)
pub fn fixed_size_panics_on_negative_size_test() {
  let did_panic =
    panicked(fn() {
      let _result = binary.fixed_size(from_list([<<>>]), size: -10)
      Nil
    })
  did_panic |> should.be_true
}

// --- checked variants (#189) -------------------------------------------

pub fn length_prefixed_checked_ok_decodes_one_byte_prefix_test() {
  let assert Ok(s) =
    binary.length_prefixed_checked(
      over: from_list([<<2, 1, 2, 1, 3>>]),
      prefix_size: 1,
    )
  s |> fold.to_list |> should.equal([Ok(<<1, 2>>), Ok(<<3>>)])
}

pub fn length_prefixed_checked_rejects_three_byte_prefix_test() {
  let assert Error(binary.InvalidPrefixSize(function: name, given: g)) =
    binary.length_prefixed_checked(over: from_list([<<>>]), prefix_size: 3)
  name |> should.equal("length_prefixed")
  g |> should.equal(3)
}

pub fn length_prefixed_checked_rejects_zero_prefix_test() {
  let assert Error(binary.InvalidPrefixSize(function: _, given: g)) =
    binary.length_prefixed_checked(over: from_list([<<>>]), prefix_size: 0)
  g |> should.equal(0)
}

pub fn length_prefixed_with_checked_ok_decodes_one_byte_prefix_test() {
  let assert Ok(s) =
    binary.length_prefixed_with_checked(
      over: from_list([<<2, 1, 2>>]),
      prefix_size: 1,
      max_frame_size: 64,
    )
  s |> fold.to_list |> should.equal([Ok(<<1, 2>>)])
}

pub fn length_prefixed_with_checked_rejects_invalid_prefix_test() {
  let assert Error(binary.InvalidPrefixSize(function: name, given: g)) =
    binary.length_prefixed_with_checked(
      over: from_list([<<>>]),
      prefix_size: 5,
      max_frame_size: 64,
    )
  name |> should.equal("length_prefixed")
  g |> should.equal(5)
}

pub fn fixed_size_checked_ok_matches_fixed_size_test() {
  let assert Ok(s) =
    binary.fixed_size_checked(over: from_list([<<1, 2, 3, 4, 5, 6>>]), size: 2)
  s |> fold.to_list |> should.equal([<<1, 2>>, <<3, 4>>, <<5, 6>>])
}

pub fn fixed_size_checked_rejects_zero_test() {
  let assert Error(binary.NotPositiveSize(function: name, given: g)) =
    binary.fixed_size_checked(over: from_list([<<>>]), size: 0)
  name |> should.equal("fixed_size")
  g |> should.equal(0)
}

pub fn fixed_size_checked_rejects_negative_test() {
  let assert Error(binary.NotPositiveSize(function: _, given: g)) =
    binary.fixed_size_checked(over: from_list([<<>>]), size: -3)
  g |> should.equal(-3)
}

// --- Issue #158: FrameTooLarge on oversized prefix claims ---

pub fn length_prefixed_rejects_oversized_frame_claim_test() {
  // 4-byte prefix claiming 999,999,999 bytes, with only 4 payload bytes
  from_list([<<59, 154, 201, 255, 1, 2, 3, 4>>])
  |> binary.length_prefixed_with(prefix_size: 4, max_frame_size: 1024)
  |> fold.to_list
  |> should.equal([
    Error(binary.FrameTooLarge(claimed: 999_999_999, max: 1024)),
  ])
}

pub fn length_prefixed_with_small_max_allows_small_frames_test() {
  // 1-byte prefix: length=3, payload=<<1,2,3>> — fits in max_frame_size=10
  from_list([<<3, 1, 2, 3>>])
  |> binary.length_prefixed_with(prefix_size: 1, max_frame_size: 10)
  |> fold.to_list
  |> should.equal([Ok(<<1, 2, 3>>)])
}

pub fn length_prefixed_default_allows_normal_frames_test() {
  // Default max (16 MiB) allows a 3-byte frame without issue
  from_list([<<3, 65, 66, 67>>])
  |> binary.length_prefixed(prefix_size: 1)
  |> fold.to_list
  |> should.equal([Ok(<<65, 66, 67>>)])
}
