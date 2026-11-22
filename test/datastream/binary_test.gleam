import datastream.{Done, Next}
import datastream/internal/ref
import gleam/list
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

// --- bytes ----------------------------------------------------------------

pub fn bytes_yields_each_byte_in_order_test() {
  from_list([<<1, 2>>, <<3>>])
  |> datastream.bytes
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([1, 2, 3])
}

pub fn bytes_empty_chunk_yields_no_bytes_test() {
  from_list([<<>>])
  |> datastream.bytes
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([])
}

pub fn bytes_empty_source_yields_empty_test() {
  from_list([])
  |> datastream.bytes
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([])
}

pub fn bytes_preserves_full_byte_range_test() {
  from_list([<<255, 0, 128>>])
  |> datastream.bytes
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([255, 0, 128])
}

// --- length_prefixed -----------------------------------------------------

pub fn length_prefixed_one_byte_prefix_two_frames_test() {
  from_list([<<2, 65, 66, 1, 67>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<65, 66>>), Ok(<<67>>)])
}

pub fn length_prefixed_chunk_boundary_inside_payload_test() {
  from_list([<<2, 65>>, <<66>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<65, 66>>)])
}

pub fn length_prefixed_chunk_boundary_between_prefix_and_payload_test() {
  from_list([<<2>>, <<65, 66>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<65, 66>>)])
}

pub fn length_prefixed_zero_length_frame_test() {
  from_list([<<0>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<>>)])
}

pub fn length_prefixed_incomplete_payload_emits_error_test() {
  from_list([<<2, 65>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Error(datastream.IncompleteFrame(expected: 2, got: 1))])
}

pub fn length_prefixed_incomplete_prefix_emits_error_test() {
  from_list([<<2>>])
  |> datastream.length_prefixed(prefix_size: 2, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Error(datastream.IncompleteFrame(expected: 2, got: 1))])
}

pub fn length_prefixed_short_payload_after_full_frame_test() {
  // First frame is well-formed (length 1, payload 65); second prefix
  // declares 200 bytes but only 5 follow, so the truncation surfaces
  // after the well-formed frame.
  from_list([<<1, 65, 200, 1, 2, 3, 4, 5>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([
    Ok(<<65>>),
    Error(datastream.IncompleteFrame(expected: 200, got: 5)),
  ])
}

pub fn length_prefixed_zero_byte_payload_at_eof_is_ok_test() {
  // Prefix says "zero bytes follow"; that frame is well-formed and
  // the stream ends cleanly with no trailing IncompleteFrame.
  from_list([<<0>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<>>)])
}

pub fn length_prefixed_four_byte_big_endian_prefix_test() {
  from_list([<<0, 0, 0, 1, 99>>])
  |> datastream.length_prefixed(prefix_size: 4, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<99>>)])
}

pub fn length_prefixed_two_byte_prefix_decodes_big_endian_test() {
  // Length 257 = 0x0101 = <<1, 1>>; payload is 257 bytes.
  let payload = list.repeat(7, 257)
  let payload_bits = list.fold(payload, <<>>, fn(acc, b) { <<acc:bits, b>> })
  from_list([<<<<1, 1>>:bits, payload_bits:bits>>])
  |> datastream.length_prefixed(prefix_size: 2, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(payload_bits)])
}

pub fn length_prefixed_eight_byte_prefix_small_frame_test() {
  from_list([<<0, 0, 0, 0, 0, 0, 0, 2, 65, 66>>])
  |> datastream.length_prefixed(prefix_size: 8, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<65, 66>>)])
}

pub fn length_prefixed_prefix_split_across_chunks_test() {
  // prefix_size = 4, prefix bytes <<0, 0, 0, 1>> spans chunks
  // [<<0, 0>>, <<0, 1, 99>>]; payload is the trailing 0x63.
  from_list([<<0, 0>>, <<0, 1, 99>>])
  |> datastream.length_prefixed(prefix_size: 4, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<99>>)])
}

pub fn length_prefixed_multiple_frames_back_to_back_two_byte_prefix_test() {
  // Three frames of length 1, 2, 3 each with a 2-byte big-endian
  // prefix, all in a single chunk.
  from_list([<<0, 1, 65, 0, 2, 66, 67, 0, 3, 68, 69, 70>>])
  |> datastream.length_prefixed(prefix_size: 2, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<65>>), Ok(<<66, 67>>), Ok(<<68, 69, 70>>)])
}

// --- delimited ----------------------------------------------------------

pub fn delimited_yields_frames_separated_by_delimiter_test() {
  from_list([<<65, 66, 10, 67, 10>>])
  |> datastream.delimited(on: <<10>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<65, 66>>, <<67>>, <<>>])
}

pub fn delimited_preserves_empty_frames_between_consecutive_delimiters_test() {
  from_list([<<65, 10, 10, 66, 10>>])
  |> datastream.delimited(on: <<10>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<65>>, <<>>, <<66>>, <<>>])
}

pub fn delimited_chunk_boundary_after_delimiter_test() {
  from_list([<<65, 66, 10>>, <<67>>])
  |> datastream.delimited(on: <<10>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<65, 66>>, <<67>>])
}

pub fn delimited_delimiter_at_start_of_next_chunk_test() {
  from_list([<<65>>, <<10, 66>>])
  |> datastream.delimited(on: <<10>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<65>>, <<66>>])
}

pub fn delimited_trailing_partial_frame_preserved_test() {
  from_list([<<65, 10, 66>>])
  |> datastream.delimited(on: <<10>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<65>>, <<66>>])
}

pub fn delimited_empty_input_yields_no_frames_test() {
  from_list([<<>>])
  |> datastream.delimited(on: <<10>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([])
}

// --- fixed_size --------------------------------------------------------

pub fn fixed_size_evenly_divisible_test() {
  from_list([<<1, 2, 3, 4>>])
  |> datastream.fixed_size(size: 2)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>])
}

pub fn fixed_size_drops_trailing_partial_test() {
  from_list([<<1, 2, 3, 4, 5>>])
  |> datastream.fixed_size(size: 2)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>])
}

pub fn fixed_size_frame_straddles_chunks_test() {
  from_list([<<1, 2, 3>>, <<4, 5, 6>>])
  |> datastream.fixed_size(size: 2)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<1, 2>>, <<3, 4>>, <<5, 6>>])
}

pub fn fixed_size_empty_input_yields_no_frames_test() {
  from_list([<<>>])
  |> datastream.fixed_size(size: 2)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([])
}

pub fn delimited_rejects_empty_delimiter_test() {
  assert datastream.delimited(from_list([<<1, 2, 3>>]), on: <<>>)
    |> should.be_error
    == datastream.EmptyDelimiter
}

// --- checked variants (#189) -------------------------------------------

pub fn length_prefixed_checked_ok_decodes_one_byte_prefix_test() {
  let assert Ok(s) =
    datastream.length_prefixed(
      over: from_list([<<2, 1, 2, 1, 3>>]),
      prefix_size: 1,
      max_frame_size: 16_777_216,
    )
  s |> datastream.to_list |> should.equal([Ok(<<1, 2>>), Ok(<<3>>)])
}

pub fn length_prefixed_checked_rejects_three_byte_prefix_test() {
  let assert Error(datastream.InvalidPrefixSize(given: g)) =
    datastream.length_prefixed(
      over: from_list([<<>>]),
      prefix_size: 3,
      max_frame_size: 16_777_216,
    )
  g |> should.equal(3)
}

pub fn length_prefixed_checked_rejects_zero_prefix_test() {
  let assert Error(datastream.InvalidPrefixSize(given: g)) =
    datastream.length_prefixed(
      over: from_list([<<>>]),
      prefix_size: 0,
      max_frame_size: 16_777_216,
    )
  g |> should.equal(0)
}

pub fn length_prefixed_with_checked_ok_decodes_one_byte_prefix_test() {
  let assert Ok(s) =
    datastream.length_prefixed(
      over: from_list([<<2, 1, 2>>]),
      prefix_size: 1,
      max_frame_size: 64,
    )
  s |> datastream.to_list |> should.equal([Ok(<<1, 2>>)])
}

pub fn length_prefixed_with_checked_rejects_invalid_prefix_test() {
  let assert Error(datastream.InvalidPrefixSize(given: g)) =
    datastream.length_prefixed(
      over: from_list([<<>>]),
      prefix_size: 5,
      max_frame_size: 64,
    )
  g |> should.equal(5)
}

pub fn fixed_size_checked_ok_matches_fixed_size_test() {
  let assert Ok(s) =
    datastream.fixed_size(over: from_list([<<1, 2, 3, 4, 5, 6>>]), size: 2)
  s
  |> datastream.collect_result
  |> should.be_ok
  |> should.equal([<<1, 2>>, <<3, 4>>, <<5, 6>>])
}

pub fn fixed_size_checked_rejects_zero_test() {
  let assert Error(datastream.NotPositive(
    argument: "size",
    function: name,
    given: g,
  )) = datastream.fixed_size(over: from_list([<<>>]), size: 0)
  name |> should.equal("fixed_size")
  g |> should.equal(0)
}

pub fn fixed_size_checked_rejects_negative_test() {
  let assert Error(datastream.NotPositive(
    argument: "size",
    function: _,
    given: g,
  )) = datastream.fixed_size(over: from_list([<<>>]), size: -3)
  g |> should.equal(-3)
}

// --- Issue #158: FrameTooLarge on oversized prefix claims ---

pub fn length_prefixed_rejects_oversized_frame_claim_test() {
  // 4-byte prefix claiming 999,999,999 bytes, with only 4 payload bytes
  from_list([<<59, 154, 201, 255, 1, 2, 3, 4>>])
  |> datastream.length_prefixed(prefix_size: 4, max_frame_size: 1024)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([
    Error(datastream.FrameTooLarge(claimed: 999_999_999, max: 1024)),
  ])
}

pub fn length_prefixed_with_small_max_allows_small_frames_test() {
  // 1-byte prefix: length=3, payload=<<1,2,3>> — fits in max_frame_size=10
  from_list([<<3, 1, 2, 3>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 10)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<1, 2, 3>>)])
}

pub fn length_prefixed_default_allows_normal_frames_test() {
  // Default max (16 MiB) allows a 3-byte frame without issue
  from_list([<<3, 65, 66, 67>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<65, 66, 67>>)])
}

// --- boundaries found by mutation testing ---------------------------------

pub fn length_prefixed_accepts_a_frame_of_exactly_max_frame_size_test() {
  from_list([<<3, 1, 2, 3>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 3)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<1, 2, 3>>)])
}

pub fn length_prefixed_reads_two_frames_from_one_chunk_test() {
  from_list([<<2, 1, 2, 1, 3>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Ok(<<1, 2>>), Ok(<<3>>)])
}

pub fn fixed_size_one_yields_every_byte_test() {
  from_list([<<1, 2>>, <<3>>])
  |> datastream.fixed_size(size: 1)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<1>>, <<2>>, <<3>>])
}

pub fn fixed_size_checked_accepts_one_test() {
  let assert Ok(s) = datastream.fixed_size(over: from_list([<<1, 2>>]), size: 1)
  s |> datastream.collect_result |> should.be_ok |> should.equal([<<1>>, <<2>>])
}

pub fn delimited_keeps_the_empty_frame_after_a_trailing_delimiter_test() {
  from_list([<<1, 10>>])
  |> datastream.delimited(on: <<10>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<1>>, <<>>])
}

// --- length_prefixed ends the stream after FrameTooLarge --------------------

pub fn length_prefixed_ends_after_frame_too_large_across_chunks_test() {
  // Once a prefix claims too many bytes the framing is lost, so the
  // bytes after it must not be read as new prefixes.
  from_list([<<5>>, <<1, 2, 3, 4, 5>>, <<1, 9>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 2)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Error(datastream.FrameTooLarge(claimed: 5, max: 2))])
}

pub fn length_prefixed_ends_after_frame_too_large_in_one_chunk_test() {
  from_list([<<5, 1, 2, 3, 4, 5, 1, 9>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 2)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Error(datastream.FrameTooLarge(claimed: 5, max: 2))])
}

pub fn length_prefixed_closes_upstream_after_frame_too_large_test() {
  let closed = ref.new(0)
  datastream.resource(
    open: fn() { [<<5, 1, 2>>, <<3, 4, 5>>] },
    next: fn(chunks) {
      case chunks {
        [] -> Done
        [chunk, ..rest] -> Next(chunk, rest)
      }
    },
    close: fn(_) { ref.set(closed, read(closed) + 1) },
  )
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 2)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([Error(datastream.FrameTooLarge(claimed: 5, max: 2))])
  read(closed) |> should.equal(1)
}

pub fn length_prefixed_frames_split_over_many_chunks_test() {
  from_list([<<0>>, <<3, 1>>, <<2>>, <<3, 0, 1>>, <<>>, <<9>>, <<0, 2, 7>>])
  |> datastream.length_prefixed(prefix_size: 2, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.to_list
  |> should.equal([
    Ok(<<1, 2, 3>>),
    Ok(<<9>>),
    Error(datastream.IncompleteFrame(expected: 2, got: 1)),
  ])
}

pub fn fixed_size_frames_split_over_many_chunks_test() {
  from_list([<<1>>, <<2, 3>>, <<>>, <<4, 5, 6, 7>>, <<8>>])
  |> datastream.fixed_size(size: 3)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<1, 2, 3>>, <<4, 5, 6>>])
}

pub fn delimited_delimiter_split_over_many_chunks_test() {
  from_list([<<1, 0>>, <<0>>, <<0, 2, 3, 0>>, <<>>, <<0, 0>>, <<0>>, <<4>>])
  |> datastream.delimited(on: <<0, 0, 0>>)
  |> should.be_ok
  |> datastream.map(with: should.be_ok)
  |> datastream.to_list
  |> should.equal([<<1>>, <<2, 3>>, <<0, 4>>])
}

fn read(cell: ref.Ref(a)) -> a {
  case ref.get(cell) {
    Ok(value) -> value
    Error(_) -> panic as "test ref was deleted"
  }
}

// --- framers reject input that is not byte-aligned ---------------------------

pub fn fixed_size_rejects_non_byte_aligned_chunks_test() {
  from_list([<<1:4>>, <<2:4>>])
  |> datastream.fixed_size(size: 1)
  |> should.be_ok
  |> datastream.take(up_to: 10)
  |> datastream.to_list
  |> should.equal([Error(datastream.UnalignedChunk(bit_size: 4))])
}

pub fn length_prefixed_rejects_a_non_byte_aligned_chunk_test() {
  from_list([<<1:4>>])
  |> datastream.length_prefixed(prefix_size: 1, max_frame_size: 16_777_216)
  |> should.be_ok
  |> datastream.take(up_to: 10)
  |> datastream.to_list
  |> should.equal([Error(datastream.UnalignedChunk(bit_size: 4))])
}

pub fn delimited_rejects_a_non_byte_aligned_chunk_test() {
  from_list([<<1:4>>])
  |> datastream.delimited(on: <<0>>)
  |> should.be_ok
  |> datastream.take(up_to: 10)
  |> datastream.to_list
  |> should.equal([Error(datastream.UnalignedChunk(bit_size: 4))])
}

pub fn bytes_rejects_a_non_byte_aligned_chunk_test() {
  from_list([<<1:4>>])
  |> datastream.bytes
  |> datastream.take(up_to: 10)
  |> datastream.to_list
  |> should.equal([Error(datastream.UnalignedChunk(bit_size: 4))])
}
