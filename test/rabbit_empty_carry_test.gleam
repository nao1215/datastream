import datastream
import gleam/list

pub fn split_preserves_empty_input_marker_test() {
  use delimiter <- list.each(["|", "::"])
  assert datastream.empty()
    |> datastream.split(on: delimiter)
    |> datastream.to_list
    == []
  assert datastream.from_list(["", "", ""])
    |> datastream.split(on: delimiter)
    |> datastream.to_list
    == [""]
}

pub fn delimited_empty_chunks_do_not_create_a_frame_test() {
  use delimiter <- list.each([<<0>>, <<0, 0>>])
  let assert Ok(frames) =
    datastream.delimited(
      datastream.from_list([<<>>, <<>>, <<>>]),
      on: delimiter,
    )
  assert datastream.to_list(frames) == []
}

pub fn empty_chunks_preserve_cross_chunk_delimiters_and_trailing_empty_test() {
  let empty_text = list.repeat("", 200)
  let text = list.append(["a:"], list.append(empty_text, [":b::", ""]))
  assert datastream.from_list(text)
    |> datastream.split(on: "::")
    |> datastream.to_list
    == ["a", "b", ""]
  let empty_binary = list.repeat(<<>>, 200)
  let bytes =
    list.append(
      [<<"a", 0>>],
      list.append(empty_binary, [<<0, "b", 0, 0>>, <<>>]),
    )
  let assert Ok(frames) =
    datastream.delimited(datastream.from_list(bytes), on: <<0, 0>>)
  assert datastream.to_list(frames) == [Ok(<<"a">>), Ok(<<"b">>), Ok(<<>>)]
}
