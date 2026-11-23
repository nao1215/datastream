// JavaScript FFI for the text functions in datastream.gleam.

import { Ok, Error } from "../../gleam.mjs";

// Split at the first occurrence of `delimiter`, comparing code units.
export function split_once(text, delimiter) {
  const index = text.indexOf(delimiter);
  if (index < 0) return new Error(undefined);
  return new Ok([text.slice(0, index), text.slice(index + delimiter.length)]);
}

function isRegionalIndicatorEnd(text, index) {
  // U+1F1E6..U+1F1FF is the surrogate pair D83C DDE6..DDFF.
  if (index < 2) return false;
  const high = text.charCodeAt(index - 2);
  const low = text.charCodeAt(index - 1);
  return high === 0xd83c && low >= 0xdde6 && low <= 0xddff;
}

// [truncated, suffix]: the last `size` code units of `text`, moved back
// to a code point boundary and past any regional indicators just before
// it, so the suffix counts regional indicator pairs from the same place
// as the whole text.
export function suffix(text, size) {
  if (text.length <= size) return [false, text];
  let start = text.length - size;
  const unit = text.charCodeAt(start);
  if (unit >= 0xdc00 && unit <= 0xdfff) start -= 1;
  while (isRegionalIndicatorEnd(text, start)) start -= 2;
  if (start <= 0) return [false, text];
  return [true, text.slice(start)];
}
