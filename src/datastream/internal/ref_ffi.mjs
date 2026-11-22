// JavaScript FFI for datastream/internal/ref.gleam.
//
// Mutable single-field object. Each Ref allocation is independent;
// no shared global state.

import { Ok, Error } from "../../gleam.mjs";

export function new_ref(value) {
  return { value: value, present: true };
}

export function get_ref(ref) {
  return ref.present ? new Ok(ref.value) : new Error(undefined);
}

export function set_ref(ref, value) {
  ref.value = value;
  ref.present = true;
  return undefined;
}

export function delete_ref(ref) {
  ref.value = undefined;
  ref.present = false;
  return undefined;
}
