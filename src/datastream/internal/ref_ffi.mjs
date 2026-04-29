// JavaScript FFI for datastream/internal/ref.gleam.
//
// Mutable single-field object. Each Ref allocation is independent;
// no shared global state.

export function new_ref(value) {
  return { value: value };
}

export function get_ref(ref) {
  return ref.value;
}

export function set_ref(ref, value) {
  ref.value = value;
  return undefined;
}
