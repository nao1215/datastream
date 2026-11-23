import { Ok, Error, toList } from "./gleam.mjs";

export function rescue(thunk) {
  try {
    return new Ok(thunk());
  } catch (_) {
    return new Error(undefined);
  }
}

let ledger = new Map();
let nextId = 0;
let notesSeen = [];

export function ledger_reset() {
  ledger = new Map();
  nextId = 0;
  notesSeen = [];
}

export function ledger_open() {
  const id = nextId++;
  ledger.set(id, 0);
  return id;
}

export function ledger_close(id) {
  ledger.set(id, (ledger.get(id) ?? 100) + 1);
}

// [opened, resources whose close count is not one]
export function ledger_report() {
  let bad = 0;
  for (const closes of ledger.values()) if (closes !== 1) bad++;
  return [ledger.size, bad];
}

export function note(value) {
  notesSeen.push(value);
}

export function notes() {
  return toList(notesSeen);
}
