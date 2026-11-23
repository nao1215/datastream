// JavaScript FFI for datastream/internal/cleanup.gleam.
//
// A cell holds the cleanup of one open resource. A frame lists the
// cells that should be cleaned up if the code running inside it throws.
// `frames` is the stack of frames entered by the synchronous code that
// is running now; pulls never suspend, so the stack is always balanced.

const frames = [];

function current() {
  return frames.length === 0 ? null : frames[frames.length - 1];
}

function register(cell) {
  const frame = current();
  if (frame === null || cell.frame === frame) return;
  if (cell.frame !== null) cell.frame.cells?.delete(cell);
  cell.frame = frame;
  if (frame.cells === null) frame.cells = new Set();
  frame.cells.add(cell);
}

function unregister(cell) {
  if (cell.frame !== null) {
    cell.frame.cells?.delete(cell);
    cell.frame = null;
  }
}

function quietly(fun) {
  try {
    fun();
  } catch (_) {
    // The exception that started the unwinding is the one reported.
  }
}

function clean(frame) {
  if (frame.cells === null) return;
  const cells = Array.from(frame.cells).reverse();
  frame.cells = null;
  for (const cell of cells) {
    const cleanup = cell.cleanup;
    cell.frame = null;
    if (cleanup !== null) {
      cell.cleanup = null;
      quietly(cleanup);
    }
  }
}

export function new_cell(cleanup) {
  const cell = { cleanup, frame: null };
  register(cell);
  return cell;
}

export function set_cell(cell, cleanup) {
  if (cell.cleanup === null) return undefined;
  cell.cleanup = cleanup;
  register(cell);
  return undefined;
}

export function touch_cell(cell) {
  if (cell.cleanup !== null) register(cell);
  return undefined;
}

export function take_cell(cell, _fallback) {
  const cleanup = cell.cleanup;
  if (cleanup === null) return undefined;
  cell.cleanup = null;
  unregister(cell);
  cleanup();
  return undefined;
}

export function discard_cell(cell) {
  cell.cleanup = null;
  unregister(cell);
  return undefined;
}

// `cells` is created on first use; most frames never hold a cell.
export function new_frame() {
  return { cells: null };
}

export function guard(body) {
  const frame = new_frame();
  frames.push(frame);
  let value;
  try {
    value = body();
  } catch (error) {
    frames.pop();
    clean(frame);
    throw error;
  }
  frames.pop();
  if (frame.cells === null) return value;
  // Cells still open move to the enclosing frame, oldest first.
  const cells = Array.from(frame.cells);
  frame.cells = null;
  for (const cell of cells) {
    cell.frame = null;
    register(cell);
  }
  return value;
}

export function guard_in(frame, body, on_raise) {
  frames.push(frame);
  let value;
  try {
    value = body();
  } catch (error) {
    frames.pop();
    clean(frame);
    quietly(on_raise);
    throw error;
  }
  frames.pop();
  return value;
}
