//// Cleanup when a callback raises inside a terminal operation.
////
//// Each resource records its close calls in a ledger. After a raising
//// run, every opened resource must have been closed exactly once, at
//// its latest state.

import datastream.{type Stream, Done, Next}
import gleam/int
import gleam/list
import gleam/result

@external(erlang, "exception_cleanup_ffi", "rescue")
@external(javascript, "./exception_cleanup_ffi.mjs", "rescue")
fn rescue(thunk: fn() -> a) -> Result(a, Nil)

@external(erlang, "exception_cleanup_ffi", "ledger_reset")
@external(javascript, "./exception_cleanup_ffi.mjs", "ledger_reset")
fn ledger_reset() -> Nil

@external(erlang, "exception_cleanup_ffi", "ledger_open")
@external(javascript, "./exception_cleanup_ffi.mjs", "ledger_open")
fn ledger_open() -> Int

@external(erlang, "exception_cleanup_ffi", "ledger_close")
@external(javascript, "./exception_cleanup_ffi.mjs", "ledger_close")
fn ledger_close(id: Int) -> Nil

@external(erlang, "exception_cleanup_ffi", "ledger_report")
@external(javascript, "./exception_cleanup_ffi.mjs", "ledger_report")
fn ledger_report() -> #(Int, Int)

@external(erlang, "exception_cleanup_ffi", "note")
@external(javascript, "./exception_cleanup_ffi.mjs", "note")
fn note(value: Int) -> Nil

@external(erlang, "exception_cleanup_ffi", "notes")
@external(javascript, "./exception_cleanup_ffi.mjs", "notes")
fn notes() -> List(Int)

/// A resource yielding `0..count - 1` that records its close and the
/// index it was closed at.
fn counted(count: Int) -> Stream(Int) {
  datastream.resource(
    open: fn() { #(ledger_open(), 0) },
    next: fn(state) {
      let #(id, index) = state
      case index >= count {
        True -> Done
        False -> Next(index, #(id, index + 1))
      }
    },
    close: fn(state) {
      note(state.1)
      ledger_close(state.0)
    },
  )
}

fn explode_at(stream: Stream(Int), trigger: Int) -> Stream(Int) {
  datastream.map(stream, fn(value) {
    case value == trigger {
      True -> panic as "explode"
      False -> value
    }
  })
}

fn assert_closed_once(opened: Int) -> Nil {
  assert ledger_report() == #(opened, 0)
}

pub fn map_raise_closes_resource_once_at_latest_state_test() {
  ledger_reset()
  let result =
    rescue(fn() { counted(10) |> explode_at(3) |> datastream.to_list })
  assert result == Error(Nil)
  assert_closed_once(1)
  assert notes() == [4]
}

pub fn raise_on_first_element_closes_resource_test() {
  ledger_reset()
  let result = rescue(fn() { counted(10) |> explode_at(0) |> datastream.first })
  assert result == Error(Nil)
  assert_closed_once(1)
}

pub fn raise_in_next_closes_the_state_passed_to_next_test() {
  ledger_reset()
  let stream =
    datastream.resource(
      open: fn() { #(ledger_open(), 0) },
      next: fn(state) {
        case state.1 {
          2 -> panic as "read failed"
          index -> Next(index, #(state.0, index + 1))
        }
      },
      close: fn(state) {
        note(state.1)
        ledger_close(state.0)
      },
    )
  assert rescue(fn() { datastream.drain(stream) }) == Error(Nil)
  assert_closed_once(1)
  assert notes() == [2]
}

pub fn raise_in_terminal_callback_closes_resource_test() {
  ledger_reset()
  let fold =
    rescue(fn() {
      datastream.fold(counted(5), 0, fn(total, value) {
        case value {
          2 -> panic as "fold"
          _ -> total + value
        }
      })
    })
  assert fold == Error(Nil)
  let each =
    rescue(fn() {
      datastream.each(counted(5), fn(value) {
        case value {
          1 -> panic as "each"
          _ -> Nil
        }
      })
    })
  assert each == Error(Nil)
  assert_closed_once(2)
}

pub fn raise_closes_inner_before_outer_test() {
  ledger_reset()
  let stream =
    counted(3)
    |> datastream.flat_map(fn(_) { counted(3) })
    |> explode_at(1)
  assert rescue(fn() { datastream.to_list(stream) }) == Error(Nil)
  assert_closed_once(2)
  // The inner resource (closed at index 2) closes before the outer (1).
  assert notes() == [2, 1]
}

pub fn raising_close_is_not_called_again_test() {
  ledger_reset()
  let failing_close =
    datastream.resource(
      open: fn() { ledger_open() },
      next: fn(id) { Next(id, id) },
      close: fn(id) {
        ledger_close(id)
        panic as "close failed"
      },
    )
  let stream =
    datastream.zip(counted(5), datastream.take(failing_close, 1))
    |> datastream.map(fn(pair) { pair.0 })
  assert rescue(fn() { datastream.to_list(stream) }) == Error(Nil)
  assert_closed_once(2)
}

pub fn nested_terminal_raise_closes_only_its_resources_test() {
  ledger_reset()
  let outer =
    counted(3)
    |> datastream.map(fn(value) {
      let inner =
        rescue(fn() { counted(4) |> explode_at(1) |> datastream.count })
      assert inner == Error(Nil)
      value
    })
  assert datastream.to_list(outer) == [0, 1, 2]
  assert_closed_once(4)
}

pub fn broadcast_consumer_raise_keeps_other_consumer_running_test() {
  ledger_reset()
  let assert [left, right] = datastream.broadcast(counted(6), 2)
  assert rescue(fn() { left |> explode_at(2) |> datastream.to_list })
    == Error(Nil)
  // The raising consumer is released, so the other one reads everything
  // and the shared upstream closes once, at its end.
  assert datastream.to_list(right) == [0, 1, 2, 3, 4, 5]
  assert_closed_once(1)
}

pub fn bounded_broadcast_consumer_raise_releases_its_queue_test() {
  ledger_reset()
  let assert Ok([left, right]) =
    datastream.broadcast_bounded(counted(10), 2, max_queue: 2)
  assert rescue(fn() {
      left
      |> datastream.map(fn(element) {
        case element {
          Ok(1) -> panic as "consumer"
          _ -> element
        }
      })
      |> datastream.to_list
    })
    == Error(Nil)
  assert datastream.to_list(right)
    == list.map([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], Ok)
  assert_closed_once(1)
}

pub fn broadcast_upstream_raise_closes_upstream_and_ends_others_test() {
  ledger_reset()
  let stream =
    datastream.resource(
      open: fn() { #(ledger_open(), 0) },
      next: fn(state) {
        case state.1 {
          2 -> panic as "upstream"
          index -> Next(index, #(state.0, index + 1))
        }
      },
      close: fn(state) { ledger_close(state.0) },
    )
  let assert [left, right] = datastream.broadcast(stream, 2)
  assert rescue(fn() { datastream.to_list(left) }) == Error(Nil)
  assert datastream.to_list(right) == [0, 1]
  assert_closed_once(1)
}

// --- random pipelines -------------------------------------------------------

fn next_seed(seed: Int) -> Int {
  { seed * 48_271 } % 2_147_483_647
}

fn pick(seed: Int, below: Int) -> #(Int, Int) {
  let seed = next_seed(seed)
  #(seed, { seed / 65_536 } % below)
}

fn pipeline(seed: Int, depth: Int, raising: Bool) -> #(Int, Stream(Int)) {
  let #(seed, operation) = case depth {
    0 -> #(seed, 0)
    _ -> pick(seed, 16)
  }
  let #(seed, k) = pick(seed, 5)
  let child = fn(seed) { pipeline(seed, depth - 1, raising) }
  case operation {
    0 -> #(seed, counted(k))
    1 -> {
      let #(seed, s) = child(seed)
      #(seed, datastream.map(s, fn(x) { x + 1 }))
    }
    2 -> {
      let #(seed, s) = child(seed)
      #(seed, datastream.filter(s, int.is_even))
    }
    3 -> {
      let #(seed, s) = child(seed)
      #(seed, datastream.take(s, k))
    }
    4 -> {
      let #(seed, s) = child(seed)
      #(seed, datastream.drop(s, k))
    }
    5 -> {
      let #(seed, a) = child(seed)
      let #(seed, b) = child(seed)
      #(seed, datastream.append(a, b))
    }
    6 -> {
      let #(seed, a) = child(seed)
      let #(seed, b) = child(seed)
      #(seed, datastream.zip_with(a, b, int.add))
    }
    7 -> {
      let #(seed, s) = child(seed)
      #(seed, datastream.flat_map(s, fn(x) { counted(x % 3) }))
    }
    8 -> {
      let #(seed, s) = child(seed)
      let signal = datastream.map(counted(k + 2), fn(x) { x == k })
      #(seed, datastream.interrupt_when(s, signal))
    }
    9 -> {
      let #(seed, s) = child(seed)
      let assert Ok(s) = datastream.buffer(s, k + 1)
      #(seed, s)
    }
    10 -> {
      let #(seed, s) = child(seed)
      let assert Ok(s) = datastream.chunks_of(s, k + 1)
      #(seed, datastream.map(s, int.sum))
    }
    11 -> {
      let #(seed, s) = child(seed)
      #(seed, datastream.take_while(s, fn(x) { x < k }))
    }
    12 -> {
      let #(seed, a) = child(seed)
      let #(seed, b) = child(seed)
      #(seed, datastream.concat([a, b]))
    }
    13 -> {
      let #(seed, s) = child(seed)
      let assert [x, y] = datastream.broadcast(s, 2)
      #(seed, datastream.zip_with(x, y, int.add))
    }
    14 -> {
      let #(seed, s) = child(seed)
      #(seed, datastream.intersperse(s, 9))
    }
    _ -> {
      let #(seed, s) = child(seed)
      case raising {
        True -> #(seed, explode_at(s, k))
        False -> #(seed, datastream.scan(s, 0, int.add))
      }
    }
  }
}

fn consume(seed: Int, stream: Stream(Int)) -> Int {
  let #(_, terminal) = pick(seed, 7)
  case terminal {
    0 -> datastream.to_list(stream) |> list.length
    1 -> datastream.first(stream) |> result.unwrap(0)
    2 -> datastream.find(stream, fn(x) { x == 2 }) |> result.unwrap(0)
    3 -> bool_to_int(datastream.any(stream, fn(x) { x == 3 }))
    4 -> bool_to_int(datastream.all(stream, fn(x) { x < 4 }))
    5 -> datastream.count(stream)
    _ -> datastream.take(stream, 2) |> datastream.to_list |> list.length
  }
}

fn bool_to_int(value: Bool) -> Int {
  case value {
    True -> 1
    False -> 0
  }
}

pub fn random_pipelines_close_every_resource_once_test() {
  let raised =
    datastream.range(from: 1, to: 601)
    |> datastream.to_list
    |> list.fold(0, fn(raised, case_id) {
      ledger_reset()
      let #(seed, stream) = pipeline(case_id * 7, 4, False)
      let _consumed = consume(seed, stream)
      let #(opened, _) = ledger_report()
      assert_closed_once(opened)

      ledger_reset()
      let #(seed, stream) = pipeline(case_id * 7, 4, True)
      let outcome = rescue(fn() { consume(seed, stream) })
      let #(opened, _) = ledger_report()
      assert_closed_once(opened)
      case outcome {
        Ok(_) -> raised
        Error(_) -> raised + 1
      }
    })
  // The generator must exercise the raising path.
  assert raised > 20
}
