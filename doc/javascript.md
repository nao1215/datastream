# JavaScript adapter

`datastream.to_async_iterable` exposes a synchronous stream to JavaScript's `for await` loop. Both this function and the `AsyncIterable(a)` type are available only when compiling for JavaScript.

```gleam
pub type AsyncIterable(a)

pub fn to_async_iterable(stream: Stream(a)) -> AsyncIterable(a)
```

With callbacks that return normally, each call to the iterator's `next()` method pulls one element synchronously and returns a promise for the result. The adapter does not turn asynchronous input, such as a fetch response body, into a Gleam stream. Handle that input in JavaScript and pass completed values or batches into Gleam.

## Example

In a Gleam project named `app`, put this function in `src/app.gleam`:

```gleam
import datastream

pub fn lines_for_host() -> datastream.AsyncIterable(String) {
  datastream.from_list(["A\nB", "\nC\n"])
  |> datastream.lines
  |> datastream.to_async_iterable
}
```

Build with `gleam build --target javascript`, then consume the exported function from JavaScript:

```javascript
import { lines_for_host } from "./build/dev/javascript/app/app.mjs";

for await (const line of lines_for_host()) {
  console.log(line);
  if (line === "B") break;
}
```

This prints `A` and `B`. Breaking out of the loop calls the iterator's `return()` method, which closes the remaining stream. Calling `return()` before the first pull leaves a lazy resource unopened. Calling it again does nothing.

An adapter is a single-use iterator: calling `[Symbol.asyncIterator]()` returns the same iterator. Call `to_async_iterable` again to start another run of a replayable stream. Shared `broadcast`, `broadcast_bounded`, and `unzip` consumers remain single-use even when wrapped in a new adapter.

The adapter also implements `throw(error)`: it closes the stream and, when cleanup returns normally, returns a rejected promise. A thrown pull callback attempts cleanup before rejecting `next()`. Cleanup after a callback exception is best-effort. In particular, an exception from `close` can make an iterator method throw synchronously and can replace an earlier error.
