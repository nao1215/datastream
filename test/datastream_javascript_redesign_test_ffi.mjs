function equal(actual, expected, context) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${context}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

async function remains_done(iterator, history, expected) {
  equal(await iterator.next(), { done: true }, "next after finish");
  equal(await iterator.return(), { done: true }, "return after finish");
  equal(await iterator.return(), { done: true }, "repeated return");
  equal(Array.from(history()), expected, "history after repeated termination");
}

export async function assert_break(iterable, history) {
  const iterator = iterable[Symbol.asyncIterator]();
  const seen = [];
  for await (const value of iterator) {
    seen.push(value);
    if (seen.length === 2) break;
  }
  equal(seen, [0, 1], "values before break");
  const expected = ["open", "pull:0", "pull:1", "close:2"];
  equal(Array.from(history()), expected, "history at break");
  await remains_done(iterator, history, expected);
}

export async function assert_throw(iterable, history) {
  const iterator = iterable[Symbol.asyncIterator]();
  equal(await iterator.next(), { done: false, value: 0 }, "first next");
  equal(await iterator.next(), { done: false, value: 1 }, "second next");
  const failure = new Error("consumer requested iterator.throw");
  let rejected;
  try {
    await iterator.throw(failure);
  } catch (error) {
    rejected = error;
  }
  if (rejected !== failure) throw new Error("throw must reject with its supplied error");
  const expected = ["open", "pull:0", "pull:1", "close:2"];
  equal(Array.from(history()), expected, "history after throw");
  await remains_done(iterator, history, expected);
}

export async function assert_loop_throw(iterable, history) {
  const iterator = iterable[Symbol.asyncIterator]();
  const failure = new Error("consumer loop failed");
  let rejected;
  const seen = [];
  try {
    for await (const value of iterator) {
      seen.push(value);
      throw failure;
    }
  } catch (error) {
    rejected = error;
  }
  if (rejected !== failure) throw new Error("consumer loop must preserve its own error");
  equal(seen, [0], "values before loop throws");
  const expected = ["open", "pull:0", "close:1"];
  equal(Array.from(history()), expected, "history after loop throws");
  await remains_done(iterator, history, expected);
}

export async function assert_unstarted_return(iterable, history) {
  const iterator = iterable[Symbol.asyncIterator]();
  equal(Array.from(history()), [], "history before first next");
  equal(await iterator.return(), { done: true }, "unstarted return");
  await remains_done(iterator, history, []);
}

// `raising` throws while the stream produces the element at index `at`.
export async function assert_callback_raise(iterable, history, at, gleam_expected) {
  const expected = Array.from(gleam_expected);
  const iterator = iterable[Symbol.asyncIterator]();
  for (let index = 0; index < at; index++) {
    equal(await iterator.next(), { done: false, value: index }, `next ${index}`);
  }
  let rejected = false;
  try {
    await iterator.next();
  } catch (_) {
    rejected = true;
  }
  if (!rejected) throw new Error("a raising callback must reject next()");
  equal(Array.from(history()), expected, "history after callback raise");
  await remains_done(iterator, history, expected);
}

// The resource's close throws. Iterator methods must still return
// promises, and `throw(error)` must reject with the supplied error.
export async function assert_close_error(make) {
  const returned = make()[Symbol.asyncIterator]();
  equal(await returned.next(), { done: false, value: 0 }, "next before return");
  let promise;
  try {
    promise = returned.return();
  } catch (_) {
    throw new Error("return() must not throw synchronously");
  }
  let rejected = false;
  try {
    await promise;
  } catch (_) {
    rejected = true;
  }
  if (!rejected) throw new Error("return() must reject when close throws");
  equal(await returned.next(), { done: true }, "next after failed return");

  const thrown = make()[Symbol.asyncIterator]();
  equal(await thrown.next(), { done: false, value: 0 }, "next before throw");
  const failure = new Error("consumer failure");
  try {
    promise = thrown.throw(failure);
  } catch (_) {
    throw new Error("throw() must not throw synchronously");
  }
  let reason;
  try {
    await promise;
  } catch (error) {
    reason = error;
  }
  if (reason !== failure) throw new Error("throw() must reject with the supplied error");
}
