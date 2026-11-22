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
