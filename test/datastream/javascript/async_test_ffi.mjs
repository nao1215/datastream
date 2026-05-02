function fail(message) {
  throw new Error(message);
}

export async function assert_async_iterable_equals(iterable, expected) {
  const actual = [];
  for await (const value of iterable) {
    actual.push(value);
  }

  const wanted = Array.from(expected);
  if (actual.length !== wanted.length) {
    fail(`expected ${wanted.length} items, got ${actual.length}`);
  }

  for (let index = 0; index < wanted.length; index += 1) {
    if (actual[index] !== wanted[index]) {
      fail(`mismatch at ${index}: expected ${wanted[index]}, got ${actual[index]}`);
    }
  }

  const done = await iterable.next();
  if (!done.done) {
    fail("expected iterator to stay done after exhaustion");
  }
}

export async function assert_exhaustion_closes_once(iterable, close_count) {
  for await (const _value of iterable) {
    // Drain completely.
  }

  if (close_count() !== 1) {
    fail(`expected close count 1 after exhaustion, got ${close_count()}`);
  }
}

export async function assert_return_closes_once(iterable, close_count) {
  const iterator = iterable[Symbol.asyncIterator]();
  await iterator.next();
  await iterator.next();
  await iterator.return();
  await iterator.return();

  if (close_count() !== 1) {
    fail(`expected close count 1 after early return, got ${close_count()}`);
  }

  const done = await iterator.next();
  if (!done.done) {
    fail("expected iterator to stay done after return()");
  }
}
