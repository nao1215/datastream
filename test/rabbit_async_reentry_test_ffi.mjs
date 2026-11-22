function equal(actual, expected, context) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${context}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

async function stays_closed(iterator, pulls, closes, expectedPulls, expectedCloses) {
  equal(await iterator.next(), { done: true }, "next after return");
  equal(await iterator.return(), { done: true }, "second return");
  equal(await iterator.return(), { done: true }, "third return");
  equal(pulls, expectedPulls, "no pull after return");
  equal(closes, expectedCloses, "no repeated close");
}

export async function assert_reentrant_return(factory, stopAt, finishes) {
  const pulls = [];
  const closes = [];
  const returns = [];
  const iterator = factory(
    state => {
      pulls.push(state);
      if (state === stopAt) returns.push(iterator.return());
    },
    state => closes.push(state),
  )[Symbol.asyncIterator]();
  for (let state = 0; state < stopAt; state++) {
    equal(await iterator.next(), { done: false, value: state }, "preceding next");
  }
  const outer = await iterator.next();
  equal(returns.length, 1, "return invoked from resource next");
  equal(await returns[0], { done: true }, "reentrant return result");
  const expectedCloses = [finishes ? stopAt : stopAt + 1];
  equal(closes, expectedCloses, "latest state closed exactly once");
  equal(outer, { done: true }, "in-flight next after reentrant return");
  await stays_closed(iterator, pulls, closes,
    Array.from({ length: stopAt + 1 }, (_, i) => i), expectedCloses);
}

export async function assert_between_pulls(factory) {
  const pulls = [];
  const closes = [];
  const iterator = factory(state => pulls.push(state), state => closes.push(state));
  equal(await iterator.next(), { done: false, value: 0 }, "first next");
  const returned = iterator.return();
  equal(closes, [1], "return closes before its promise settles");
  equal(await returned, { done: true }, "return result");
  await stays_closed(iterator, pulls, closes, [0], [1]);
}

export async function assert_nested_next(factory) {
  const pulls = [];
  const closes = [];
  let attempted = false;
  let nested;
  let thrownSynchronously;
  const iterator = factory(state => {
    pulls.push(state);
    if (!attempted) {
      attempted = true;
      try {
        nested = iterator.next();
      } catch (error) {
        thrownSynchronously = error;
      }
    }
  }, state => closes.push(state));
  const outer = await iterator.next();
  if (thrownSynchronously) throw new Error("nested next must return a rejected promise, not throw synchronously");
  if (!(nested instanceof Promise)) throw new Error("nested next must return a promise");
  const [settled] = await Promise.allSettled([nested]);
  equal(pulls, [0], "nested next must not pull the source again");
  equal(settled.status, "rejected", "nested next promise");
  if (!(settled.reason instanceof TypeError)) throw new Error("nested next must reject with TypeError");
  equal(outer, { done: false, value: 0 }, "outer next remains usable");
  equal(closes, [], "nested next rejection does not stop outer next");
  await iterator.return();
  equal(closes, [1], "cleanup after nested next rejection");
  await stays_closed(iterator, pulls, closes, [0], [1]);
}
