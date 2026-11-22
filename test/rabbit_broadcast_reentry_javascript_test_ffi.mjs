function equal(actual, expected, context) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${context}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

export async function assert_active_queued_pop(factory, readValue) {
  const history = [];
  const nested = [];
  const [first, second] = factory(index => {
    if (index === 1 || index === 2) nested.push(second.next());
  }, event => history.push(event));
  const firstValues = [];
  for await (const value of first) firstValues.push(readValue(value));
  equal(firstValues, [0, 1, 2], "outer consumer never overflows a freed queue slot");
  const nestedValues = [];
  for (const pending of nested) {
    const step = await pending;
    equal(step.done, false, "nested queued pull remains open");
    nestedValues.push(readValue(step.value));
  }
  equal(nestedValues, [0, 1], "queued values consumed exactly once");
  const tail = [];
  for await (const value of second) tail.push(readValue(value));
  equal(tail, [2], "active sibling still receives the next fan-out");
  const expectedHistory = ["open", "pull:0", "pull:1", "pull:2", "pull:3", "close:3"];
  equal(history, expectedHistory, "single source run and natural close");
  equal(await first.next(), { done: true }, "first stays finished");
  equal(await second.next(), { done: true }, "second stays finished");
  await first.return();
  await second.return();
  equal(history, expectedHistory, "no additional source pull or close");
}
