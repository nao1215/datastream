import { Ok } from "../../gleam.mjs";

export function make_async_iterable(next, stop) {
  let finished = false;

  function done_result() {
    return { done: true, value: undefined };
  }

  function finish() {
    if (!finished) {
      finished = true;
      stop();
    }
    return done_result();
  }

  return {
    [Symbol.asyncIterator]() {
      return this;
    },

    next() {
      if (finished) {
        return Promise.resolve(done_result());
      }

      try {
        const step = next();
        if (step instanceof Ok) {
          return Promise.resolve({ done: false, value: step[0] });
        }

        finished = true;
        return Promise.resolve(done_result());
      } catch (error) {
        finish();
        return Promise.reject(error);
      }
    },

    return() {
      return Promise.resolve(finish());
    },

    throw(error) {
      finish();
      return Promise.reject(error);
    },
  };
}
