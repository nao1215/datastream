import { Ok } from "../../gleam.mjs";

export function make_async_iterable(next, stop, failed) {
  let finished = false;
  let pulling = false;

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
      if (pulling) {
        return Promise.reject(new TypeError("Cannot call next() during a stream pull"));
      }

      pulling = true;
      try {
        const step = next();
        if (step instanceof Ok) {
          return Promise.resolve({ done: false, value: step[0] });
        }

        finished = true;
        return Promise.resolve(done_result());
      } catch (error) {
        finished = true;
        failed();
        return Promise.reject(error);
      } finally {
        pulling = false;
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
