# Testing

`just ci` runs formatting, type checking, linting, builds, and tests on both targets. CI also tests the Gleam floor (1.14.0, OTP 27, Node 22) alongside the latest Gleam 1.x. Erlang-only operations have tests guarded with `@target(erlang)`.

## Examples and unit tests

Tests under `test/datastream/` cover element order, resource cleanup on completion and early termination, typed argument errors, and boundary values such as `fixed_size(size: 1)`, `chunks_of(into: 1)`, and a frame of exactly `max_frame_size`. Resource tests record opening, pulls, and the state passed to close. `volume_test.gleam` pulls long streams to catch stack growth and quadratic buffering. The pipelines linked from the README run as tests in `test/examples/`.

`datastream_redesign_test.gleam` checks whole-chunk binary alignment rejection, fail-fast cleanup, bounded broadcast overflow, and consumer cancellation. The Erlang redesign tests coordinate workers with explicit gates to verify parallel ordering and completion. The JavaScript redesign tests run host `for await` loops and check `break`, `throw`, and repeated iterator closure against resource event logs.

## Generated properties

`test/datastream_metamon_test.gleam` uses metamon to compare the operators with `gleam/list`: `map`, `filter`, `take`, `drop`, `append` and the folds must give what the list functions give for random lists. `test/datastream_chunking_test.gleam` also compares `split` with `string.split` and `string.to_graphemes`, and checks that UTF-8 encode then decode returns the input.

## Metamorphic relations

`test/datastream_chunking_test.gleam` checks the parsers whose exact output for a random input cannot be written down without re-implementing them: `lines`, `records`, `split` (one character, several characters, graphemes), `utf8_decode`, `bytes`, `delimited` (one and two bytes), `fixed_size` and `length_prefixed` (one- and two-byte prefixes). The same input is fed once as a single chunk, once cut at random places and once with empty chunks between the pieces; every run must yield the same elements, except for UTF-8 decoding. That check compares concatenated valid text and error counts because decoded string boundaries may vary. The text generator mixes every line terminator, the delimiters, a multi-byte character, an emoji and a combining accent, so cuts land inside `\r\n` and between a letter and its accent.

This relation found three bugs: `split(on: "")` split a grapheme that spanned two chunks, `utf8_decode` turned the valid text around an encoded surrogate or overlong sequence into one error, and on Erlang `split(on: ",")` matched whole graphemes, so a delimiter followed by a combining mark was split only when a chunk boundary fell between them. `test/text_chunking_test.gleam` compares `split` with a code point oracle and checks that a grapheme of 30,000 code points delivered one code point per chunk is segmented in linear time.

## Cleanup when a callback raises

`test/exception_cleanup_test.gleam` raises from `map`, terminal callbacks, resource callbacks, and `close` itself, and checks that every opened resource is closed exactly once at its latest state. A generator builds 600 random pipelines from resources, combinators, `broadcast`, and terminals, with and without a raising stage. `test/exception_cleanup_erlang_test.gleam` checks that workers started by `merge`, `race`, `timeout`, and `map_ordered` stop and close their sources when the consumer raises, and that a source raising inside a worker is closed before the worker exits.

## Nightly runs

`.github/workflows/nightly-property.yml` runs the whole suite ten times with `METAMON_RUNS_MULTIPLIER=10`, each pass with a new seed. A failure report prints its seed, so the case can be replayed with `metamon.Config(..metamon.default_config(), seed: Some(seed))` and kept as a regression test.

## Mutation testing

`gleam run -m metamon/mutation` changes one operator, `!` or boolean literal in `src/datastream.gleam` at a time and runs `gleam test --target erlang` on each change. `.github/workflows/mutation.yml` mutates the module on every pull request that changes it and fails on a survivor that `doc/mutation/baseline.json` does not list; the weekly run does the same.

The first run (2026-11-27) scored 85.4%. Tests were then added for the read-ahead of `buffer` (it pulls exactly `prefetch` elements before the first one is returned) and for `records`, `split(on: "")`, `map_ordered` and `map_unordered` not pulling a source that has ended. Three more runs gave 88.7%, 86.0% and the baseline below: the suite has tests that depend on timing, so a mutant can live in one run and be killed in another. The baseline therefore lists every mutant that lived in any of the runs after the new tests, 24 of the 150 that compile (84.0%), so the gate does not fail on a timing change it has seen before.

The survivors are grouped by cause. Timing (10): the deadlines and windows of `sample`, `throttle`, `rate_limit` and `window`, and the wait in `time_milliseconds_until`, where moving a boundary by one millisecond is not observable with the clock the tests use. The in-flight bound of `map_ordered` and `map_unordered` (2): the dispatch check `busy + finished >= max_buffer`, which a test can only pin down by controlling when tasks finish; it is the next test to write. Equivalent (9): the chunk bookkeeping of `delimited`, `split`, `binary_take_bytes` and the read-behind of `keep_newest`, which only changes how much is rescanned or concatenated; the upper word of an 8-byte length prefix, which is only read after 4 GiB are buffered; `range` and `drop` at their equal bounds; and the `active` default of a broadcast consumer that always exists. The rest (3), in `once`, `take` and `lines`, were killed in two of the three runs and lived in the third, when the machine was under load.

<!-- mutation:start -->
| Module | Killed | Lived | Timed out | Score |
|---|---:|---:|---:|---:|
| `datastream` | 126 | 24 | 0 | 84.0% |
| all | 126 | 24 | 0 | 84.0% |
<!-- mutation:end -->
