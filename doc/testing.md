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

`.github/workflows/nightly-property.yml` runs the whole suite ten times with `METAMON_RUNS_MULTIPLIER=10`, each pass with a new seed. A failure report prints its seed, so the case can be replayed with `metamon.with_seed` and kept as a regression test.

## Mutation testing

There is no mutation testing tool for Gleam, so `gleam_mutate.py` (from the mutation-testing lab in nao1215/debimate) is run by hand on a copy of the project. It rewrites one operator or boolean result at a time and runs `gleam test` on the Erlang target; a mutant that makes the suite hang past 60 seconds counts as killed. For this run the script also mutated integer comparisons and `+` / `-`, which carry most of the decisions in this library. The score is killed / (killed + lived).

| Date | Files | Killed | Lived | Score |
|---|---|---:|---:|---:|
| 2026-11-19, first run | every module under `src/` with mutants (116) | 72 | 44 | 62.1% |
| 2026-11-19, after the boundary tests | same | 89 | 27 | 76.7% |

The names below refer to the API at the time of the recorded run. The first run showed that no test used the smallest valid argument (`fixed_size(size: 1)`, `chunks_of(into: 1)`, `buffer_checked(prefetch: 1)`, `drop_checked(up_to: 0)`, `broadcast_bounded(into: 1, max_queue: 1)`, one worker and `max_buffer == max_workers` in `par`), a frame of exactly `max_frame_size`, a queue of exactly `max_queue`, or the UTF-8 lead-byte boundaries (valid 0xC2 and rejected 0xF5). Tests for each now kill those mutants. The recorded survivor groups below account for 26 of the 27 survivors; the historical notes do not identify the remaining mutant:

- `erlang/time` (7): deadlines one millisecond earlier or later in `throttle`, `rate_limit` and `ms_until`. The tests use the wall clock, so a 1 ms shift is not observable without a fake clock.
- `erlang/par` (9): the worker and buffer limits in the dispatch loop (`>=` against `>`), which only change how many workers run at once, and the loser cleanup in `race`.
- Equivalent mutants (6): `take` and `drop` treat `n < 0` and `n <= 0` the same, `source.range` handles `start == stop` before the `<` check, `list_at_bool` is never called past the end, and `delimited` rescans a few bytes earlier (`resume_scan_offset`, `find_delimiter`) with the same result.
- `stream.buffer` (1): prefetching one element more than `capacity` is not visible in the output; only a pull counter would see it.
- `broadcast` consumer ids (1) and `delimited`'s `has_seen_input` flag (1): the observable output did not change for any test input; they need a closer look.
- `length_prefixed` (1): `prefix_size - length` instead of `+` takes a branch that still yields the same frames for the tested inputs.
