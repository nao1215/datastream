# Testing

`just ci` runs everything below except the nightly job and mutation testing. Tests run on the Erlang and JavaScript targets, on the Gleam floor (1.14.0, OTP 27, Node 22) and on the latest Gleam 1.x. The BEAM-only modules (`datastream/erlang/*`) are tested on Erlang only.

## Examples and unit tests

Every module has a test file under `test/datastream/`: elements and order for each operator, the close-exactly-once contract of resource-backed streams (`resource_test.gleam`, `try_resource_test.gleam`), the panics and `*_checked` errors for invalid arguments, and the smallest valid argument of each (`fixed_size(size: 1)`, `chunks_of(into: 1)`, a frame of exactly `max_frame_size`). `volume_test.gleam` pulls long streams to catch stack growth and quadratic buffering. The pipelines in `test/examples/` are the README examples, run as tests.

## Generated properties

`test/datastream_metamon_test.gleam` uses metamon to compare the operators with `gleam/list`: `map`, `filter`, `take`, `drop`, `append` and the folds must give what the list functions give for random lists. `test/datastream_chunking_test.gleam` also compares `text.split` with `string.split` and `string.to_graphemes`, and checks that UTF-8 encode then decode returns the input.

## Metamorphic relations

`test/datastream_chunking_test.gleam` checks the parsers whose exact output for a random input cannot be written down without re-implementing them: `text.lines`, `text.records`, `text.split` (one character, several characters, graphemes), `text.utf8_decode`, `binary.bytes`, `binary.delimited` (one and two bytes), `binary.fixed_size` and `binary.length_prefixed` (one- and two-byte prefixes). The same input is fed once as a single chunk, once cut at random places and once with empty chunks between the pieces; every run must yield the same elements. The text generator mixes every line terminator, the delimiters, a multi-byte character, an emoji and a combining accent, so cuts land inside `\r\n` and between a letter and its accent.

This relation found two bugs: `text.split(on: "")` split a grapheme that spanned two chunks, and `text.utf8_decode` turned the valid text around an encoded surrogate or overlong sequence into one error. One limitation is left out on purpose: on Erlang a delimiter followed by a combining mark (`",\u{0301}"`) is one grapheme, so `text.split(on: ",")` does not split there inside one chunk but does at a chunk boundary.

## Nightly runs

`.github/workflows/nightly-property.yml` runs the whole suite ten times with `METAMON_RUNS_MULTIPLIER=10`, each pass with a new seed. A failure report prints its seed, so the case can be replayed with `metamon.with_seed` and kept as a regression test.

## Mutation testing

There is no mutation testing tool for Gleam, so `gleam_mutate.py` (from the mutation-testing lab in nao1215/debimate) is run by hand on a copy of the project. It rewrites one operator or boolean result at a time and runs `gleam test` on the Erlang target; a mutant that makes the suite hang past 60 seconds counts as killed. For this run the script also mutated integer comparisons and `+` / `-`, which carry most of the decisions in this library. The score is killed / (killed + lived).

| Date | Files | Killed | Lived | Score |
|---|---|---:|---:|---:|
| 2026-11-19, first run | every module under `src/` with mutants (116) | 72 | 44 | 62.1% |
| 2026-11-19, after the boundary tests | same | 89 | 27 | 76.7% |

The first run showed that no test used the smallest valid argument (`fixed_size(size: 1)`, `chunks_of(into: 1)`, `buffer_checked(prefetch: 1)`, `drop_checked(up_to: 0)`, `broadcast_bounded(into: 1, max_queue: 1)`, one worker and `max_buffer == max_workers` in `par`), a frame of exactly `max_frame_size`, a queue of exactly `max_queue`, or the first and last UTF-8 lead bytes (0xC2, 0xF5). Tests for each now kill those mutants. The survivors, grouped:

- `erlang/time` (7): deadlines one millisecond earlier or later in `throttle`, `rate_limit` and `ms_until`. The tests use the wall clock, so a 1 ms shift is not observable without a fake clock.
- `erlang/par` (9): the worker and buffer limits in the dispatch loop (`>=` against `>`), which only change how many workers run at once, and the loser cleanup in `race`.
- Equivalent mutants (6): `take` and `drop` treat `n < 0` and `n <= 0` the same, `source.range` handles `start == stop` before the `<` check, `list_at_bool` is never called past the end, and `delimited` rescans a few bytes earlier (`resume_scan_offset`, `find_delimiter`) with the same result.
- `stream.buffer` (1): prefetching one element more than `capacity` is not visible in the output; only a pull counter would see it.
- `broadcast` consumer ids (1) and `delimited`'s `has_seen_input` flag (1): the observable output did not change for any test input; they need a closer look.
- `length_prefixed` (1): `prefix_size - length` instead of `+` takes a branch that still yields the same frames for the tested inputs.
