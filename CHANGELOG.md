# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

_No changes yet._

## [0.1.1] - 2026-04-25

### Changed

- Release workflow pipes `I am not using semantic versioning` into
  `gleam publish -y` so the publish step does not stall on the
  interactive confirmation that Hex requires for non-semver releases.
- README adds Hex version, Hex download count, and CI status badges.

## [0.1.0] - 2026-04-25

### Added

#### Cross-target core

- `datastream`: opaque `Stream(a)` type and `Step(a, state)` pull-result
  enum. Internal constructors `unfold`, `make`, `resource`,
  `try_resource`, and the internal `pull` / `close` operations.
- `datastream/source`: stream constructors — `empty`, `once`,
  `from_list`, `from_bit_array`, `from_option`, `from_result`,
  `from_dict`, `range`, `repeat`, `iterate`, `unfold`. Resource-backed
  constructors `resource` and `try_resource` (with `ResourceError`)
  honouring the spec close contract on every termination path.
- `datastream/stream`: lazy combinators — `map`, `filter`, `take`,
  `drop`, `take_while`, `drop_while`, `append`, `filter_map`,
  `flat_map`, `flatten`, `concat`, `scan`, `map_accum`, `zip`,
  `zip_with`, `intersperse`, `tap`, `dedupe_adjacent`. Chunked
  operations `chunks_of` and `group_adjacent`.
- `datastream/fold`: pure terminals — `to_list`, `count`, `first`,
  `last`, `fold`, `reduce`, `drain`, `all`, `any`, `find`,
  `sum_int`, `sum_float`, `product_int`, `collect_result`,
  `partition_result`, `partition_map`.
- `datastream/sink`: effectful terminals — `each`, `try_each`,
  `println`.
- `datastream/chunk`: opaque `Chunk(a)` type with `empty`, `singleton`,
  `from_list`, `to_list`, `size`, `is_empty`, `map`, `filter`,
  `concat`.
- `datastream/text`: chunk-aware text helpers — `lines`, `split`,
  `utf8_decode`, `utf8_encode`.
- `datastream/binary`: chunk-aware byte framing — `bytes`,
  `length_prefixed`, `fixed_size`, `delimited`.

#### BEAM-only extensions (`@target(erlang)`)

- `datastream/erlang/source`: `from_subject` (Subject bridge with
  poll-based termination), `ticks`, `interval`, `timeout` (per-element
  deadline).
- `datastream/erlang/sink`: `into_subject`.
- `datastream/erlang/par`: bounded-concurrency combinators —
  `map_unordered` / `map_ordered` / `each_unordered` / `each_ordered`
  (each with a `_with` variant for tunable `max_workers` / `max_buffer`),
  `merge` / `merge_with`, and `race`. `max_buffer` enforces a real
  in-flight ceiling via per-combinator back-pressure (ack-gating for
  `merge`, dispatch caps for the `map_*` family). Public defaults
  `default_max_workers = 4` and `default_max_buffer = 16`.
- `datastream/erlang/time`: time-based combinators — `debounce`,
  `throttle`, `sample`, `rate_limit`, `window_time`.
- Internal `datastream/erlang/internal/pump` helper backing the BEAM
  extension worker lifecycle (handshake-spawned stop subjects, real
  `close` callbacks on extension streams).

#### Tooling and release

- GitHub Actions CI matrix running format / typecheck / lint
  (`glinter` with warnings-as-errors) plus `gleam test` on both
  Erlang and JavaScript targets.
- Release workflow that publishes to Hex on `v*` tag push and
  generates a GitHub Release body from the version-tagged CHANGELOG
  section.
- `justfile`, `.mise.toml`, Dependabot config, FUNDING file,
  `glinter` configuration.

#### Documentation

- README with status banner, install / quick start, "When to use"
  guidance, four runnable examples (basic pipeline, line-oriented
  text, binary framing, result-shaped streams), module guide, target
  support matrix, semantics summary.
- `CONTRIBUTING.md` and `SECURITY.md`.
- Module-level doc comments on every public module.

### Notes

- The `dataprep` integration is intentionally NOT shipped in this
  release. The core stays free of any validation-library dependency;
  callers who want `Validated`-shaped collection wire it up with their
  own short helper. A separate companion package may follow.
