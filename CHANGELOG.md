# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- **source.range** doc-comment now spells out that the function is
  stop-EXCLUSIVE and explicitly contrasts it with `gleam/yielder.range`
  which is stop-INCLUSIVE. Adds runnable examples and a note on the
  off-by-one when porting from `gleam/yielder`. The previous wording
  ("keeping `range` aligned with the surrounding stdlib's `range`")
  was true for `gleam/int.range` (fold-style, stop-exclusive) but
  misleading for users who expected `gleam/yielder.range` semantics
  since `Stream(a)` is the closer analog of `Yielder(a)`. Behaviour
  unchanged. (#181)

## [0.6.0] - 2026-04-29

### Added

- **stream.buffer(stream, prefetch:)** is a new combinator that
  pulls `prefetch` elements ahead from the upstream and yields them
  to the consumer at its pace. The internal queue is refilled back
  to capacity after every consumer pull, so a latency-bound
  upstream (HTTP body bytes, slow disk reads) no longer blocks the
  consumer's per-element work serially — the next upstream pull can
  overlap with the current downstream computation. Capacity must
  be `>= 1`; element type, order, and cardinality are preserved.
  Pure-functional pull-state implementation with no `gleam_erlang`
  / process / FFI dependency, so it runs on both the Erlang and
  JavaScript targets. First sub-task of the Roadmap in #171. (#172)
- **stream.interrupt_when(stream, signal:)** is a new combinator
  that terminates `stream` on the first `True` element from
  `signal`. The signal is `Stream(Bool)` (not `Stream(Nil)` as the
  Roadmap originally proposed) — the boolean carries the
  not-yet / fire-now distinction that `Done` cannot in a pull-based
  model where `Done` is terminal. On every consumer pull the signal
  is checked first: a `True` element closes both the rest of the
  signal and `stream` and yields `Done`; a `False` element is
  consumed and the pull descends into `stream`; a `Done` from the
  signal switches the combinator into a pass-through over `stream`
  (the signal is never re-pulled, matching the `Step` contract).
  Counterpart of `take_while` for *external* termination signals
  (timer expiry, parent-shutdown probe, cancel-token bridge) where
  the decision to stop doesn't depend on the pulled element.
  Cross-target: pure-functional pull-state, no `gleam_erlang` /
  process / FFI dependency. Second sub-task of the Roadmap in
  #171. (#174)
- **stream.broadcast(stream, n)** is a new fan-out combinator: it
  splits a `Stream(a)` into `n` independent consumer streams that
  share a single underlying source. Each consumer pulls at its own
  pace; an element produced by the source is observed by every
  still-alive consumer in order. Use it for the observability-tap
  shape (one consumer drives the main pipeline, another writes
  metrics or logs) without forcing the source to be re-evaluated,
  which matters for resource-backed and otherwise non-replayable
  sources. `n` must be `>= 1`; per-consumer queue is currently
  unbounded — bounded variant with an explicit drop policy is
  follow-up work. Cross-target via the new `internal/ref` mutable
  cell (process-dictionary on Erlang, plain object on JavaScript).
  Third sub-task of the Roadmap in #171. (#176)
- **stream.unzip(stream)** splits a `Stream(#(a, b))` into a pair
  `#(Stream(a), Stream(b))`, the counterpart of `stream.zip`. Built
  on `broadcast` under the hood, so the same per-consumer
  unbounded-queue caveat applies (a pipeline that drives one half
  to completion while the other half lags accumulates the lagging
  half's queue). Cross-target. Fourth sub-task of the Roadmap
  in #171. (#177)
- **datastream/erlang/source.bridge_subject_stream** is a
  workaround helper for the `from_subject` × `par.*` / `time.*` /
  `source.timeout` incompatibility documented on `from_subject`.
  Materialises the upstream into a `List(a)` inside the calling
  (subject-owning) process and reopens it as a process-safe
  list-backed stream, which can then be composed with `par.*` /
  `time.timeout` / `source.timeout` without triggering the BEAM
  owner-only receive constraint. Caveat: buffers the entire
  upstream in memory, so only suitable for bounded subjects.
  BEAM-only. Closes the last open Roadmap follow-up in #171. (#178)

### Internal

- **datastream/internal/ref** is a small cross-target mutable cell
  (`new(value)` / `get(ref)` / `set(ref, value)`). Process-
  dictionary-backed on Erlang via `src/datastream_ref_ffi.erl` and
  one-field-object-backed on JavaScript via
  `src/datastream/internal/ref_ffi.mjs`. Internal to the library
  — `internal_modules = ["datastream/internal/**"]` keeps it out
  of the public surface and the generated docs. Required by
  `broadcast` and `unzip` to share upstream and per-consumer queue
  state across multiple consumer streams; pure-functional pull-
  state cannot express that observation across separate stream
  values without a mutable bridge.

## [0.5.0] - 2026-04-28

### Added

- **fold.to_string**, **fold.to_string_tree**, and **fold.to_bit_array**
  are new terminal folds that materialise a `Stream(String)` /
  `Stream(BitArray)` into a single concatenated value without going
  through `to_list |> string.concat`. They accumulate into a
  `StringTree` / `BytesTree` internally (Erlang `iolist_to_binary`
  on the BEAM, incremental string concatenation on JavaScript), so
  cost is `O(total length)` rather than the `O(n²)` shape of
  `to_list`-then-concat. The shape is the natural completion of a
  text- or byte-output pipeline (NDJSON / CSV export, log
  aggregation, HTTP response bodies). `to_string_tree` is provided
  for callers who want to keep building the tree before producing
  the final `String`. (#168)

## [0.4.0] - 2026-04-28

### Added

- **text.records** groups a `Stream(String)` of lines into a
  `Stream(List(String))` of records separated by blank lines —
  the framing used by Server-Sent Events (SSE), blank-line-separated
  NDJSON variants, mbox / RFC 822 envelopes, and ad-hoc text wire
  formats. Pair with `text.lines` for an end-to-end record decoder
  (`stream |> text.lines |> text.records |> stream.map(parse_event)`).
  Multiple consecutive blank lines collapse to a single separator
  (no empty records, no spurious leading record), and a trailing
  record without a terminating blank line is still emitted so the
  last record is never silently dropped. The previous workaround —
  `string.split("\n\n")` over a buffered `String` — required the
  entire payload up-front; this primitive runs incrementally over
  any chunked source. (#165)

## [0.3.0] - 2026-04-27

### Fixed

- **text.lines** no longer loops infinitely when a source emits
  consecutive empty-string chunks while a `\r` is pending (waiting to
  see if the next character is `\n`). Empty chunks are now skipped in
  that state. (#157, #160)
- **binary.length_prefixed** now rejects frames whose prefix claims
  more bytes than `max_frame_size` (default 16 MiB) immediately with
  `Error(FrameTooLarge(claimed:, max:))`, preventing unbounded memory
  buffering on corrupt or malicious input. A new
  `length_prefixed_with` function accepts an explicit limit. (#158,
  #161)
- **par.map_unordered** and **par.map_ordered** no longer deadlock
  when a worker function panics. Workers are now monitored via
  `process.monitor`; abnormal exits halt the stream with `Done`
  instead of blocking forever. (#156, #162)

### Added

- `binary.FrameTooLarge(claimed:, max:)` variant added to
  `IncompleteFrame` type.
- `binary.length_prefixed_with(over:, prefix_size:, max_frame_size:)`
  for explicit frame-size limits.

### Documentation

- Add "Compatibility with web frameworks" section to README listing
  minimum compatible versions of wisp (≥ 2.0.0), mist (≥ 6.0.0),
  and gleam_httpc (≥ 5.0.0). (#153, #159)
- Document the self-close-on-Done contract in the README Semantics
  section and in `flat_map` / `append` doc comments so users
  understand when `close` is and isn't called. (#163)

## [0.2.0] - 2026-04-25

### Changed

- **BREAKING**: `source.from_bit_array` now panics at construction
  time when the input `BitArray` is not byte-aligned (its bit length
  is not a multiple of 8) instead of silently dropping the trailing
  sub-byte tail. This matches the existing
  `binary.length_prefixed` / `binary.fixed_size` policy of crashing
  on programmer error so callers cannot quietly trust corrupted
  input. (#144)
- **BREAKING**: `binary.length_prefixed` now returns
  `Stream(Result(BitArray, IncompleteFrame))` instead of
  `Stream(BitArray)`. Well-formed frames surface as `Ok(frame)`. When
  the input ends mid-frame (short prefix or short payload), the
  stream emits exactly one
  `Error(IncompleteFrame(expected:, got:))` element before halting
  so callers can distinguish "no more frames" from "truncated final
  frame" — the previous silent-halt behaviour was unsafe for any
  framed protocol that must reject partial messages. The
  `Stream(Result(_, _))` shape mirrors `text.utf8_decode`. (#147)

- **BREAKING**: `stream.take`, `stream.drop`, and `stream.chunks_of`
  now panic at construction time on programmer-error arguments
  instead of silently normalising. Specifically: `take(_, n)` and
  `drop(_, n)` panic when `n < 0` (the `0` case still yields the
  empty stream / the identity respectively, since both are
  mathematically natural); `chunks_of(_, size)` panics when
  `size < 1` (the previous "normalised to 1" behaviour was hiding
  the real bug). This unifies the library's invalid-argument policy
  with `binary.fixed_size` / `binary.length_prefixed` so a user who
  learns the rule for one function can predict the rest. The new
  contract is documented in the `datastream` module-level
  docstring. (#145)

### Added

- `binary.IncompleteFrame(expected:, got:)` type, surfaced by
  `binary.length_prefixed` on truncated input.
- `datastream` module-level "Invalid-argument policy" section
  documenting the unified `panic`-on-bad-arg contract referenced
  from per-function docstrings.

### Documentation

- `text.lines` opening sentence now lists `\n`, `\r\n`, and lone
  `\r` as terminators (matching the actual behaviour and Python's
  `str.splitlines()`). The chunk-boundary paragraph already
  described this; only the opening summary was out of date. (#146)

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
