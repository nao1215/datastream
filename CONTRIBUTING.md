# Contributing to datastream

Thanks for your interest in contributing. This document covers the
local development setup, the test commands you will use day-to-day,
and the expectations for changes to public API.

## Development setup

You need the following tools installed:

- [Gleam](https://gleam.run/) (1.15+)
- Erlang/OTP (28+)
- [Node.js](https://nodejs.org/) (22+) — required to run the
  JavaScript-target test suite
- [just](https://github.com/casey/just) as a task runner
- [mise](https://mise.jdx.dev/) is recommended for managing the
  toolchain (the repository ships with `.mise.toml`)

Clone the repository and install the toolchain:

```console
git clone https://github.com/nao1215/datastream.git
cd datastream
mise install        # if using mise
gleam deps download
```

## Running tests

The full check used by CI is:

```console
just ci
```

This runs format check, type check, lint, both-target builds, and
both-target tests. You can also run individual steps:

| Command | What it does |
| --- | --- |
| `just format` | Reformat `src/` and `test/` |
| `just format-check` | Fail if any file would be reformatted |
| `just typecheck` | Run `gleam check` |
| `just lint` | Run `glinter` (warnings are errors) |
| `just build` | Build for the default Erlang target |
| `just build-erlang` | Build for the Erlang target explicitly |
| `just build-javascript` | Build for the JavaScript target |
| `just build-all-targets` | Build for both targets |
| `just test` | Run the test suite on the Erlang target |
| `just test-erlang` | Run the test suite on the Erlang target |
| `just test-javascript` | Run the test suite on the JavaScript target |
| `just test-all-targets` | Run the test suite on both targets |
| `just docs` | Build HexDocs HTML |
| `just check` | Format check + type check + lint + build + test (Erlang) |
| `just all` | Everything above on both targets, plus docs |
| `just clean` | Delete the build directory |

## Project layout

```
src/
  datastream.gleam              -- top-level library entry
  datastream/
    source.gleam                -- stream constructors
    stream.gleam                -- middle-of-pipeline combinators
    fold.gleam                  -- pure terminal reducers
    sink.gleam                  -- effectful terminal consumers
    chunk.gleam                 -- Chunk(a) opaque type and operations
    text.gleam                  -- chunk-boundary-aware text operations
    binary.gleam                -- BitArray framing
    dataprep.gleam              -- optional integration helpers
    erlang/                     -- Erlang-target-only extensions
      source.gleam              -- subject / timer based sources
      sink.gleam                -- subject sink
      par.gleam                 -- bounded concurrency
      time.gleam                -- time-based combinators
test/
  datastream_test.gleam         -- gleeunit entry point and tests
doc/
  reference/
    spec.md                     -- specification (v0.1.0)
    dataprep-relation.md        -- dataprep integration background
.github/workflows/ci.yml         -- CI for both targets
gleam.toml                       -- package metadata + glinter config
justfile                         -- local task runner
.mise.toml                       -- toolchain pin
scripts/lib/mise_bootstrap.sh    -- PATH bootstrap shared by justfile
```

The `datastream/erlang/*` modules MUST NOT be imported by cross-target
modules. The cross-target core MUST compile and pass tests on both
targets; the BEAM extension is only built and tested on Erlang.

## Submitting changes

1. Open or claim a GitHub issue first. The implementation roadmap is
   issue #1.
2. Match each PR to a single issue when possible.
3. Run `just ci` locally before pushing; CI runs the same set.
4. Update `doc/reference/spec.md` together with any breaking change
   to a documented API. The spec is the source of truth for v0.1.0.
5. Add an entry under `## [Unreleased]` in `CHANGELOG.md`.

## Test conventions

- Tests use [gleeunit](https://hexdocs.pm/gleeunit/). Test functions
  must end in `_test` and be `pub` (gleeunit auto-discovers them).
- Each issue carries an explicit table of (input → expected output)
  test cases in its description. Implement those cases as the
  acceptance criteria.
- Cross-cutting test obligations from the roadmap (laziness,
  order preservation, close contract) apply to every relevant
  combinator.
- Tests for the cross-target core MUST pass on both `--target erlang`
  and `--target javascript`. Tests that depend on `datastream/erlang/*`
  MUST be confined to that target.

## Code style

- `gleam format` is the formatter. CI fails on formatting drift.
- `glinter` is the linter. Configuration lives under
  `[tools.glinter]` in `gleam.toml`. Add per-rule justifications to
  the config when changing it.
- Public API (`pub fn`, `pub type`) MUST have doc comments.
