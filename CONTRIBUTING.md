# Contributing to datastream

## Development setup

- Gleam 1.15+
- Erlang/OTP 28+
- Node.js 22+ (required for the JavaScript-target test run)
- [just](https://github.com/casey/just) (task runner)
- [mise](https://mise.jdx.dev/) (recommended; the repository ships with `.mise.toml`)

```console
git clone https://github.com/nao1215/datastream.git
cd datastream
mise install
gleam deps download
```

`just` recipes locate the mise-managed toolchain via
`scripts/lib/mise_bootstrap.sh`, so `mise activate` is not required
in the current shell.

## Running tests

```console
just ci
```

Runs format check, type check, lint, both-target builds, and
both-target tests. Individual steps:

| Command | Effect |
| --- | --- |
| `just format` | Reformat `src/` and `test/` |
| `just format-check` | Fail on formatting drift |
| `just typecheck` | `gleam check` |
| `just lint` | `glinter` (warnings as errors) |
| `just build-erlang` / `just build-javascript` | Per-target build |
| `just test-erlang` / `just test-javascript` | Per-target test |
| `just docs` | Build HexDocs HTML |
| `just clean` | Delete `build/` |

The cross-target core must pass on both `--target erlang` and
`--target javascript`. Tests that import `datastream/erlang/*` are
limited to the Erlang target.

## Code style

- Run `gleam format src/ test/` before committing.
- The build uses `--warnings-as-errors`; fix all warnings.
- `glinter` runs in `warnings_as_errors` mode. Rule overrides live
  under `[tools.glinter]` in `gleam.toml`; add a justification
  comment when changing them.
- Public API (`pub fn`, `pub type`) requires doc comments.
- Prefer early errors over silent fallbacks.
- Keep private functions private; do not expose them just for testing.

## Pull request expectations

- All CI checks must pass (`just ci`).
- Include tests for new behavior.
- Use [Conventional Commits](https://www.conventionalcommits.org/)
  for commit messages (`feat:`, `fix:`, `chore:`, `docs:`, `ci:`, …).
- One logical change per PR.

## Public documentation style

User-facing docs (README, release notes, doc comments on public API)
follow a terse reference-oriented style. Treat violations as review
blockers.

- No marketing prose. Write as if documenting a standard library.
- No bold emphasis on inline phrases. Use bold only for table headers
  or actual UI labels.
- No emoji in code examples, tables, or prose.
- Prefer code examples over explanations. If the example is
  self-explanatory, delete the surrounding commentary.
- State constraints factually: "X requires Y" beats "You need to make
  sure that X has Y".
- Do not use second-person cheerleading ("you can easily", "just do
  X").
