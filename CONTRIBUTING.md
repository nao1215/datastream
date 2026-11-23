# Contributing to datastream

## Development setup

- Gleam 1.14+ (`.mise.toml` pins the local development version)
- Erlang/OTP 27+ (CI tests 27 and 29)
- Node.js 22+ (required for the JavaScript-target test run)
- [just](https://github.com/casey/just) (task runner)
- [mise](https://mise.jdx.dev/) (recommended; the repository ships with `.mise.toml`)

```console
git clone https://github.com/nao1215/datastream.git
cd datastream
mise install
just deps
```

`just` recipes locate the mise-managed toolchain via `scripts/lib/mise_bootstrap.sh`, so `mise activate` is not required in the current shell.

## Running tests

```console
just ci
```

Runs format check, type check, lint, both-target builds, and both-target tests. Individual steps:

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

The cross-target core must pass on both `--target erlang` and `--target javascript`. Tests of Erlang-only operations use `@target(erlang)`.

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

User-facing docs (README, release notes, doc comments on public API) follow a terse reference-oriented style. Treat violations as review blockers.

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

## Verifying releases

Each GitHub Release attaches the tarball published to Hex (`datastream-<version>.tar`), `checksums.txt`, a cosign signature bundle (`checksums.txt.sigstore.json`) and build provenance (`datastream-<version>.intoto.jsonl`). The release workflow downloads the tarball from Hex after `gleam publish` and checks it against the checksum Hex records before signing it, so the tarball you get from Hex is the one that is signed and attested.

```sh
curl -fsSLO https://repo.hex.pm/tarballs/datastream-<version>.tar
gh attestation verify datastream-<version>.tar --repo nao1215/datastream
```

To check the signature as well, download `checksums.txt` and `checksums.txt.sigstore.json` from the Release into the same directory. The workflow runs on the release tag, or on `main` when the assets of a published tag are attached again.

```sh
cosign verify-blob \
  --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/nao1215/datastream/\.github/workflows/release\.yml@refs/(tags/v.+|heads/main)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt
sha256sum --check checksums.txt
```
