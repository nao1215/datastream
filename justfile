set shell := ["sh", "-cu"]

# Make the mise-managed toolchain (erlang / gleam / rebar / node)
# visible to every recipe even when the invoking shell has not run
# `mise activate`. The bootstrap helper is the same one that
# standalone shell scripts source, so `just <recipe>` and
# `bash scripts/<name>.sh` behave identically in a fresh shell.
export PATH := shell('. scripts/lib/mise_bootstrap.sh; printf %s "$PATH"')

default:
  @just --list

# --- toolchain ---------------------------------------------------------------

deps:
  gleam deps download

# --- formatting / linting ---------------------------------------------------

format:
  gleam format src/ test/

format-check:
  gleam format --check src/ test/

lint:
  gleam run -m glinter

# --- type and build checks --------------------------------------------------

typecheck:
  gleam check

build:
  gleam build --warnings-as-errors

# Cross-target build verification. The Erlang build is the default
# `gleam build`; the JavaScript build needs an explicit `--target`.
build-erlang:
  gleam build --warnings-as-errors --target erlang

build-javascript:
  gleam build --warnings-as-errors --target javascript

build-all-targets: build-erlang build-javascript

# --- tests ------------------------------------------------------------------

test:
  gleam test

# Cross-target test runs. The cross-target core MUST pass on both
# targets; the BEAM extension is exercised only by the Erlang run.
test-erlang:
  gleam test --target erlang

test-javascript:
  gleam test --target javascript

test-all-targets: test-erlang test-javascript

# --- docs -------------------------------------------------------------------

docs:
  gleam docs build

# --- aggregates -------------------------------------------------------------

# Single-target check used during day-to-day development. Mirrors the
# step set CI runs on the Erlang target.
check: clean
  gleam format --check src/ test/
  gleam check
  gleam run -m glinter
  gleam build --warnings-as-errors
  gleam test

# Entrypoint CI uses. Adds the JavaScript-target build and test on
# top of `check` so cross-target regressions fail fast.
ci: deps check build-javascript test-javascript

# Run absolutely everything: format check, lint, both-target builds,
# both-target tests, docs.
all: clean deps
  gleam format --check src/ test/
  gleam check
  gleam run -m glinter
  gleam build --warnings-as-errors --target erlang
  gleam build --warnings-as-errors --target javascript
  gleam test --target erlang
  gleam test --target javascript
  gleam docs build
  @echo ""
  @echo "All checks passed."

# --- maintenance ------------------------------------------------------------

clean:
  gleam clean
