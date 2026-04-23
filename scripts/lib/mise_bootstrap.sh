#!/bin/sh
# mise_bootstrap.sh -- Shared helper that makes mise-managed tools
# (erlang, gleam, rebar, node) visible on PATH without requiring the
# caller to have run `mise activate` in the current shell.
#
# Usage:
#   . scripts/lib/mise_bootstrap.sh   # source from any bash/sh script
#   datastream_require_tool gleam     # fail fast with a setup message
#
# The sourcing itself runs `datastream_mise_bootstrap` once, so most
# callers only need the `.` line. The helper is idempotent — sourcing
# it twice leaves PATH unchanged. Intentionally POSIX-compatible (no
# `local`, no `[[ ]]`), so it can be sourced from `sh -cu` as well as
# bash.

_datastream_mise_prepend() {
  case ":${PATH-}:" in
    *":$1:"*) ;;
    *) PATH="$1${PATH:+:$PATH}" ;;
  esac
}

# Tools whose parent directory should be added to PATH. `escript` and
# `erl` are needed for any Erlang-target build; `node` is needed for
# `gleam test --target javascript`.
_DATASTREAM_MISE_TOOLS="gleam escript erl rebar3 node"

datastream_mise_bootstrap() {
  if [ -n "${HOME:-}" ] && [ -d "$HOME/.local/bin" ]; then
    _datastream_mise_prepend "$HOME/.local/bin"
  fi

  if command -v mise >/dev/null 2>&1; then
    for _datastream_bootstrap_tool in $_DATASTREAM_MISE_TOOLS; do
      _datastream_bootstrap_path="$(mise which "$_datastream_bootstrap_tool" 2>/dev/null || true)"
      if [ -n "$_datastream_bootstrap_path" ]; then
        _datastream_mise_prepend "$(dirname "$_datastream_bootstrap_path")"
      fi
    done
    unset _datastream_bootstrap_tool _datastream_bootstrap_path
  fi

  if ! command -v gleam >/dev/null 2>&1 && [ -n "${HOME:-}" ]; then
    _datastream_mise_prepend "$HOME/.local/share/mise/shims"
  fi

  export PATH
}

datastream_require_tool() {
  if command -v "$1" >/dev/null 2>&1; then
    return 0
  fi
  cat >&2 <<EOF
error: required tool '$1' was not found on PATH.

This repository manages its toolchain (Erlang, Gleam, rebar3, Node)
with mise. To set up the development environment from a fresh
checkout:

    mise install

If mise itself is missing, see https://mise.jdx.dev/getting-started.html.
Alternatively, install '$1' by hand and make sure it is on PATH
before running this command.
EOF
  return 127
}

datastream_mise_bootstrap
