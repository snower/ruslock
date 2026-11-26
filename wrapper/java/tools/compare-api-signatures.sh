#!/usr/bin/env bash
# Compare two API signature files, ignoring provenance comment lines.
#
# Usage: compare-api-signatures.sh <baseline-signatures> <candidate-signatures>
set -euo pipefail

[ "$#" -eq 2 ] || {
  echo "usage: compare-api-signatures.sh <baseline> <candidate>" >&2
  exit 2
}

BASELINE="$1"
CANDIDATE="$2"
[ -f "$BASELINE" ] || {
  echo "baseline not found: $BASELINE" >&2
  exit 2
}
[ -f "$CANDIDATE" ] || {
  echo "candidate not found: $CANDIDATE" >&2
  exit 2
}

TMP="$(mktemp -d "${TMPDIR:-/tmp}/ruslock-api-compare.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

grep -v '^#' "$BASELINE" >"$TMP/baseline.txt" || true
grep -v '^#' "$CANDIDATE" >"$TMP/candidate.txt" || true

if diff -u --label baseline "$TMP/baseline.txt" --label candidate "$TMP/candidate.txt" >"$TMP/diff.txt"; then
  echo "OK: API signatures match ($(grep -c '^TYPE ' "$TMP/baseline.txt") types)"
else
  cat "$TMP/diff.txt" >&2
  echo "FAIL: API signatures differ" >&2
  exit 1
fi
