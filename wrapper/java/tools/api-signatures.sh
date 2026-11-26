#!/usr/bin/env bash
# Generate normalized public/protected API signatures from a jar.
#
# The output starts with comment lines (#) containing provenance. Use
# compare-api-signatures.sh to diff two outputs while ignoring those comments.
#
# Usage: api-signatures.sh <jar> <output|-> [label]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

[ "$#" -ge 2 ] || {
  echo "usage: api-signatures.sh <jar> <output|-> [label]" >&2
  exit 2
}

JAR="$1"
OUT="$2"
LABEL="${3:-}"

[ -f "$JAR" ] || {
  echo "jar not found: $JAR" >&2
  exit 2
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "no sha256 tool found" >&2
    exit 2
  fi
}

TMP="$(mktemp -d "${TMPDIR:-/tmp}/ruslock-api-signatures.XXXXXX")"
cleanup() {
  rm -rf "$TMP"
}
trap cleanup EXIT

if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/javac" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVAC="$JAVA_HOME/bin/javac"
  JAVA="$JAVA_HOME/bin/java"
else
  JAVAC="$(command -v javac)"
  JAVA="$(command -v java)"
fi
[ -n "$JAVAC" ] && [ -n "$JAVA" ] || {
  echo "javac/java not found; set JAVA_HOME" >&2
  exit 2
}

"$JAVAC" -d "$TMP" "$HERE/ApiSignature.java"
"$JAVA" -cp "$TMP" ApiSignature "$JAR" "$TMP/api.txt"

JAR_HASH="$(sha256_file "$JAR")"
JDK_LINE="$("$JAVA" -version 2>&1 | head -1)"

emit() {
  echo "# RusLock Java API signatures"
  [ -n "$LABEL" ] && echo "# label: $LABEL"
  echo "# jar: $(basename "$JAR")"
  echo "# jar-sha256: $JAR_HASH"
  echo "# jdk: $JDK_LINE"
  echo "# generator: wrapper/java/tools/ApiSignature.java"
  cat "$TMP/api.txt"
}

if [ "$OUT" = "-" ]; then
  emit
else
  mkdir -p "$(dirname "$OUT")"
  emit >"$OUT"
  echo "wrote $OUT ($(wc -l <"$OUT" | tr -d ' ') lines) from $JAR" >&2
fi
