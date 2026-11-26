#!/usr/bin/env bash
# Verify that every class in a jar has the expected class-file major version.
# Java 8 = 52. Usage: check-class-major.sh <jar> [expected-major]
set -euo pipefail

[ "$#" -ge 1 ] || {
  echo "usage: check-class-major.sh <jar> [expected-major]" >&2
  exit 2
}

JAR="$1"
EXPECTED="${2:-52}"
[ -f "$JAR" ] || {
  echo "jar not found: $JAR" >&2
  exit 2
}

command -v unzip >/dev/null 2>&1 || {
  echo "unzip not found" >&2
  exit 2
}
command -v od >/dev/null 2>&1 || {
  echo "od not found" >&2
  exit 2
}

COUNT=0
BAD=0
while IFS= read -r entry; do
  [ -n "$entry" ] || continue
  major="$(unzip -p "$JAR" "$entry" | od -An -tu1 -N8 | awk '{print $7 * 256 + $8}')"
  COUNT=$((COUNT + 1))
  if [ "$major" != "$EXPECTED" ]; then
    echo "FAIL: $entry has major $major, expected $EXPECTED" >&2
    BAD=$((BAD + 1))
  fi
done < <(unzip -Z1 "$JAR" | grep '\.class$' || true)

[ "$COUNT" -gt 0 ] || {
  echo "FAIL: no class files in $JAR" >&2
  exit 1
}

if [ "$BAD" -ne 0 ]; then
  echo "FAIL: $BAD/$COUNT classes are not major $EXPECTED" >&2
  exit 1
fi

echo "OK: $COUNT classes have major $EXPECTED"
