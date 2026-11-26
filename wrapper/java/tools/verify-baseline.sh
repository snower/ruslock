#!/usr/bin/env bash
# Verify that the checked-in ClientTest.java and LICENSE-JASLOCK still match the
# pinned Jaslock baseline. Read-only: this script never copies or edits files.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAVA_ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck disable=SC1090
source "$HERE/baseline.properties"

JASLOCK_SRC="${JASLOCK_SRC:-D:/workspace/github/jaslock}"
TEST_DEST="$JAVA_ROOT/src/test/java/io/github/snower/jaslock/ClientTest.java"
LICENSE_DEST="$JAVA_ROOT/LICENSE-JASLOCK"

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

sha256_stdin() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 | awk '{print $1}'
  else
    echo "no sha256 tool found" >&2
    exit 2
  fi
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

[ -d "$JASLOCK_SRC/.git" ] || fail "Jaslock checkout not found: $JASLOCK_SRC (set JASLOCK_SRC)"
[ -f "$TEST_DEST" ] || fail "missing checked-in test: $TEST_DEST"
[ -f "$LICENSE_DEST" ] || fail "missing checked-in license: $LICENSE_DEST"

actual_head="$(git -C "$JASLOCK_SRC" rev-parse HEAD)"
[ "$actual_head" = "$JASLOCK_COMMIT" ] \
  || fail "Jaslock HEAD is $actual_head, expected $JASLOCK_COMMIT"

pinned_test_hash="$(git -C "$JASLOCK_SRC" show "$JASLOCK_COMMIT:$CLIENT_TEST_PATH" | sha256_stdin)"
pinned_license_hash="$(git -C "$JASLOCK_SRC" show "$JASLOCK_COMMIT:$LICENSE_PATH" | sha256_stdin)"
[ "$pinned_test_hash" = "$CLIENT_TEST_SHA256" ] \
  || fail "pinned test hash $pinned_test_hash != $CLIENT_TEST_SHA256"
[ "$pinned_license_hash" = "$LICENSE_GIT_SHA256" ] \
  || fail "pinned license hash $pinned_license_hash != $LICENSE_GIT_SHA256"

dest_test_hash="$(sha256_file "$TEST_DEST")"
dest_license_hash="$(sha256_file "$LICENSE_DEST")"
[ "$dest_test_hash" = "$CLIENT_TEST_SHA256" ] \
  || fail "checked-in test hash $dest_test_hash != $CLIENT_TEST_SHA256"
if [ "$dest_license_hash" != "$LICENSE_SHA256" ] && [ "$dest_license_hash" != "$LICENSE_GIT_SHA256" ]; then
  fail "checked-in license hash $dest_license_hash matches neither $LICENSE_SHA256 nor $LICENSE_GIT_SHA256"
fi

if [ -f "$JASLOCK_SRC/$LICENSE_PATH" ]; then
  src_license_hash="$(sha256_file "$JASLOCK_SRC/$LICENSE_PATH")"
  if [ "$src_license_hash" != "$dest_license_hash" ]; then
    case "$src_license_hash" in
      "$LICENSE_SHA256" | "$LICENSE_GIT_SHA256") ;;
      *) fail "source license hash $src_license_hash is not the pinned content" ;;
    esac
    echo "NOTE: checked-in license differs from source checkout only by line endings"
  fi
fi

test_count="$(grep -c '@Test' "$TEST_DEST")"
[ "$test_count" = "$EXPECTED_TESTS" ] \
  || fail "checked-in test contains $test_count @Test annotations, expected $EXPECTED_TESTS"

pom_version="$(git -C "$JASLOCK_SRC" show "$JASLOCK_COMMIT:pom.xml" | sed -n 's:.*<version>\([^<]*\)</version>.*:\1:p' | head -1)"
[ "$pom_version" = "$JASLOCK_POM_VERSION" ] \
  || fail "pinned POM version $pom_version != $JASLOCK_POM_VERSION"

printf 'OK: Jaslock %s (%s)\n' "$JASLOCK_COMMIT_SHORT" "$JASLOCK_COMMIT"
printf 'OK: ClientTest.java sha256=%s @Test=%s\n' "$dest_test_hash" "$test_count"
printf 'OK: LICENSE-JASLOCK sha256=%s\n' "$dest_license_hash"
printf 'OK: POM version=%s\n' "$pom_version"
