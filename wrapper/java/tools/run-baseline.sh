#!/usr/bin/env bash
# Reproduce the pinned Jaslock/JDK 8 baseline against a real Slock on
# 127.0.0.1:5658.
#
# The original checkout is never modified: the pinned commit is extracted with
# `git archive` into a temporary directory, then `mvn -B clean test` runs there.
# Test reports and logs stay in the temporary directory (or --workdir), never in
# this repository.
#
# Usage:
#   run-baseline.sh [--package] [--signatures FILE] [--workdir DIR]
#
# Logs and reports are always retained in the printed workdir.
#
# Env:
#   JASLOCK_SRC   Jaslock checkout (default D:/workspace/github/jaslock)
#   JAVA8_HOME    JDK 8 home (default: JAVA_HOME if 1.8, else the Temurin path)
#   SLOCK_BIN     optional slock binary used only for a read-only `info` probe
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1090
source "$HERE/baseline.properties"

JASLOCK_SRC="${JASLOCK_SRC:-D:/workspace/github/jaslock}"
JAVA8_HOME="${JAVA8_HOME:-}"
SLOCK_BIN="${SLOCK_BIN:-}"
WORKDIR=""
PACKAGE=0
SIGNATURES=""

usage() {
  cat <<'EOF'
Reproduce the pinned Jaslock/JDK 8 baseline against a real Slock on
127.0.0.1:5658.

The original checkout is never modified: the pinned commit is extracted with
`git archive` into a temporary directory, then `mvn -B clean test` runs there.
Test reports and logs stay in the printed workdir, never in this repository.

Usage:
  run-baseline.sh [--package] [--signatures FILE] [--workdir DIR]

Env:
  JASLOCK_SRC   Jaslock checkout (default D:/workspace/github/jaslock)
  JAVA8_HOME    JDK 8 home (default: JAVA_HOME if 1.8, else the Temurin path)
  SLOCK_BIN     optional slock binary used only for a read-only `info` probe
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --package)
      PACKAGE=1
      ;;
    --signatures)
      [ "$#" -ge 2 ] || {
        echo "--signatures needs a file" >&2
        exit 2
      }
      SIGNATURES="$2"
      shift
      ;;
    --workdir)
      [ "$#" -ge 2 ] || {
        echo "--workdir needs a directory" >&2
        exit 2
      }
      WORKDIR="$2"
      shift
      ;;
    --keep)
      # Kept for callers that used the earlier flag; logs are always retained.
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

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

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

java8_version_ok() {
  "$1/bin/java" -version 2>&1 | head -1 | grep -q 'version "1\.8'
}

resolve_java8() {
  if [ -n "$JAVA8_HOME" ]; then
    [ -x "$JAVA8_HOME/bin/java" ] || fail "JAVA8_HOME has no java: $JAVA8_HOME"
    java8_version_ok "$JAVA8_HOME" || fail "JAVA8_HOME is not JDK 8: $JAVA8_HOME"
    return
  fi
  if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ] && java8_version_ok "$JAVA_HOME"; then
    JAVA8_HOME="$JAVA_HOME"
    return
  fi
  if [ -x "C:/Users/Administrator/.jdks/temurin-1.8.0_462/bin/java" ] \
    && java8_version_ok "C:/Users/Administrator/.jdks/temurin-1.8.0_462"; then
    JAVA8_HOME="C:/Users/Administrator/.jdks/temurin-1.8.0_462"
    return
  fi
  if command -v javac >/dev/null 2>&1; then
    local javac_bin
    javac_bin="$(dirname "$(command -v javac)")"
    if java8_version_ok "$javac_bin"; then
      JAVA8_HOME="$javac_bin"
      return
    fi
  fi
  fail "JDK 8 not found; set JAVA8_HOME"
}

check_slock_port() {
  if (exec 3<>"/dev/tcp/$SLOCK_HOST/$SLOCK_PORT") 2>/dev/null; then
    exec 3>&-
    return 0
  fi
  if command -v nc >/dev/null 2>&1 && nc -z "$SLOCK_HOST" "$SLOCK_PORT" >/dev/null 2>&1; then
    return 0
  fi
  local powershell_bin
  powershell_bin="$(command -v powershell.exe || command -v powershell || true)"
  if [ -n "$powershell_bin" ]; then
    "$powershell_bin" -NoProfile -Command \
      "if ((Test-NetConnection -ComputerName '$SLOCK_HOST' -Port $SLOCK_PORT).TcpTestSucceeded) { exit 0 } else { exit 1 }" \
      >/dev/null 2>&1 && return 0
  fi
  return 1
}

[ -d "$JASLOCK_SRC/.git" ] || fail "Jaslock checkout not found: $JASLOCK_SRC (set JASLOCK_SRC)"
actual_head="$(git -C "$JASLOCK_SRC" rev-parse HEAD)"
[ "$actual_head" = "$JASLOCK_COMMIT" ] \
  || fail "Jaslock HEAD is $actual_head, expected $JASLOCK_COMMIT"
head_short="$(git -C "$JASLOCK_SRC" rev-parse --short HEAD)"

resolve_java8
export JAVA_HOME="$JAVA8_HOME"
if command -v cygpath >/dev/null 2>&1; then
  JAVA8_BIN="$(cygpath -u "$JAVA_HOME/bin")"
else
  JAVA8_BIN="$JAVA_HOME/bin"
fi
export PATH="$JAVA8_BIN:$PATH"

mvn_version="$(mvn -version 2>&1)"
echo "$mvn_version" | grep -q 'Java version: 1\.8' \
  || fail "Maven is not running on JDK 8:\n$mvn_version"
java -version 2>&1 | head -1 | grep -q 'version "1\.8' \
  || fail "java on PATH is not JDK 8: $(command -v java)"

check_slock_port || fail "no Slock listening on $SLOCK_HOST:$SLOCK_PORT"

if [ -z "$WORKDIR" ]; then
  WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/ruslock-jaslock-baseline.XXXXXX")"
else
  mkdir -p "$WORKDIR"
  WORKDIR="$(cd "$WORKDIR" && pwd)"
fi
CHECKOUT="$WORKDIR/checkout"
mkdir -p "$CHECKOUT"
TEST_LOG="$WORKDIR/mvn-test.log"

echo "== baseline =="
echo "Jaslock: $JASLOCK_COMMIT ($head_short)"
echo "JDK: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
echo "Maven: $(echo "$mvn_version" | head -1)"
echo "Slock: $SLOCK_HOST:$SLOCK_PORT reachable"
if [ -n "$SLOCK_BIN" ] && [ -x "$SLOCK_BIN" ]; then
  echo "-- slock info ($SLOCK_BIN) --"
  "$SLOCK_BIN" info 2>&1 || true
  echo "-- end slock info --"
fi
echo "Workdir: $WORKDIR"
echo

git -C "$JASLOCK_SRC" archive --format=tar --output="$WORKDIR/source.tar" "$JASLOCK_COMMIT"
tar -xf "$WORKDIR/source.tar" -C "$CHECKOUT"
rm -f "$WORKDIR/source.tar"

checked_test="$CHECKOUT/$CLIENT_TEST_PATH"
checked_license="$CHECKOUT/$LICENSE_PATH"
[ -f "$checked_test" ] || fail "extracted test missing"
[ -f "$checked_license" ] || fail "extracted license missing"

test_hash="$(sha256_file "$checked_test")"
license_hash="$(sha256_file "$checked_license")"
test_count="$(grep -c '@Test' "$checked_test")"
pom_version="$(sed -n 's:.*<version>\([^<]*\)</version>.*:\1:p' "$CHECKOUT/pom.xml" | head -1)"

[ "$test_hash" = "$CLIENT_TEST_SHA256" ] || fail "test hash $test_hash != $CLIENT_TEST_SHA256"
if [ "$license_hash" != "$LICENSE_SHA256" ] && [ "$license_hash" != "$LICENSE_GIT_SHA256" ]; then
  fail "license hash $license_hash matches neither $LICENSE_SHA256 nor $LICENSE_GIT_SHA256"
fi
[ "$test_count" = "$EXPECTED_TESTS" ] || fail "found $test_count @Test, expected $EXPECTED_TESTS"
[ "$pom_version" = "$JASLOCK_POM_VERSION" ] || fail "POM version $pom_version != $JASLOCK_POM_VERSION"

echo "CHECK: ClientTest.java sha256=$test_hash @Test=$test_count"
echo "CHECK: LICENSE sha256=$license_hash"
echo "CHECK: POM version=$pom_version"
echo

echo "COMMAND: (cd $CHECKOUT && mvn -B clean test)"
set +e
(cd "$CHECKOUT" && mvn -B clean test) >"$TEST_LOG" 2>&1
test_exit=$?
set -e

xml="$CHECKOUT/target/surefire-reports/TEST-io.github.snower.jaslock.ClientTest.xml"
tests="?"
failures="?"
errors="?"
skipped="?"
if [ -f "$xml" ]; then
  attr() {
    grep -m1 -o "$1=\"[0-9]*\"" "$xml" | head -1 | cut -d'"' -f2
  }
  tests="$(attr tests)"
  failures="$(attr failures)"
  errors="$(attr errors)"
  skipped="$(attr skipped)"
fi

echo "EXIT: $test_exit"
echo "RESULT: tests=$tests failures=$failures errors=$errors skipped=$skipped"
echo "REPORT: $xml"
echo "LOG: $TEST_LOG"

status=0
if [ "$test_exit" -ne 0 ]; then
  status=1
  echo "---- last 80 log lines ----" >&2
  tail -80 "$TEST_LOG" >&2 || true
  echo "---- end log ----" >&2
fi
if [ "$tests" != "$EXPECTED_TESTS" ] || [ "$failures" != "0" ] || [ "$errors" != "0" ] || [ "$skipped" != "0" ]; then
  status=1
fi

JAR="$CHECKOUT/target/jaslock-$JASLOCK_POM_VERSION.jar"
if [ "$PACKAGE" -eq 1 ]; then
  PACKAGE_LOG="$WORKDIR/mvn-package.log"
  echo "COMMAND: (cd $CHECKOUT && mvn -B -DskipTests package)"
  set +e
  (cd "$CHECKOUT" && mvn -B -DskipTests package) >"$PACKAGE_LOG" 2>&1
  package_exit=$?
  set -e
  echo "EXIT: $package_exit"
  echo "LOG: $PACKAGE_LOG"
  if [ "$package_exit" -ne 0 ]; then
    status=1
    tail -80 "$PACKAGE_LOG" >&2 || true
  elif [ -f "$JAR" ]; then
    echo "JAR: $JAR"
    echo "JAR_SHA256: $(sha256_file "$JAR")"
  else
    status=1
    echo "FAIL: expected jar not produced: $JAR" >&2
  fi
fi

if [ -n "$SIGNATURES" ]; then
  [ "$PACKAGE" -eq 1 ] || fail "--signatures requires --package"
  [ -f "$JAR" ] || fail "cannot emit signatures, jar missing: $JAR"
  bash "$HERE/api-signatures.sh" "$JAR" "$SIGNATURES" "jaslock $head_short"
fi

if [ "$status" -eq 0 ]; then
  echo "BASELINE: PASS ($EXPECTED_TESTS/$EXPECTED_TESTS, 0 failures, 0 errors, 0 skipped)"
else
  echo "BASELINE: FAIL"
fi

echo "NOTE: workdir retained for inspection: $WORKDIR"

exit "$status"
