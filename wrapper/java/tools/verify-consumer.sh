#!/usr/bin/env bash
# Dependency-only consumer verification for the packaged RusLock Java artifact.
#
# The check has three hard requirements:
#   1. it runs a real JDK 8 (not a newer JDK in "--release 8" mode);
#   2. the artifact is resolved from a fresh, isolated Maven local repository;
#   3. the fixture uses only the public io.github.snower.jaslock API, has an
#      empty java.library.path and performs a real lock round trip on Slock.
#
# The wrapper project itself is not on the consumer classpath. The fixture is
# copied to a temporary directory and Maven is told to use a repository below
# that directory, so the check cannot accidentally borrow wrapper/target,
# wrapper/native/target or an earlier ~/.m2 install.
#
# Usage:
#   verify-consumer.sh [--jar FILE] [--workdir DIR] [--host HOST] [--port PORT]
#
# Without --jar the script builds and installs wrapper/java with tests skipped
# into the isolated repository. Use --jar after `mvn clean verify` to consume
# the exact verified artifact. CI does the latter.
#
# Env:
#   JAVA8_HOME     JDK 8 home (default: JAVA_HOME if 1.8, else the Temurin path)
#   RUSLOCK_JAR    same as --jar
#   SLOCK_HOST     default 127.0.0.1
#   SLOCK_PORT     default 5658
#
# The work directory and all Maven/consumer logs are retained and printed.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAVA_ROOT="$(cd "$HERE/.." && pwd)"
FIXTURE="$HERE/consumer"
ARTIFACT_GROUP_PATH="io/github/snower/ruslock"
ARTIFACT_VERSION="1.1.5"
ARTIFACT_JAR="ruslock-${ARTIFACT_VERSION}.jar"

JAR="${RUSLOCK_JAR:-}"
WORKDIR=""
SLOCK_HOST="${SLOCK_HOST:-127.0.0.1}"
SLOCK_PORT="${SLOCK_PORT:-5658}"
JAVA8_HOME="${JAVA8_HOME:-}"

usage() {
  cat <<'EOF'
Dependency-only consumer verification for the packaged RusLock Java artifact.

Usage:
  verify-consumer.sh [--jar FILE] [--workdir DIR] [--host HOST] [--port PORT]

Options:
  --jar FILE     Consume this JAR instead of building wrapper/java.
  --workdir DIR  Use DIR (must be empty). Default: a fresh temp directory.
  --host HOST    Slock host (default 127.0.0.1).
  --port PORT    Slock port (default 5658).

Env:
  JAVA8_HOME     JDK 8 home (default: JAVA_HOME if 1.8, else the Temurin path)
  RUSLOCK_JAR    same as --jar
  SLOCK_HOST     default 127.0.0.1
  SLOCK_PORT     default 5658
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --jar)
      [ "$#" -ge 2 ] || {
        echo "--jar needs a file" >&2
        exit 2
      }
      JAR="$2"
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
    --host)
      [ "$#" -ge 2 ] || {
        echo "--host needs a value" >&2
        exit 2
      }
      SLOCK_HOST="$2"
      shift
      ;;
    --port)
      [ "$#" -ge 2 ] || {
        echo "--port needs a value" >&2
        exit 2
      }
      SLOCK_PORT="$2"
      shift
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

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

native_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$1"
  else
    printf '%s' "$1"
  fi
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

[ -f "$JAVA_ROOT/pom.xml" ] || fail "wrapper/java POM not found: $JAVA_ROOT/pom.xml"
[ -f "$FIXTURE/pom.xml" ] || fail "consumer fixture not found: $FIXTURE"
[ -f "$FIXTURE/src/main/java/io/github/snower/consumer/ConsumerCheck.java" ] \
  || fail "consumer fixture sources not found under $FIXTURE"

# The fixture must stay dependency-only. These strings are checked without
# matching the explanatory comments by looking for the XML elements only.
if grep -q '<systemPath>' "$FIXTURE/pom.xml"; then
  fail "consumer fixture must not use a <systemPath> dependency"
fi
if [ "$(grep -c '<dependency>' "$FIXTURE/pom.xml")" -ne 1 ]; then
  fail "consumer fixture must declare exactly one dependency (io.github.snower:ruslock)"
fi

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
  WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/ruslock-consumer.XXXXXX")"
else
  mkdir -p "$WORKDIR"
  if [ -n "$(ls -A "$WORKDIR")" ]; then
    fail "workdir must be empty so the Maven repository stays isolated: $WORKDIR"
  fi
  WORKDIR="$(cd "$WORKDIR" && pwd)"
fi

REPO="$WORKDIR/m2"
CONSUMER="$WORKDIR/consumer"
JVM_TMP="$WORKDIR/jvm-tmp"
INSTALL_LOG="$WORKDIR/maven-install.log"
CONSUMER_LOG="$WORKDIR/maven-consumer.log"
mkdir -p "$REPO" "$JVM_TMP"
REPO_NATIVE="$(native_path "$REPO")"
JAVA_ROOT_NATIVE="$(native_path "$JAVA_ROOT")"
JAVA_POM_NATIVE="$(native_path "$JAVA_ROOT/pom.xml")"

echo "== isolated consumer verification =="
echo "JDK: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
echo "Maven: $(echo "$mvn_version" | head -1)"
echo "Slock: $SLOCK_HOST:$SLOCK_PORT reachable"
echo "Isolated Maven repo: $REPO"
echo "Workdir: $WORKDIR"
echo

if [ -n "$JAR" ]; then
  [ -f "$JAR" ] || fail "jar not found: $JAR"
  JAR="$(cd "$(dirname "$JAR")" && pwd)/$(basename "$JAR")"
  JAR_NATIVE="$(native_path "$JAR")"
  echo "COMMAND: mvn install-file $JAR -> $REPO"
  set +e
  mvn -B org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile="$JAR_NATIVE" \
    -DpomFile="$JAVA_POM_NATIVE" \
    -Dmaven.repo.local="$REPO_NATIVE" >"$INSTALL_LOG" 2>&1
  install_exit=$?
  set -e
else
  echo "COMMAND: mvn -B -f $JAVA_ROOT/pom.xml -Dmaven.test.skip=true install -Dmaven.repo.local=$REPO"
  set +e
  mvn -B -f "$JAVA_POM_NATIVE" \
    -Dmaven.test.skip=true \
    install \
    -Dmaven.repo.local="$REPO_NATIVE" >"$INSTALL_LOG" 2>&1
  install_exit=$?
  set -e
fi

echo "EXIT: $install_exit"
echo "LOG: $INSTALL_LOG"
if [ "$install_exit" -ne 0 ]; then
  tail -80 "$INSTALL_LOG" >&2 || true
  fail "artifact was not installed into the isolated repository"
fi

INSTALLED="$REPO/$ARTIFACT_GROUP_PATH/$ARTIFACT_VERSION/$ARTIFACT_JAR"
[ -f "$INSTALLED" ] || fail "expected artifact missing after install: $INSTALLED"
installed_hash="$(sha256_file "$INSTALLED")"
CONSUMER_POM_NATIVE="$(native_path "$CONSUMER/pom.xml")"
echo "ARTIFACT: $INSTALLED"
echo "ARTIFACT_SHA256: $installed_hash"

if [ -n "${JAR:-}" ]; then
  if [ -f "$JAR" ] && [ "$(sha256_file "$JAR")" != "$installed_hash" ]; then
    fail "installed artifact hash $installed_hash differs from input $JAR"
  fi
fi

# The release artifact must carry at least one native library for a supported
# platform. The runtime check below still proves that the running platform's
# resource is present and loadable.
if command -v unzip >/dev/null 2>&1; then
  native_entries="$(unzip -Z1 "$INSTALLED" \
    | grep -E '^io/github/snower/jaslock/native/[^/]+/(ruslock_java\.dll|libruslock_java\.(so|dylib))$' || true)"
  [ -n "$native_entries" ] || fail "artifact has no bundled native library resource"
  echo "NATIVE RESOURCES:"
  echo "$native_entries" | sed 's/^/  /'
else
  echo "NOTE: unzip not found; skipping the static native-resource listing" >&2
fi

# Copy only the fixture's POM and sources. Running it in place would let the
# consumer classpath include wrapper/java/tools/consumer/target.
mkdir -p "$CONSUMER"
cp "$FIXTURE/pom.xml" "$CONSUMER/pom.xml"
mkdir -p "$CONSUMER/src"
cp -R "$FIXTURE/src/." "$CONSUMER/src/"

run_consumer_check() {
  local repo="$1"
  local expected="$2"
  local tmp="$3"
  local expect_missing="$4"
  local log="$5"
  mkdir -p "$tmp"
  (
    # A forked JVM inherits these; clear them so no external native search path
    # or JVM option can influence the result.
    unset LD_LIBRARY_PATH DYLD_LIBRARY_PATH JAVA_TOOL_OPTIONS _JAVA_OPTIONS JDK_JAVA_OPTIONS
    mvn -B -f "$CONSUMER_POM_NATIVE" \
      verify \
      -Dmaven.repo.local="$(native_path "$repo")" \
      -Druslock.host="$SLOCK_HOST" \
      -Druslock.port="$SLOCK_PORT" \
      -Druslock.expected.jar="$(native_path "$expected")" \
      -Druslock.forbidden.root="$JAVA_ROOT_NATIVE" \
      -Druslock.jvm.tmpdir="$(native_path "$tmp")" \
      -Druslock.expect.missing.native="$expect_missing"
  ) >"$log" 2>&1
}

echo
echo "COMMAND: mvn -B -f $CONSUMER/pom.xml verify -Dmaven.repo.local=$REPO"
set +e
run_consumer_check "$REPO" "$INSTALLED" "$JVM_TMP" false "$CONSUMER_LOG"
consumer_exit=$?
set -e

echo "EXIT: $consumer_exit"
echo "LOG: $CONSUMER_LOG"
grep -E '^CONSUMER: ' "$CONSUMER_LOG" || true

if [ "$consumer_exit" -ne 0 ]; then
  echo "---- last 100 consumer log lines ----" >&2
  tail -100 "$CONSUMER_LOG" >&2 || true
  echo "---- end log ----" >&2
fi

status=0
[ "$consumer_exit" -eq 0 ] || status=1
grep -q '^CONSUMER: PASS$' "$CONSUMER_LOG" || status=1

# Negative control from the plan: an artifact without the current platform's
# native resource must fail closed. Strip the resource from the verified JAR,
# install it in a second isolated repository and require the documented
# "not bundled / no java.library.path fallback" failure.
MISSING_LOG="$WORKDIR/maven-consumer-missing-native.log"
if command -v unzip >/dev/null 2>&1 && command -v jar >/dev/null 2>&1; then
  echo
  echo "== negative control: JAR without bundled native resources =="
  STRIP="$WORKDIR/no-native"
  STRIP_JAR="$STRIP/$ARTIFACT_JAR"
  mkdir -p "$STRIP/classes"
  (cd "$STRIP/classes" && unzip -q "$INSTALLED")
  rm -rf "$STRIP/classes/io/github/snower/jaslock/native"
  rm -f "$STRIP/classes/META-INF/MANIFEST.MF"
  (cd "$STRIP/classes" && jar cf "$STRIP_JAR" . >/dev/null)

  REPO_MISSING="$WORKDIR/m2-no-native"
  mkdir -p "$REPO_MISSING"
  echo "COMMAND: mvn install-file (no native) $STRIP_JAR -> $REPO_MISSING"
  set +e
  mvn -B org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
    -Dfile="$(native_path "$STRIP_JAR")" \
    -DpomFile="$JAVA_POM_NATIVE" \
    -Dmaven.repo.local="$(native_path "$REPO_MISSING")" \
    >"$WORKDIR/maven-install-no-native.log" 2>&1
  missing_install_exit=$?
  set -e
  MISSING_INSTALLED="$REPO_MISSING/$ARTIFACT_GROUP_PATH/$ARTIFACT_VERSION/$ARTIFACT_JAR"
  if [ "$missing_install_exit" -ne 0 ] || [ ! -f "$MISSING_INSTALLED" ]; then
    echo "FAIL: could not install the no-native control artifact" >&2
    tail -40 "$WORKDIR/maven-install-no-native.log" >&2 || true
    status=1
  else
    set +e
    run_consumer_check "$REPO_MISSING" "$MISSING_INSTALLED" \
      "$WORKDIR/jvm-tmp-no-native" true "$MISSING_LOG"
    missing_exit=$?
    set -e
    echo "EXIT: $missing_exit"
    echo "LOG: $MISSING_LOG"
    grep -E '^CONSUMER: ' "$MISSING_LOG" || true
    if [ "$missing_exit" -ne 0 ] || ! grep -q '^CONSUMER: PASS$' "$MISSING_LOG"; then
      status=1
      echo "---- last 100 no-native consumer log lines ----" >&2
      tail -100 "$MISSING_LOG" >&2 || true
      echo "---- end log ----" >&2
    fi
  fi
else
  echo "NOTE: unzip or jar not found; skipping the missing-native negative control" >&2
fi

echo
if [ "$status" -eq 0 ]; then
  echo "CONSUMER VERIFY: PASS"
  echo "  artifact: $INSTALLED"
  echo "  sha256: $installed_hash"
  echo "  isolated repo: $REPO"
else
  echo "CONSUMER VERIFY: FAIL"
fi
echo "NOTE: workdir retained for inspection: $WORKDIR"

exit "$status"
