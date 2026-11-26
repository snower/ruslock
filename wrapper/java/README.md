# RusLock Java 8 wrapper

Maven artifact **`io.github.snower:ruslock:1.1.5`** providing the Jaslock-compatible Java 8
API (`io.github.snower.jaslock`) backed by the RusLock Rust primitives through JNI.

- Java source/target: **JDK 8**, class-file major **52**.
- API baseline: Jaslock commit `85c2d2b` (`1.1.5`), preserved MIT headers.
- Native path: Java facade → JNI `cdylib` → Rust `binding`/`aio` primitives → Slock.
- Native libraries are **bundled resources**, selected by OS/CPU and extracted to a private
  temp directory. There is no `System.loadLibrary`, `java.library.path` or silent pure-Java
  fallback.

> **Status:** Not published. See [Validation status](#validation-status) for
> the platforms and checks actually exercised.

## Layout

| Path | Purpose |
| --- | --- |
| `pom.xml` | Maven artifact, JDK 8 compiler settings, platform native profiles, JAR assembly. |
| `src/main/java/io/github/snower/jaslock/` | Jaslock-compatible facade and JNI seam. |
| `src/test/java/.../ClientTest.java` | Byte-for-byte Jaslock baseline test (24 tests); do not edit. |
| `native/` | JNI `cdylib` crate (`ruslock_java`) with a committed `Cargo.lock`. |
| `tools/` | Baseline, API-signature, class-major and isolated-consumer checks. |
| `tools/consumer/` | Dependency-only consumer fixture used by `verify-consumer.sh`. |

## Prerequisites

- Real **JDK 8** (`java -version` reports `1.8.0_*`). A newer JDK compiling with
  `--release 8` is not accepted as evidence for this module.
- Maven 3.8+ running on that JDK 8.
- Rust stable with Cargo, because the default build compiles the JNI crate with
  `--locked`.
- A Slock service on `127.0.0.1:5658` for `ClientTest` and the consumer lock check.
- POSIX `bash` (Git Bash on Windows) for `tools/*.sh`.

## Build

Full gate, including the 24 copied Jaslock tests and the native load tests:

```bash
JAVA_HOME="C:/Users/Administrator/.jdks/temurin-1.8.0_462" \
  PATH="C:/Users/Administrator/.jdks/temurin-1.8.0_462/bin:$PATH" \
  mvn -B -f wrapper/java/pom.xml clean verify
```

Package without running tests (still compiles and bundles the current platform's native
library):

```bash
JAVA_HOME="..." PATH="$JAVA_HOME/bin:$PATH" \
  mvn -B -f wrapper/java/pom.xml -Dmaven.test.skip=true package
```

Output: `wrapper/java/target/ruslock-1.1.5.jar`.

The `native-build` profile activates when `wrapper/java/native/Cargo.toml` exists. It runs
`cargo build --locked --release` with a private target directory
(`target/native`) and never reaches into another checkout. Build output, Surefire XML and
logs stay under `wrapper/java/target`.

## Native packaging per platform

Each platform profile activates on the host that is building and bundles exactly one
resource. The POM defines the following layout:

```text
io/github/snower/jaslock/native/<platform>/<library>
```

| Platform id | Bundled library | Maven profile | Build / verification status |
| --- | --- | --- | --- |
| `windows-x86_64` | `ruslock_java.dll` | `native-bundle-windows-x86_64` (`amd64` or `x86_64`) | **Verified locally** with JDK 8 and the isolated Maven consumer. |
| `linux-x86_64` | `libruslock_java.so` | `native-bundle-linux-x86_64` (`amd64` or `x86_64`) | Build path defined. **Candidate**: CI target; awaiting a green CI run. |
| `linux-aarch64` | `libruslock_java.so` | `native-bundle-linux-aarch64` | Build path defined. **Candidate**: no runner/artifact yet. |
| `macos-x86_64` | `libruslock_java.dylib` | `native-bundle-macos-x86_64` | Build path defined. **Candidate**: not built or run in CI. |
| `macos-aarch64` | `libruslock_java.dylib` | `native-bundle-macos-aarch64` (`aarch64` or `arm64`) | Build path defined. **Candidate**: not built or run in CI. |

Rules for this table:

- “Candidate” means the profile and resource name exist; it is **not** a support claim.
- Move a platform to “Verified” only after `verify-consumer.sh` has loaded the packaged
  native library and completed the real lock fixture on that exact OS, CPU architecture and
  Linux libc.
- Linux builds target the runner's glibc. musl/Alpine is not supported by this artifact.

## Packaging a multi-platform JAR

A normal build bundles only the machine that runs it. To assemble a JAR with several
platforms, build on each supported host, then merge the corresponding
`io/github/snower/jaslock/native/<platform>/` trees into one JAR. Record the source commit,
artifact SHA-256 and the consumer result for every merged platform. Do not add a resource
for a platform that was not built and exercised on that platform.

## Native loading contract

`NativeLibrary` loads the resource for the running JVM:

1. map `os.name`/`os.arch` to `windows-x86_64`, `linux-x86_64`, `linux-aarch64`,
   `macos-x86_64` or `macos-aarch64`;
2. find `io/github/snower/jaslock/native/<platform>/<library>` on the classpath;
3. create a fresh private directory under `java.io.tmpdir`, extract the library there with
   owner-only permissions where the file system supports POSIX modes, and call
   `System.load` on that fresh path.

Missing or unsupported resources fail with a message naming the platform and expected
classpath path. Nothing is downloaded at runtime and no local library is reused.

Notes and limits:

- Windows keeps a loaded DLL mapped until JVM exit, so the private temp directory may
  disappear only on reboot/cleanup; it is not reused across loads.
- The loader is guarded by a single static lock. Concurrent initialization within one class
  loader loads once; multiple class loaders each extract their own private copy.

## Isolated consumer verification

`tools/verify-consumer.sh` proves the packaged JAR is consumable by a project that knows
only the Maven coordinate.

```bash
# Preferred: consume the exact JAR built by `mvn clean verify`.
JAVA8_HOME="C:/Users/Administrator/.jdks/temurin-1.8.0_462" \
  bash wrapper/java/tools/verify-consumer.sh \
  --jar "$PWD/wrapper/java/target/ruslock-1.1.5.jar" \
  --workdir "$HOME/ruslock-consumer-check"

# Or build/install wrapper/java into the isolated repository first.
JAVA8_HOME="..." bash wrapper/java/tools/verify-consumer.sh
```

The work directory must be empty and should be outside the repository; the script creates
`<workdir>/m2` as the only Maven local repository. It then:

- installs `io.github.snower:ruslock:1.1.5` there (`install-file` for `--jar`, otherwise
  `mvn install -Dmaven.test.skip=true`);
- copies only `tools/consumer/pom.xml` and its sources out of the repository;
- runs `mvn verify` on the copied fixture with the isolated repository;
- for the fixture JVM, sets `-Djava.library.path=` and clears `LD_LIBRARY_PATH`,
  `DYLD_LIBRARY_PATH`, `JAVA_TOOL_OPTIONS`, `_JAVA_OPTIONS` and `JDK_JAVA_OPTIONS`.

The fixture (`tools/consumer`) declares only the `io.github.snower:ruslock` dependency and
asserts:

- `SlockClient` was loaded from the installed JAR, not from a source or `target` directory;
- `java.library.path` is empty;
- a `ruslock-native-*` extraction under `java.io.tmpdir` contains the native library;
- no consumer classpath entry or temp directory comes from `wrapper/java`;
- a real Slock accepts `ping`, lock acquire/release, `with()`, and re-acquire after release;
- a second client cannot acquire the same held lock (it receives `LockTimeoutException`),
  then succeeds after the first client releases it.

The script also runs a negative control: it strips the native resource from the verified
JAR, installs that copy in a second isolated repository, and requires the client to fail
with the documented “not bundled / no `java.library.path` fallback” message. An artifact
that silently loads some other native library therefore fails the check.

Success prints the machine marker `CONSUMER: PASS`; any other result exits non-zero. Logs
are retained in the printed workdir.

## Compatibility and artifact checks

CI runs the existing tools rather than duplicating them:

```bash
bash wrapper/java/tools/check-class-major.sh wrapper/java/target/ruslock-1.1.5.jar 52
bash wrapper/java/tools/api-signatures.sh \
  wrapper/java/target/ruslock-1.1.5.jar "$RUNNER_TEMP/candidate-api.txt" ci
bash wrapper/java/tools/compare-api-signatures.sh \
  wrapper/java/tools/baseline-api-signatures.txt "$RUNNER_TEMP/candidate-api.txt"
```

`verify-baseline.sh` re-checks the copied `ClientTest.java` and `LICENSE-JASLOCK` against the
pinned Jaslock commit. The full wrapper `ClientTest` run additionally requires Slock on
`127.0.0.1:5658` and is part of `mvn -B -f wrapper/java/pom.xml clean verify`.

## CI

`.github/workflows/java.yml` is the Java-only job. It uses:

- Temurin **8** for Maven and the consumer JVM;
- Rust stable with a locked Cargo build;
- an isolated Docker Slock service pinned to **`sujin190/slock:v2.1.14`** with manifest
  digest `sha256:41d7bfb8417f69c403a7c6eee56720221d8a6c7f2509b610440b63efd8dea9c0`
  (the same digest resolves from the `v2.1.14` tag; it is not `latest`);
- the pinned Jaslock commit clone only for `verify-baseline.sh`.

The job order is: baseline hash check → `mvn clean verify` → class major/API signature
checks → isolated consumer verification. A failure in any step fails the job; there is no
fallback to an unpinned image or to wrapper source/native paths.

## Validation status

| Item | Status |
| --- | --- |
| Baseline original Jaslock on JDK 8 / Windows, 24 tests | Recorded in `tools/README.md` (2026-11-26) |
| Wrapper `mvn clean verify` on JDK 8 | 66 passed on Windows x86-64, with no failures, errors, or skips; includes all 24 unchanged Jaslock tests and `testBenchmark` |
| Wrapper class major 52 / API signature parity | Verified on Windows x86-64 / JDK 8: major 52, 61 API types match |
| Isolated consumer load + real lock (Windows x86_64) | Verified on JDK 8 with Slock v2.1.14, including the missing-DLL negative control |
| Isolated consumer load + real lock (Linux x86_64) | CI gate defined; not yet green |
| linux-aarch64, macOS x86_64/aarch64 | Candidate only; no artifact or runtime evidence |

Do not infer support for a platform from a source profile. Only the CI consumer run (or an
equivalent recorded run on that platform) is evidence.

## Troubleshooting

- **`JAVA_HOME` is not JDK 8** — set `JAVA8_HOME` explicitly; the scripts refuse a newer JDK.
- **Missing native resource** — the JAR was built without the profile for that platform, or
  a multi-platform merge omitted it. The loader reports the expected resource path.
- **Consumer check resolves classes from the repository** — run it through
  `verify-consumer.sh` with an empty workdir; do not run the fixture in place.
- **Slock connection refused** — start the pinned service; the check does not create or stop
  a service on `:5658`.
- **Windows temp directory remains** — expected while a DLL is mapped; the next run uses a
  new private directory.

## License

The wrapper is MIT. Mirrored Jaslock Java sources retain their original MIT copyright; the
copied license is `LICENSE-JASLOCK` and is packaged under `META-INF/`.
