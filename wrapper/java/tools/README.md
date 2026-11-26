# Java wrapper baseline tools

These scripts pin the Jaslock compatibility target and make the JDK 8 baseline
repeatable without touching the external checkout's tracked files.

Baseline: `D:\workspace\github\jaslock` commit
`85c2d2bc337e701d560c17085c5fb3ae3277dd29` (`85c2d2b`, POM `1.1.5`).

| File | Purpose |
| --- | --- |
| `baseline.properties` | Pinned commit, test/license SHA-256, expected test count, Slock build. |
| `verify-baseline.sh` | Read-only check of the checked-in `ClientTest.java` / `LICENSE-JASLOCK` against the pinned commit. |
| `run-baseline.sh` | Extracts the pinned commit to a temp dir, runs `mvn -B clean test` on JDK 8 against Slock `127.0.0.1:5658`, optionally packages and emits signatures. |
| `ApiSignature.java` | Reflection-based signature emitter (public/protected, generic types, checked exceptions, synthetic/bridge filtered). |
| `api-signatures.sh` | Compiles/executes `ApiSignature.java` for a jar and writes a provenance header plus stable signatures. |
| `compare-api-signatures.sh` | Diffs two signature files while ignoring `#` provenance lines. |
| `baseline-api-signatures.txt` | Checked-in reference emitted from the pinned baseline jar. |
| `check-class-major.sh` | Asserts every class in a jar has class-file major 52 (Java 8). |

Run from Git Bash / Linux (the scripts are POSIX `bash`):

```bash
# 1. Verify the checked-in test and license bytes.
bash wrapper/java/tools/verify-baseline.sh

# 2. Reproduce the baseline 24 tests. Set SLOCK_BIN for a read-only version probe.
JAVA8_HOME="C:/Users/Administrator/.jdks/temurin-1.8.0_462" \
SLOCK_BIN="C:/path/to/slock.exe" \
bash wrapper/java/tools/run-baseline.sh --package

# 3. Regenerate the reference signatures (only when intentionally updating the pin).
bash wrapper/java/tools/run-baseline.sh --package \
  --signatures wrapper/java/tools/baseline-api-signatures.txt

# 4. Compare a candidate jar with the reference.
bash wrapper/java/tools/api-signatures.sh path/to/candidate.jar /tmp/candidate.txt
bash wrapper/java/tools/compare-api-signatures.sh \
  wrapper/java/tools/baseline-api-signatures.txt /tmp/candidate.txt

# 5. Check the Java 8 class-file level.
bash wrapper/java/tools/check-class-major.sh path/to/candidate.jar 52
```

`run-baseline.sh` never writes into the source checkout or this repository
(except the explicit `--signatures` path). Maven logs and Surefire XML stay in
the printed temp workdir.

## Reported baseline (2026-11-26)

- Platform: Windows amd64.
- JDK: Temurin `1.8.0_462`; Maven `3.10.0`; command `mvn -B clean test`.
- Slock: `v2.1.14` (`1669b53d9334454d4d8f8c25cf2311a269e0dbbe`), existing local
  process on `127.0.0.1:5658` (PID 13232, started `11:33:56`). Docker Desktop's
  daemon was unavailable, so no container was started. The process was not
  stopped or reconfigured; `slock info` reported DB 0 with `KeyCount=0` and
  `LockedCount=0` around the run.
- Result: `Tests run: 24, Failures: 0, Errors: 0, Skipped: 0`, exit 0.
- Artifacts: baseline jar had 62 classes at major 52; signatures contain 61
  visible types, 465 methods and 117 constructors.

Latest retained run (Git Bash paths; Windows path under
`C:\Users\Administrator\AppData\Local\Temp`):

- Workdir: `/tmp/ruslock-jaslock-baseline.k8caL6`
- Test log: `/tmp/ruslock-jaslock-baseline.k8caL6/mvn-test.log`
- Surefire:
  `/tmp/ruslock-jaslock-baseline.k8caL6/checkout/target/surefire-reports/TEST-io.github.snower.jaslock.ClientTest.xml`
- Package log: `/tmp/ruslock-jaslock-baseline.k8caL6/mvn-package.log`

For a later "independent clean" wrapper run, reuse the same service only if
`KeyCount`/`LockedCount` are 0 before the run, or ask the service owner to
restart Slock with a clean data directory. These tools deliberately never stop
an existing Slock process.
