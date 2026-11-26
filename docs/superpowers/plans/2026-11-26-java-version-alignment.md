# Java Version Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish matching Java and Rust versions and reject version drift in CI.

**Architecture:** Keep Maven's release version explicit in the wrapper POM. Read that version in artifact verification tooling and compare it against the root Cargo package in CI.

**Tech Stack:** Maven, JDK 8, Bash, GitHub Actions, Python 3 (CI only).

**Spec:** `docs/superpowers/specs/2026-11-26-java-version-alignment-design.md`

## Global Constraints

- Current RusLock release version is `0.1.0` in Maven and Cargo; Jaslock `1.1.5` remains the unchanged baseline.
- No API changes, automatic commit, or publication.

## Review Focus

- Version drift must fail before Maven builds; artifact checks must not accidentally consume a JAR from the old version.

### Task 1: Align the wrapper and verification tooling

**Files:** `.github/workflows/java.yml`, `wrapper/java/pom.xml`, `wrapper/java/tools/verify-consumer.sh`, `wrapper/java/tools/consumer/pom.xml`, and `wrapper/java/README.md`.

**Interfaces:** CI reads root `Cargo.toml` `[package].version` and wrapper POM `project/version`, rejects mismatches, and supplies the corresponding JAR path to its checks. The consumer verifier resolves `project.version` from the wrapper POM via Maven and supplies `-Druslock.version` to both consumer runs.

- [ ] Add the CI version comparison; run the comparison against the current unequal versions and confirm a nonzero exit that names both values.
- [ ] Set the wrapper version to `0.1.0`. Remove the independently maintained consumer fixture version and workflow JAR literal. Update the README dependency and examples, leaving pinned-baseline references unchanged.
- [ ] Verify the matching version check, JDK 8 `mvn -B -f wrapper/java/pom.xml clean verify`, isolated consumer positive and negative checks against `ruslock-0.1.0.jar`, and pinned baseline hash. Confirm the Java workflow uses the checked version in both artifact checks.
