# Client Reliability Implementation Plan

> **For agentic workers:** Use test-driven development per lane; integrate and verify under Deepwork.

**Goal:** Fix the six transport, callback, and publication findings in the review.

**Architecture:** Keep the current APIs. Fix deadline/connection state at the TCP boundary, and cancellation/late reply handling at the Sans-IO request boundary. Fix the event predicate independently.

**Tech Stack:** Rust, Tokio, std TCP, GitHub Actions.

**Spec:** The six findings in the preceding review and the approved repair scope. No new dependencies, public signature changes, push, or publication.

## Lanes and evidence

- [x] TCP (`src/{aio,blocking}/connection.rs`, TCP mock tests): verify silent INIT returns within the connection budget, cancellation allows another `open`, concurrent opens cannot claim readiness early, and configured response limit is applied before allocation. Check clean reopen/close behavior.
- [x] Callback (`src/callback/`, callback mock tests): verify timed-out request completes once, a late reply does not stop dispatch of the next valid reply, unknown IDs still error, and the original tree handle cancels a continuation (between stages and after a redirect). Cancellation is local; it does not revoke server-side operations.
- [x] Release (`.github/workflows/release.yml`): check all three supported paths—published release, manual dry run and manual publish—without making external changes.
- [x] Integration: focused regressions, all-feature locked suite, callback-only check, final diff and oracle gate.

The final serial suite against upstream slock `1669b53d9334` passed 128 tests with one benchmark ignored. A preceding parallel run saw three async parity INIT timeouts; the cause of that intermittent result was not established. The live tests use one server; replica failover is covered by transport mocks, not by a live cluster. Release workflow validation is local only.
