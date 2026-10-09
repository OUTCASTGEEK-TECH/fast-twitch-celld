# Phase 1 qualification

The **seven-item follow-up is complete with a native process-kill limitation**. Scoped completion and release readiness are recorded separately. The target is
Celld v0.6.2 (`90b43017241f81189453d326d05948f388b34652`). Source implementation,
portable checks, public-command checks, and actual native execution are separate
evidence. A passing family fixture does not certify every overload or option.

The retained [evidence snapshot](qualification-evidence.json) retains actual case results and integrity hashes. The resource catalog is the stable source inventory; the qualified catalog and evidence live under `docs` so writing test results does not change a runnable generation.

## Reproduce the evidence

Ordinary commands use the published Fast-Twitch pin in `deps.edn`; `:local` is an
optional sibling development override. Retained evidence keeps its original pins. Each
native runner starts and stops its own process sequentially and preserves local
state. Do not run a second process against the same `.celld/dev` database.

```sh
bin/celld-clojure -M:test
bb test/public_build_failures.clj
bb test/dev_qualification.clj
bb test/dependency_qualification.clj
bb test/dependency_qualification.clj --extra-alias
bb test/concurrent_build_qualification.clj
bb test/build_reproducibility.clj
bb test/native_qualification.clj
```

`--reuse-origin` is for an already-owned controlled-origin fixture. To resume
specific groups while retaining earlier evidence, use `--fixtures orchestration
interop generated-clients`. The runner's `complete` flag remains false because
this runner records fixture results; scoped completion is reconciled separately.

| Evidence | Meaning |
| --- | --- |
| `target/correction-native-verified/evidence.json` | Current correction and resumed Phase 1 exact immutable generations, bundle/gzip sizes, build/startup times, named native assertions, and repeated RPC timing. |
| `target/native-transport-evidence.json` | Owned WebSocket open/close counters, sliced binary, auto-response, forced native eviction and repeated cleanup. |
| `target/public-build-fixtures/evidence.json` | Actual public command failures, pointer retention and positive recovery. |
| `target/dev-qualification/evidence.json` | Fixture-owned JVM macro reload, malformed-app stale state, recovery, dotenv removal and safe clean. |
| `target/dependency-qualification/evidence.json` and `target/dependency-extra-qualification/evidence.json` | Fresh dependency resolution, regular/alias local-root changes, source edits, failure and recovery. |
| `target/build-reproducibility.json` | Two byte-identical public builds, source/config-only immutable-generation checks, matching hashes and a minimal-bundle budget. |
| `target/concurrent-build/evidence.json` | Lock-wait edits, selected profile resources, JSONC strings and native-file-relative resource edits. |
| `docs/coverage.json` and `docs/api-catalog.edn` | Member-level qualification with exact artifact/manifest links; unqualified members remain explicit. |

Native alarm and queue fixtures clear only their owned result markers before
the scheduled effect. Cron clears its marker before waiting for a new UTC event.
Other local state is retained. SQL resource cases drain write cursors on bounded
overflow, early reduction and throwing reducers while preserving primary errors.

## Container qualification gap

The existing Podman 6.1.3 machine was used without changing its defaults. The
macOS rootless route could not provide the native network prerequisite; the
existing rootful service was then exercised with an owned temporary fixture
inside that same VM using the official Linux ARM Celld 0.6.2 executable.

The current owned fixture passed 16 of 17 assertions: declared start/running,
port Fetch/TCP, exec streams/output/exit/PID, signal invocation, awaited inactivity
configuration, monitor and destroy. Signal is native fire-and-forget; the case
verifies invocation and running state, rather than signal delivery acknowledgment.

Native process kill remains limited: PID `253578` accepted `kill(15)`, then exited
normally with code `0` after 3,056 ms for the bounded 3-second sleep. The adapter
preserves the native receiver, signal, PID and settlement. The record distinguishes
`adapter-qualified: true` from `qualified: false` and retains the earlier 14/15
result under `previous-run`. Its current bundle/manifest hashes are verified.
This is an observed native/engine limit, not an outstanding adapter repair or VM
provisioning requirement. [Exact container evidence](qualification-evidence.json)
retains the source generation and diagnostics.

## Observed resource budgets

The owned direct-Cell fixture makes 100 RPC requests in groups of four and
requires contiguous, exactly-once effects. Its declared local budget is p95
below 100 ms and a maximum below 1,000 ms. This measures the compiled fixture,
HTTP path, validation and native dispatch together; it does not isolate validator
allocation or promise an application throughput. The runner retains actual
measurements in `target/native-resource-evidence.json`. A separate native test forces ten Cell evictions and requires eleven distinct activation tokens, the same Cell identity, fresh transient state, and initialization before access, with a local maximum activation budget of 1,000 ms. Its measurements are in `target/native-activation-evidence.json`.

SQL fixtures consume 10,000 rows with a 10-row reduction, reject bounded collection
overflow, and drain write/RETURNING cursors after overflow or a throwing reducer.
The stream fixture rejects a 2,048-byte response against its explicit read bound,
cancels its reader and releases the lock. WebSocket fixtures repeat ten resident
open/close cycles with exact callback counters and force one native hibernation.
Unknown and missing dispatch attachments close with code 4008 without callback dispatch. Abrupt native socket closure delivers exactly one typed error and reaches zero open sockets within the bounded observation window.

Minimal fixture manifests retain the analyzed namespace graph so unrelated
SQL, Workflow, Worker and container imports can be checked. The minimal checked RPC Cell has a declared bundle budget below 2,500,000 bytes and gzip below 300,000 bytes. Two unchanged public builds passed with an identical 1,699,809-byte bundle and 213,615-byte gzip result. The analyzed graph excludes unrelated service/SQL/Worker imports. The additional bounded validation/allocation evidence is described below.

## Scoped completion and native limits

Cell-context abort/reset, queue metrics, TCP startTls, asset map middleware,
current container adapters, evidence reconciliation, bounded validation/allocation
measurement, and keyword-first documentation/examples have their named evidence.
The catalog’s exact remaining scoped paths are empty. Release readiness remains
false because process kill has the recorded native limitation; it is distinct
from completion of the authorized seven items.

The measurement uses an external Node `performance.now` clock because the pinned
native Date/performance clocks are frozen within a turn. After one warmup request
per operation, five rounds time batches of 20,000 operations against the same
compiled Cell. Median incremental cached scalar validation was about 0.197 µs per
call; receiver-preserving adapter dispatch over direct Reflect.apply was about
0.370 µs. The raw paired samples include routing/HTTP noise and retain outliers;
these are local incremental estimates, rather than an isolated CPU/throughput claim.

Allocation is a source estimate of one explicit JS argument array per `invoke`
call and one JS field object per options projection, corroborated by distinct
projection identities. CLJS intermediates, bytes, GC and V8 escape analysis are
not measured. The actual samples, methodology, and immutable generation are in
[qualification evidence](qualification-evidence.json). The earlier zero native-clock
samples remain labeled unusable timing diagnostics.

Regenerate source documentation and reconcile the catalog after qualification:

```sh
bin/celld-clojure -M dev/scripts/public_api.clj
CELLD_SOURCE_HARNESS=/path/to/pinned-v0.6.2/harness.js bb dev/scripts/reconcile_api.clj
```

The optional harness path adds source signatures from the pinned source; it is
read-only. Reconciliation accepts only specific recorded assertions as evidence.

## Local handoff state

The Celld correction and qualification changes remain local and uncommitted.
Fast-Twitch is now pinned to published commit `a9f96bf90d0ded9282ac80a0f7fb808f1e5c719a`.
Historical evidence and artifact hashes retain their original provenance. Fixture processes are owned by the
qualification runners, which stop them after each run; durable state is retained.

The member counts and exact artifact links are in [coverage](coverage.json).
The public source inventory and documentation both list 211 functions.
The evidence accounts for named assertions and actual wrapper invocations,
including historical container artifact scope. Documented standard interop and
mandatory config validation do not imply an exhaustive Node or option audit.

To reconcile the current observed cases without changing compilation resources:

```sh
CELLD_EVIDENCE_FILE=target/correction-native-verified/evidence.json CELLD_RECONCILE_DOCS_ONLY=1 bb dev/scripts/reconcile_api.clj
CELLD_EVIDENCE_FILE=target/correction-native-verified/evidence.json bb test/freeze_release_evidence.clj
```

The Context property cases cover absent root properties/container handles and
presence of loopback exports. They do not certify configured startup properties,
container capabilities, or every export operation. TCP opened/closed cases cover
successful settlement around the owned plain TCP half-close fixture. The startTls
case verifies the local native trust-rejection and consumed-plaintext boundary. Native cache evidence covers the pinned runtime's documented
always-miss behavior.
