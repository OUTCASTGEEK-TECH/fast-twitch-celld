# Native values and resource ownership

The target is Celld **0.6.2**, source `90b43017241f81189453d326d05948f388b34652`, with the compiler/runtime pins in `resources/fast_twitch/celld/runtime-profiles.edn`. Celld documentation defines behavior. A shared signature does not imply another deployment target.

## Data boundaries

Native handles are retained. Calling `native/invoke` uses `Reflect.apply` with the original receiver; this also works with loaded RPC proxy methods whose `apply` property denotes another pipelined operation. No wrapper retries an effect. Sync APIs stay synchronous, and native Promise rejection is preserved.

A native data policy accepts checked primitives, finite safe numbers, native arrays/plain objects, BigInt, Date, RegExp, Map, Set, ArrayBuffer and typed-array views. Native object identity and cycles are retained; the checked graph has a depth limit of 64. It rejects persistent CLJS collections, keywords, symbols, functions and unsupported capabilities. `:json` uses a version-2 envelope preserving keyword keys/values and namespaces; its encoded UTF-8 payload shares the profile byte budget in both directions. Legacy version-1 reads project received fields to keywords. Authored JSON maps require keyword keys, including native literals and internal envelopes. Missing values remain distinct from stored null.

The explicit RPC-only `:rpc` policy retains native capabilities for native RPC transfer eligibility. Storage, attachments, queues and durable Workflow data require data-only policies; they cannot persist an RPC resource. Contracts validate input before handler effects and resolved output before encoding. Validation is active in release output. Failures carry bounded semantic diagnostics without dumping payloads or credentials.

`native/data-map` is a shallow keyword projection with collision checks. D1 and service data retain their documented wrapper records. Workflow event/status/output maps use `:payload`, `:output`, `:status` and `:error`; status/type selectors are keywords. Running or waiting Workflow status can omit output. R2 metadata views contain public data fields and the original object handle; `body-used?` reads the live getter after consumption.

Service KV metadata encodes CLJS keyword data through the explicit versioned codec and rejects resources before invoking a write. The native JSON serializer additionally rejects cycles and BigInt and applies its metadata size limit; JSON metadata cannot preserve the richer storage/RPC structured-data domain. Use the explicit codec for lossless CLJS keyword values; the shared HTTP JSON codec emits ordinary REST JSON and projects received fields to keywords.

## Lifetimes

A Cell constructor associates one actual receiver with private context and synchronously establishes the native initialization gate. Context contains native state and env plus a per-activation atom. Ordinary Workers, named Workers and Workflows retain distinct context kinds. Env and native handles are not global caches. Native ctx.abort terminates an activation; handles must be reacquired for later events.

Async transactions receive the actual native transaction view; retaining it after the callback is invalid. A synchronous transaction checks for a thenable **inside** the native callback, before native commit. SQL cursors remain native iterators. Bounded collection/cardinality helpers drain before returning or throwing; there is no invented cursor close API. Cell KV iterators are realized before a subsequent list invalidates them.

HTTP uses Fast-Twitch's shared request/response conversions. `http/forward!` returns a preserved response map, and middleware edits the current Ring map before final native conversion. Request/Response bodies and AbortSignals remain native. The asset fixture retains unchanged response identity, edits headers through map middleware, and verifies the shared consumed-response guard. In the pinned runtime, text rereads use cached bytes and consuming an edited Response does not change the retained origin carrier’s `bodyUsed` flag; these observations remain native behavior. Celld v0.6.2 rejects Request `cache` fields at construction; the adapter rejects effective cache options after middleware and before request construction or body consumption. WebSocket upgrades use the shared Ring upgrade finalizer.

Resident WebSockets use shared listeners with once-only open/close delivery. Hibernating sockets store a versioned handler ID and attachment; native message/close/error entrypoints dispatch through the shared event projection. Reattachment does not replay open. Socket tags, native auto-responses and timestamps remain runtime-owned. An application owns any replay/cursor policy.

Missing or incompatible durable dispatch metadata closes with application code `4008` and reason `Invalid dispatch attachment`, without invoking the application callback. This policy close is the asynchronous diagnostic surface; throwing afterward would discard captured frames and make the native host close with its own error. Partial upgrade cleanup uses `4011`. These codes follow Celld 0.6.2's WebSocket close API, which accepts `1000` and `3000`–`4999`.

TCP sockets and container port sockets are invocation-scoped. `start-tls` returns a new synchronous socket handle and consumes the plaintext socket; handshake settlement remains on native `:opened`/`:closed` Promises. The local self-signed fixture verifies native certificate rejection, equal rejection identity, and once-only upgrade/consumption; it does not certify successful encrypted transfer. Reconnect for a later event. Half-close obtains, awaits and releases one writer; full close keeps the native terminal Promise. AbortSignal listeners are removed when the socket closes. Shared EventSource and MessagePort helpers retain handles and require explicit close. Celld rejects nonempty MessagePort transfer lists; cloneable native data is supported.

Workflow run replays from the beginning, so durable effects belong in step callbacks. Repeated step names, including empty names, are valid in loops. Step callbacks receive the original native attempt/config context. `:sensitive :output` is recognized by the native validator but execution is unavailable in v0.6.2; the wrapper rejects it before invoking a step. Native step defaults, retry policy, timeouts, pause/resume/restart and buffered events remain native. Worker queues and UTC cron are distinct from Cell alarms; no library scheduler, outbox or settlement ledger is installed.

Cell alarm handlers receive `:retry-count`, `:is-retrying?` and `:scheduled-time`. The retry flag projects the pinned runtime's `isRetry` field; it is false for the first native delivery. Times remain native absolute milliseconds.

Loaded Worker code is supplied explicitly. Authored module-map keys use keywords (for example, `:loaded.mjs`); the adapter projects their full spelling to native filenames. Filename fields such as `:mainModule` retain path strings. Typed arrays/wasm bytes and env capabilities remain native, and loading is lazy. `dispose!` prevents new calls while already-started native calls finish. Facets belong to their root and retain independent storage; parent transactions do not undo facet effects. Facet abort retains data and delete removes descendant databases.

Container disk is ephemeral. Monitor/exec do not keep an event alive after its handler returns. `set-inactivity-timeout!` returns a Promise and must be awaited to observe rejection. `output!` is one-shot and cannot follow locked stream consumption. Output modes are `:pipe`, `:ignore` and stderr `:combined`; the adapter projects these enums at the native boundary. Native process PID/kill effects depend on the engine; the evidence distinguishes adapter behavior from native or engine limitations.

## Native interop and exclusions

Standard Headers, URL, encoders, timers, console, crypto, WebAssembly, HTMLRewriter and supported partial Node modules use ordinary CLJS `:refer-global` and `:require-global` module declarations. For example, `(:require-global ["node:path" :as path])` selects the native ESM module; the mandatory build binds its namespace and rejects unsupported specifiers before publication. The API catalog names each interop path and its exact coverage state. `caches` has the pinned always-miss behavior; it must not be advertised as durable storage.

Node module inclusion means the module can be imported, not that every Node API is implemented. The [pinned compatibility documentation](https://github.com/denoland/celld/blob/90b43017241f81189453d326d05948f388b34652/docs/cloudflare-compat.md#nodejs-compatibility) and [module resolver](https://github.com/denoland/celld/blob/90b43017241f81189453d326d05948f388b34652/crates/celld/js/modules.rs) are the authorities for this subset:

| Module or global | Native limit |
| --- | --- |
| `node:crypto` | Diffie-Hellman, streaming signatures, ciphers, RSA-PSS and DSA signing/key generation are unavailable. |
| `node:zlib` | Synchronous gzip/deflate operations only. |
| `node:fs` and `node:fs/promises` | The implemented file methods expose Worker modules under read-only `/bundle` and an empty request-local `/tmp`; this is not host filesystem access. |
| `node:diagnostics_channel` | No tail-Worker message export. |
| `process` | Only the native defined fields exist; `kill` and `features` are unavailable. |
| Raw ESM | There is no global CommonJS `require`; use `:require-global`. |

The pinned resolver also exposes `node:test` and reporters, but module resolution does not certify an executable Node test runner. Its partial exports remain documented native interop, without a claim that every Node API executes. Unsupported module specifiers fail the mandatory application build rather than becoming an accidental host Node dependency.

The catalog excludes operator/fleet authority, unsupported Worker events and services, private source-only SQL prepare/ingest, unsupported jurisdiction/encryption/snapshot/interception APIs, and ineffective flags/options. Native passThroughOnException has no CDN fallback; KV cacheTtl and positive container hardTimeout have no effective implementation in this target. Namespace/Workflow location hints do not establish fleet placement. No raw resource is silently converted with deep `js->clj`/`clj->js`.
