# Public ClojureScript API

Target: Celld v0.6.2. Source signatures describe implemented wrappers. [Native semantics](native-semantics.md), the member catalog and [qualification](qualification.md) define the limits of each verified case.

## fast-twitch.celld.alarms

### `set-at!`

```clojure
(set-at! storage timestamp)
```

Returns the native setAlarm Promise for an absolute millisecond timestamp or Date. Facet alarms reject natively.

### `set-after!`

```clojure
(set-after! storage duration-ms)
```

Schedules after a positive duration in milliseconds, explicitly.

### `get-time!`

```clojure
(get-time! storage)
```

Returns native absolute milliseconds or nil when absent.

### `delete!`

```clojure
(delete! storage)
```

Returns the native deleteAlarm Promise; removes the one Cell alarm.

## fast-twitch.celld.bindings

### `get-binding`

```clojure
(get-binding context binding)
```

Returns the named native env capability or fails before effects when missing. Keyword identities retain namespaces through native identifier projection.

### `native`

```clojure
(native context binding)
```

Returns the same native binding capability without conversion.

## fast-twitch.celld.codec

### `native-value!`

```clojure
(native-value! value)
(native-value! value depth)
```

Checks native structured data, including BigInt and cycles, without cloning resources or losing identity.
  CLJS collections/functions and unsupported capabilities require an explicit operation policy.

### `rpc-value!`

```clojure
(rpc-value! value)
```

Explicit native RPC domain: keeps live native capabilities, while rejecting
  CLJS collections/functions and imprecise numbers. Native RPC enforces transfer
  eligibility; this policy is never accepted by storage or Workflow persistence.

### `write-json`

```clojure
(write-json value)
```

Serializes CLJS JSON data losslessly using version 2 and a UTF-8 byte budget.

### `read-json`

```clojure
(read-json text)
```

Reads version-2 CLJS JSON text or interoperable plain JSON.

### `encode`

```clojure
(encode policy value)
```

Encodes an explicitly selected native/JSON/RPC boundary value; storage callers require a data-only policy.

### `decode`

```clojure
(decode policy value)
```

Decodes checked native data or strict version-2 JSON text.

### `persistence-policy!`

```clojure
(persistence-policy! policy)
```

Checks a data-only policy before persistent reads or effects; RPC capabilities cannot be persisted.

## fast-twitch.celld.communication

### `adapt-socket`

```clojure
(adapt-socket socket)
(adapt-socket socket options)
```

Retains native socket/readable/writable handles. Half-close owns one writer
  only for its call; full close preserves the native socket's terminal Promise.

### `check-options!`

```clojure
(check-options! options)
```

Checks shared TCP options before opening any native socket; an already-aborted
  signal throws its native reason before connection effects.

### `connect!`

```clojure
(connect! address)
(connect! address native-options shared-options)
```

Uses Celld's documented cloudflare:sockets connector, synchronously returning
  the shared connection map. Reconnect in each later event; this is not durable I/O.

### `start-tls`

```clojure
(start-tls connection)
(start-tls connection options)
```

Consumes a native starttls socket and returns its new shared TLS connection.

### `native-socket`

```clojure
(native-socket connection)
```

Returns the actual event-scoped TCP socket.

### `event-source!`

```clojure
(event-source! options)
```

Uses shared native EventSource options/reconnection; retain and explicitly close its handle.

### `message-channel!`

```clojure
(message-channel!)
(message-channel! first-options second-options)
```

Creates shared port handles with explicit codecs. Celld v0.6.2 rejects nonempty transfer lists; cloned data is supported. Close owned ports.

## fast-twitch.celld.context

### `activate!`

```clojure
(activate! instance ctx env initializer)
(activate! instance ctx env initializer kind)
```

Associates one actual instance with ctx/env and establishes initialization gating
  synchronously. The constructor itself returns no Promise.

### `of`

```clojure
(of instance)
```

Returns the private activation context for the actual Cell receiver; rejects an unrelated instance synchronously.

### `invocation`

```clojure
(invocation context event)
```

Adds the current native event to an explicit context value; does not install dynamic/global context.

### `worker`

```clojure
(worker env ctx)
```

Creates invocation context from native Worker env and execution context; no binding is cached.

### `native`

```clojure
(native context)
```

Returns the original native state/execution context from a checked context map.

### `env`

```clojure
(env context)
```

Returns this invocation/activation env without serialization.

### `storage`

```clojure
(storage context)
```

Returns the live native Cell storage handle; unavailable in ordinary Workers.

### `id`

```clojure
(id context)
```

Returns the native Cell ID, retaining optional native name semantics.

### `props`

```clojure
(props context)
```

Returns the native startup properties for this activation.

### `exports`

```clojure
(exports context)
```

Returns native loopback export capabilities for this context.

### `facets`

```clojure
(facets context)
```

Returns the native Cell facet registry, scoped to its root object.

### `container`

```clojure
(container context)
```

Returns the experimental native container handle when configured.

### `wait-until!`

```clojure
(wait-until! context promise)
```

Registers a Promise with the native invocation context, synchronously. Native rejection and lifetime semantics remain intact.

### `block-concurrency!`

```clojure
(block-concurrency! context callback)
```

Establishes a native input gate immediately and invokes the callback in that gate. It does not make arbitrary awaits atomic.

### `abort!`

```clojure
(abort! context reason)
```

Aborts this native Cell activation with the supplied reason; future handle use follows native terminal semantics.

## fast-twitch.celld.core

### `native-context`

```clojure
(native-context ctx)
```

Returns the actual native state/execution context synchronously.

### `binding`

```clojure
(binding ctx name)
```

Checks and returns this explicit context's native binding without caching it.

## fast-twitch.celld.http

### `client-for`

```clojure
(client-for binding)
(client-for binding options)
```

Returns a Fast-Twitch client for a native fetch capability; keeps its receiver.
  AbortSignal and current request edits pass through the existing converter.

### `forward!`

```clojure
(forward! binding request)
```

Forwards the current request map through the native binding using shared conversion; resolves to a preserved response map.

### `handle!`

```clojure
(handle! handler context native native?)
```

Invokes one map handler, retaining the native Request in Fast-Twitch metadata.
  Native mode passes the native Request directly. All results share finalization.

### `mount`

```clojure
(mount request path)
```

Edits the current Ring URI while retaining native request metadata and body ownership.

## fast-twitch.celld.identity

### `id-from-name`

```clojure
(id-from-name namespace value)
```

Derives a native namespace-scoped ID from the full keyword/string name, synchronously.

### `id-from-string`

```clojure
(id-from-string namespace value)
```

Validates an opaque native ID string in this namespace, synchronously.

### `new-unique-id`

```clojure
(new-unique-id namespace)
```

Creates a random native ID synchronously; jurisdiction restrictions are unsupported by Celld.

### `get-stub`

```clojure
(get-stub namespace id)
(get-stub namespace id options)
```

Returns a lazy native stub for an ID. This call does not activate the Cell.

### `get-by-name`

```clojure
(get-by-name namespace value)
(get-by-name namespace value options)
```

Returns a lazy stub for a native name (keyword namespaces are retained); native ID derivation remains the default.

### `id-string`

```clojure
(id-string id)
```

Returns the complete opaque native ID string synchronously.

### `id-name`

```clojure
(id-name id)
```

Returns the optional native name; long names may be absent even when routing succeeds.

### `equals?`

```clojure
(equals? id other)
```

Compares native IDs through their original receiver.

### `stub-id`

```clojure
(stub-id stub)
```

Returns the live native ID associated with a namespace stub.

## fast-twitch.celld.native

### `invoke`

```clojure
(invoke receiver member args)
```

Invokes once with the original receiver and exact argument count; preserves native sync/Promise result and rejection.

### `property`

```clojure
(property receiver member)
```

Reads a live native property synchronously without cloning; getter failures propagate.

### `fields`

```clojure
(fields value)
```

Projects keyword field names losslessly; rejects native-key collisions and preserves values/handles.

### `data-map`

```clojure
(data-map value)
(data-map value key-fn)
```

Shallow keyword projection preserving native handles; accepts a shared keyword selector for encoded aliases.

## fast-twitch.celld.rpc

### `serve!`

```clojure
(serve! handler context args options)
```

Decodes application arguments, checks before effects, awaits once, then checks and encodes successful output; rejects unchanged.

### `call!`

```clojure
(call! stub method args & [options])
```

Calls one stable native method. Contract schemas describe application values.

## fast-twitch.celld.services.assets

### `native`

```clojure
(native binding)
```

Returns the original native Fetch binding.

### `client-for`

```clojure
(client-for binding)
(client-for binding options)
```

Returns the receiver-preserving Fast-Twitch client for an assets or service binding.

### `fetch!`

```clojure
(fetch! binding request)
```

Forwards the current request map with native origin/stream/upgrade preservation.

## fast-twitch.celld.services.containers

### `native`

```clojure
(native handle)
```

Returns the original native container/process/port handle.

### `running?`

```clojure
(running? container)
```

Reads native engine state synchronously, without caching a local flag.

### `start!`

```clojure
(start! container & [options :as supplied])
```

Starts synchronously; native engine startup errors surface through monitor.
  Positive hardTimeout is accepted but ineffective natively and excluded here.

### `monitor!`

```clojure
(monitor! container)
```

Returns the native exit Promise. It is owned by the current invocation.

### `destroy!`

```clojure
(destroy! container & [error :as supplied])
```

Stops the native container; optional error is preserved as the monitor rejection.

### `signal!`

```clojure
(signal! container number)
```

Synchronously requests a native numeric signal; native engine errors remain native.

### `tcp-port`

```clojure
(tcp-port container port)
```

Returns a native container port capability synchronously.

### `client-for`

```clojure
(client-for port)
(client-for port options)
```

Selects receiver-preserving Fast-Twitch Fetch for a native port; use HTTP URLs.

### `connect!`

```clojure
(connect! port)
(connect! port options shared-options)
```

Returns the shared TCP representation of a native event-scoped port socket.

### `exec!`

```clojure
(exec! container command & [options :as supplied])
```

Returns a Promise of a native process with explicit stdin/stdout/stderr modes.

### `set-inactivity-timeout!`

```clojure
(set-inactivity-timeout! container duration)
```

Returns the native Promise setting the positive idle window in milliseconds; await to observe rejection.

### `process-map`

```clojure
(process-map process)
```

Projects live process streams/PID/exit Promise; consuming them remains caller-owned.

### `output!`

```clojure
(output! process)
```

Returns native whole process output once; cannot follow separate stream consumption.

### `kill!`

```clojure
(kill! process & [signal :as supplied])
```

Synchronously requests a process signal; omitted native signal defaults to 15.

## fast-twitch.celld.services.cron

### `controller-map`

```clojure
(controller-map controller)
```

Returns the original controller plus cron text and absolute scheduled milliseconds.

### `no-retry!`

```clojure
(no-retry! controller)
```

Synchronously suppresses retries for this native scheduled occurrence.

## fast-twitch.celld.services.d1

### `native`

```clojure
(native handle)
```

Returns the native database, session or statement handle.

### `prepare`

```clojure
(prepare database query)
(prepare database query params)
```

Creates and binds a native D1 statement from HoneySQL data synchronously.
  Optional keyword params resolve named placeholders; all values validate before prepare.

### `all!`

```clojure
(all! statement)
```

Returns a checked async result map with keyword row columns.

### `run!`

```clojure
(run! statement)
```

Returns native success/meta/rows without manufacturing a transaction.

### `first!`

```clojure
(first! statement)
(first! statement column)
```

No column gives a keyword row or nil. Column form returns its native value;
  a missing column rejects natively, while an absent row remains nil.

### `raw!`

```clojure
(raw! statement)
(raw! statement options)
```

Returns vectors in native column order; :columnNames prepends keyword selectors.

### `batch!`

```clojure
(batch! database statements)
```

Runs native ordered batch/atomicity and preserves each result's metadata.

### `exec!`

```clojure
(exec! database query)
```

Runs unparameterized HoneySQL DDL through native exec and returns keyword metadata.
  Queries carrying values use prepare/run! so D1 binds parameters natively.

### `with-session`

```clojure
(with-session database)
(with-session database constraint-or-bookmark)
```

Returns a native session synchronously. Bookmark/constraint omission is preserved.

### `bookmark`

```clojure
(bookmark session)
```

Returns the native opaque bookmark or nil before a successful session query.

## fast-twitch.celld.services.facets

### `native`

```clojure
(native handle)
```

Returns the original native facet registry/stub/class handle.

### `startup`

```clojure
(startup class)
(startup class id)
```

Builds native {class,id?}; class must come from a native loader or unmigrated
  ctx.exports class. Omitted ID inherits root ID and retains its name.

### `specialize-class`

```clojure
(specialize-class class props)
```

Copies native startup props through the native loopback class specialization.

### `get-facet`

```clojure
(get-facet registry name startup-callback)
```

Returns a lazy native facet stub; startup executes only when a new facet starts.

### `abort!`

```clojure
(abort! registry name)
(abort! registry name reason)
```

Stops the named native facet synchronously while retaining its database; omission of a reason is preserved.

### `delete!`

```clojure
(delete! registry name)
```

Returns native deletion Promise; deletes the facet database and descendant databases.

## fast-twitch.celld.services.kv

### `native`

```clojure
(native binding)
```

Returns the original native service KV namespace.

### `get!`

```clojure
(get! binding key & [options])
```

Returns native text/JSON/bytes/stream or nil. Bulk maps retain keyword keys and null holes.
  JSON mode projects received fields to keywords; lossless CLJS decoding uses get-json!.

### `get-with-metadata!`

```clojure
(get-with-metadata! binding key & [options])
```

Returns value/metadata/cacheStatus data, retaining native streamed values.

### `put!`

```clojure
(put! binding key value & [options :as supplied])
```

Writes string/native bytes/stream, preserving ownership. Expiration units are seconds.
  CLJS metadata uses versioned JSON; native metadata stays native. All values are checked before put.

### `put-json!`

```clojure
(put-json! binding key value options)
```

Explicitly serializes supported CLJS JSON to text before writing.

### `get-json!`

```clojure
(get-json! binding key)
```

Decodes lossless CLJS JSON text; missing and stored nil both return nil.

### `delete!`

```clojure
(delete! binding key)
```

Returns the native delete Promise for a keyword key.

### `list!`

```clojure
(list! binding & [options])
```

Returns keyword metadata records and an opaque native continuation cursor.

## fast-twitch.celld.services.loaders

### `native`

```clojure
(native handle)
```

Returns the original native loader/stub/class/entrypoint capability.

### `code`

```clojure
(code options)
```

Checks WorkerCode and projects option keys. Module names are keyword keys; modules
  may be native {wasm/data/...} records. Capabilities in env remain native.

### `get-worker`

```clojure
(get-worker loader id get-code)
```

Synchronously returns a lazy native named load. get-code executes only on native
  first compilation, so callback failures appear at first use, not necessarily here.

### `load`

```clojure
(load loader native-code)
```

Synchronously returns an anonymous native load, preserving native lazy Promise errors.

### `get-entrypoint`

```clojure
(get-entrypoint worker)
(get-entrypoint worker name)
(get-entrypoint worker name options)
```

Returns a native Fetch/RPC entrypoint with checked props/limits; omission is preserved.

### `get-cell-class`

```clojure
(get-cell-class worker)
(get-cell-class worker name)
(get-cell-class worker name options)
```

Returns the native plain-Cell class handle used by facets. Only props is supported.

### `dispose!`

```clojure
(dispose! worker)
```

Explicitly releases the native load synchronously; do not persist derived handles.

## fast-twitch.celld.services.queues

### `native`

```clojure
(native handle)
```

Returns the native queue/message/batch handle.

### `send!`

```clojure
(send! queue value & [options])
```

Returns native committed-send Promise. :codec checks/encodes the body once;
  :contentType selects native serialization. Native structured data is the default.

### `send-batch!`

```clojure
(send-batch! queue messages & [options])
```

Encodes/validates every entry before invoking native sendBatch once. Returns its Promise.

### `metrics!`

```clojure
(metrics! queue)
```

Returns Celld's native queue metrics as a shallow data map.

### `message-map`

```clojure
(message-map message)
(message-map message policy)
```

Projects ID/timestamp/attempts while retaining the native settlement capability.

### `batch-map`

```clojure
(batch-map batch policy)
```

Projects one native batch with explicit body codec; settlement handles remain native.

### `ack!`

```clojure
(ack! message)
```

Settles one native message synchronously; first settlement wins in Celld.

### `retry!`

```clojure
(retry! message & [options :as supplied])
```

Synchronously requests native retry with optional delay in seconds.

### `ack-all!`

```clojure
(ack-all! batch)
```

Synchronously acknowledges the native batch's unsettled messages.

### `retry-all!`

```clojure
(retry-all! batch & [options :as supplied])
```

Synchronously requests native retry with optional delay in seconds.

## fast-twitch.celld.services.r2

### `native`

```clojure
(native handle)
```

Returns the actual bucket/object/multipart handle.

### `object-map`

```clojure
(object-map object)
```

Projects object metadata while retaining the original object and body stream.

### `head!`

```clojure
(head! bucket key)
```

Returns metadata or nil without consuming any body.

### `get!`

```clojure
(get! bucket key & [options])
```

Returns {:state :missing/:condition-unmet/:found :object ...}. Only :found has a body.

### `put!`

```clojure
(put! bucket key value & [options])
```

Returns the stored native metadata, or nil when a native write precondition refuses it.

### `delete!`

```clojure
(delete! bucket keys)
```

Deletes one key or a checked vector of keys; returns the native Promise.

### `list!`

```clojure
(list! bucket & [options])
```

Returns streamed-object metadata pages with :truncated? and an opaque cursor when supplied.

### `create-multipart!`

```clojure
(create-multipart! bucket key & [options :as supplied])
```

Returns a Promise of a native multipart handle. Checksums/conditions are unsupported here.

### `resume-multipart`

```clojure
(resume-multipart bucket key upload-id)
```

Returns a native handle immediately; missing/lost uploads reject on first use.

### `upload-part!`

```clojure
(upload-part! upload number value)
```

Uploads one checked part number and native body; returns native part metadata.

### `complete!`

```clojure
(complete! upload parts)
```

Completes in native part order using checked {partNumber,etag} records.

### `abort!`

```clojure
(abort! upload)
```

Aborts the native upload and preserves rejection.

### `body`

```clojure
(body object)
```

Returns the original native readable body; consuming it is caller-owned.

### `write-http-metadata!`

```clojure
(write-http-metadata! object headers)
```

Writes object-owned HTTP metadata into the supplied native Headers.

### `array-buffer!`

```clojure
(array-buffer! object)
```

Explicit native whole-body consumption; native ownership/rejection applies.

### `bytes!`

```clojure
(bytes! object)
```

Explicit native bytes consumption; prefer bounded shared readers for untrusted bodies.

### `text!`

```clojure
(text! object)
```

Explicit native UTF-8 text consumption.

### `json!`

```clojure
(json! object)
```

Consumes native JSON and projects received object fields to keyword data.

### `blob!`

```clojure
(blob! object)
```

Explicit native Blob consumption.

### `body-used?`

```clojure
(body-used? object)
```

Reads the live native bodyUsed getter; does not consume or clone the body.

### `upload-map`

```clojure
(upload-map upload)
```

Projects multipart key/uploadId and retains its live native handle.

## fast-twitch.celld.services.workflows

### `duration-ms`

```clojure
(duration-ms value)
```

Checks Celld's numeric milliseconds or fixed native duration string dialect.

### `run!`

```clojure
(run! handler context event step options)
```

Workflow declaration boundary: projects native metadata with keyword keys, decodes payload, validates
  :args against payload before handler effects, then checks/encodes resolved output.

### `native`

```clojure
(native handle)
```

Returns the live Workflow binding, instance or step capability.

### `create!`

```clojure
(create! binding & [options :as supplied])
```

Returns a native instance Promise, with native generated ID when omitted.
  :codec selects params encoding; native structured data is the default.

### `create-batch!`

```clojure
(create-batch! binding options)
```

Creates 1–100 native instances after checking and encoding every entry's params with its :codec.

### `get!`

```clojure
(get! binding id)
```

Looks up an existing instance by stable native string ID; absent instances reject.

### `delete-batch!`

```clojure
(delete-batch! binding ids)
```

Returns native per-ID deletion/errors for 1–100 IDs without retries.

### `id`

```clojure
(id instance)
```

Returns the native instance ID synchronously.

### `status!`

```clojure
(status! instance & [policy :as supplied])
```

Returns native status/output/error; an optional codec decodes present output.

### `send-event!`

```clojure
(send-event! instance options)
```

Sends a keyword event type and optional payload encoded with :codec; buffering remains native.

### `pause!`

```clojure
(pause! instance)
```

Returns the native pause Promise.

### `resume!`

```clojure
(resume! instance)
```

Returns the native resume Promise.

### `restart!`

```clojure
(restart! instance & [options :as supplied])
```

Restarts through native instance control; native supported options remain explicit.

### `terminate!`

```clojure
(terminate! instance)
```

Terminates natively. Rollback options are unavailable in this target.

### `delete!`

```clojure
(delete! instance)
```

Deletes natively and preserves native success/error handling.

### `do!`

```clojure
(do! step name callback & [options])
```

Runs one durable step; defaults/retries/replay are native. The callback receives
  native step/attempt/config context; its awaited result is encoded before persistence.

### `sleep!`

```clojure
(sleep! step name duration)
```

Durably sleeps with native duration units/string dialect; returns its Promise.

### `sleep-until!`

```clojure
(sleep-until! step name timestamp)
```

Durably sleeps until native milliseconds or Date; returns its Promise.

### `wait-for-event!`

```clojure
(wait-for-event! step name options)
```

Returns a keyword event map with payload decoded using :codec; buffering remains native.

### `non-retryable-error`

```clojure
(non-retryable-error message & [name])
```

Constructs the native permanent Workflow error synchronously, with optional name.

## fast-twitch.celld.sql

### `native`

```clojure
(native storage)
```

Returns storage.sql, the live synchronous SQL receiver.

### `exec`

```clojure
(exec sql query)
(exec sql query params)
```

Executes HoneySQL query/DDL data synchronously, returning the actual native cursor.
  Optional keyword params resolve HoneySQL named parameters before any native effect.

### `cursor-native`

```clojure
(cursor-native cursor)
```

Returns the actual live native cursor. It has no public close operation.

### `metadata`

```clojure
(metadata cursor)
```

Projects ordered column names and current row counters; no rows are consumed.

### `database-size`

```clojure
(database-size sql)
```

Returns the native database size in bytes synchronously.

### `next-row`

```clojure
(next-row cursor)
```

Steps one native row; returns :done? or keyword :row data synchronously.

### `raw`

```clojure
(raw cursor)
```

Returns the native raw-row iterator. Consume while the cursor is valid.

### `reduce-rows`

```clojure
(reduce-rows cursor reducer initial)
```

Reduces synchronously. Reduced values stop the reducer but still drain the native
  cursor so write/RETURNING output is safe. A throwing reducer drains remaining rows while preserving its primary error.

### `all-rows`

```clojure
(all-rows cursor)
(all-rows cursor limit)
```

Collects at most limit rows, drains any excess before reporting a cardinality error.
  Prefer a HoneySQL :limit for large queries; draining is required for write cursors.

### `zero-or-one`

```clojure
(zero-or-one cursor)
```

Drains the cursor and returns nil or one row; excess rows fail after drain.

### `exactly-one`

```clojure
(exactly-one cursor)
```

Drains and requires exactly one row, including write RETURNING completion.

### `drain!`

```clojure
(drain! cursor)
```

Consumes every row without retaining data, then returns final native row counters.

## fast-twitch.celld.storage

### `native`

```clojure
(native storage)
```

Returns the original native storage or transaction view.

### `get!`

```clojure
(get! storage key & [options])
```

Reads one key as {:found? ... :value ...}, or vector of keys as a keyword-keyed
  map containing only present entries. :codec selects :native or versioned :json.

### `put!`

```clojure
(put! storage key value & [options])
```

Writes one keyword key/value after encoding. Native optional storage flags have no effect in this target.

### `put-many!`

```clojure
(put-many! storage entries & [options])
```

Encodes an entire keyword-keyed batch before invoking native put; returns its Promise.

### `delete!`

```clojure
(delete! storage key)
```

Deletes a keyword key (boolean) or vector of keys (count), preserving native results.

### `list-entries!`

```clojure
(list-entries! storage & [options])
```

Realizes native traversal order as a vector of [keyword decoded-value] pairs.
  Reverse/range/limit order is retained beyond the CLJS lookup-map threshold.

### `list!`

```clojure
(list! storage & options)
```

Returns a keyword-keyed lookup map. Map traversal order is unspecified;
  use list-entries! for ordered reverse/range results.

### `delete-all!`

```clojure
(delete-all! storage)
```

Returns the native deleteAll Promise; alarm deletion follows the pinned compatibility flag.

### `sync!`

```clojure
(sync! storage)
```

Waits for native durability; rejects inside a transaction or after abort.

### `transaction!`

```clojure
(transaction! storage callback)
```

Calls callback with the actual async transaction view; do not retain it.

### `transaction-sync!`

```clojure
(transaction-sync! storage callback)
```

Calls a zero-argument callback synchronously, checking its result inside native scope.

### `rollback!`

```clojure
(rollback! transaction)
```

Rolls back the actual native transaction view; terminal use rejects natively.

## fast-twitch.celld.storage.kv

### `native`

```clojure
(native storage)
```

Returns storage.kv, the synchronous native view of Cell storage.

### `get-value`

```clojure
(get-value kv key & [policy])
```

Synchronously reads a keyword key; tagged :found? distinguishes undefined from null.

### `put!`

```clojure
(put! kv key value & [policy])
```

Synchronously writes one keyword key and encoded native/JSON value; returns the native result.

### `delete!`

```clojure
(delete! kv key)
```

Synchronously deletes one key and returns the native boolean.

### `list-native`

```clojure
(list-native kv & [options :as supplied])
```

Returns the live iterator. A subsequent list on this object invalidates it.

### `list-values`

```clojure
(list-values kv & [options policy :as supplied])
```

Realizes keyword/value pairs while the native iterator is valid; optional codec decodes values.

## fast-twitch.celld.validation

### `fail!`

```clojure
(fail! operation boundary repair)
(fail! operation boundary repair details)
```

Throws a bounded structured diagnostic with known identity and optional native cause.

### `at!`

```clojure
(at! details f)
```

Adds a known boundary identity to synchronous contract failures, retaining their cause.
  Foreign/native exceptions propagate unchanged.

### `check!`

```clojure
(check! schema value operation)
(check! schema value operation details)
```

Checks release-time data before effects and reports a bounded schema location/identity.

### `method!`

```clojure
(method! receiver member)
```

Checks a live callable member without invoking it; callers must retain its receiver.

### `thenable?`

```clojure
(thenable? value)
```

Detects Promise-compatible returns at synchronous callback boundaries.

### `synchronous!`

```clojure
(synchronous! value operation)
```

Rejects thenables inside the native synchronous callback before successful commit.

### `safe-number!`

```clojure
(safe-number! value operation)
```

Rejects nonfinite numbers and integers outside the exact native number range.

## fast-twitch.celld.websocket

### `native`

```clojure
(native handle)
```

Returns the shared connection's original socket or an already-native socket.

### `pair`

```clojure
(pair)
```

Creates the native pair synchronously, returning :client and :server native sockets.

### `accept!`

```clojure
(accept! ctx socket & [tags :as supplied])
```

Accepts through ctx.acceptWebSocket exactly once; tags are native string vectors.
  Hibernating mode must have generated stable lifecycle handlers.

### `sockets`

```clojure
(sockets ctx & [tag :as supplied])
```

Returns shared native connection handles; optional tag omission is preserved.

### `tags`

```clojure
(tags ctx socket)
```

Returns the native accepted socket tags synchronously.

### `serialize-attachment!`

```clojure
(serialize-attachment! socket value & [policy])
```

Stores an explicit native/JSON codec envelope; resources/functions are rejected.

### `deserialize-attachment`

```clojure
(deserialize-attachment socket & [policy])
```

Returns the decoded native attachment. Missing native data remains nil.

### `set-auto-response!`

```clojure
(set-auto-response! ctx)
(set-auto-response! ctx request response)
```

Installs a native pair or clears it by omitted argument; UTF-8 sides are bounded natively.

### `auto-response`

```clojure
(auto-response ctx)
```

Returns native auto-response pair data, or nil when unset.

### `auto-response-timestamp`

```clojure
(auto-response-timestamp ctx socket)
```

Returns the native Date or nil; no timestamp is fabricated.

### `attach-dispatch!`

```clojure
(attach-dispatch! socket descriptor value policy)
```

Writes stable dispatch ID/version plus application attachment payload before acceptance.

### `install-handlers!`

```clojure
(install-handlers! constructor descriptor)
```

Installs stable native lifecycle methods on the actual Cell constructor.
  Rehydration reads dispatch metadata; no on-open is replayed and no listener is added.

### `upgrade-capability`

```clojure
(upgrade-capability _ctx)
```

Supplies the native Ring resident upgrade capability. A hibernating socket uses
  explicit pair/attachment/accept instead, avoiding duplicate resident dispatch.

## fast-twitch.celld.worker

### `named-constructor`

```clojure
(named-constructor)
```

Returns the actual native WorkerEntrypoint-derived CLJS constructor; no separate Cell instance is created.

### `workflow-constructor`

```clojure
(workflow-constructor)
```

Returns the actual WorkflowEntrypoint-derived CLJS constructor with receiver-associated context.

## Declaration macros

`defcontract` records a portable literal schema. `defcell-init`, `deffetch`, `defrpc`, `defalarm`, `defqueue-handler`, `defscheduled-handler` and `defwebsocket-handlers` define focused declarations selected by an owner’s `:include`. `defcell`, `defworker` and `defworkflow` emit direct exports. Require `with-transaction-sync` from `fast-twitch.celld.storage` with `:refer-macros`; it checks thenables inside the native callback.

Initialization takes `[ctx]`; fetch `[ctx request]`; RPC `[ctx & arguments]`; alarm `[ctx alarm-info]`; queue/scheduled `[ctx native-event]`; Workflow `[ctx event step]`. Literal keys, scope, names, codec/schema references, arity and export/binding/event identity are mandatory checked before publication.
