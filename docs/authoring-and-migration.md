# Application authoring and migration

The public compiler command is the application boundary. Select an entry namespace
and its source roots in `app.edn`, declare the native classes in that selected
graph, and build with `bin/celld-clojure -M:build build app.edn`. The published
Fast-Twitch Git dependency is pinned in `deps.edn`. The optional `:local` alias
selects `../fast-twitch` for development against a sibling checkout.

## Adopting class history

Keep each Cell's native export and binding explicit and stable. A ClojureScript
symbol or namespace may change while `:export` and `:binding` retain native
identity. Changing the native export creates a different class identity; it is
not a storage migration by itself.

Run `prepare-config` to write a candidate history for new SQL-backed Cell
classes. Review that candidate and explicitly adopt it as the configured history
file. The ordinary `build` command rejects an unadopted class and does not rewrite
the adopted history. It derives no domain tables or seed data. Application SQL
schema changes belong in explicit application initialization or migration code.

An existing JSON/JSONC native configuration uses `:native-config` and remains the
configuration authority. Its resource paths are relative to that native file.
Generated mode derives exports, bindings, RPC flags and Workflow/cron declarations
from the selected graph. Every supplied profile is validated before publication.
The final `.celld-build/wrangler.json` selects one complete immutable generation;
run that file, not an intermediate compiler output.

## Composing declarations

Focused declarations become native methods only when selected in `:include`.
`defcell` defines and exports the actual CLJS constructor. One initializer receives
`[ctx]`; RPC receives `[ctx & arguments]`; Cell/Worker fetch receives
`[ctx request]`. Queue, scheduled and Workflow handlers keep their native kind
and cannot be included as Cell events. Ordinary function/map forms use the same
descriptor checks as the focused macros.

Generated RPC clients live under the generation's `generated-src` directory.
Add that directory as a source root to a separate consumer and require the
generated namespace. Each function takes the native stub first and retains the
server's explicit native method spelling, schema and codec. Clients do not import
the application implementation namespace and do not substitute HTTP for RPC.

## Persisted data and version changes

Native structured data and the explicit `:json` envelope are separate domains.
Default/native policy retains native objects, buffers and supported structured
types. A CLJS persistent collection requires explicit `:json` selection, whose
version-2 JSON text preserves keyword keys and values, including namespaces.
The byte budget includes the encoded UTF-8 tags/header. Only version-2 envelopes are supported. Authored payload maps require keyword keys. Unsupported envelope versions fail before returning a value.

Keep the selected codec consistent across writes and reads. To change a stored
representation, read the old version explicitly and write the new representation
through an application-owned migration. Do not reinterpret a missing key as a
stored null. Never persist a live RPC, stream, socket, transaction or container
capability as application data.

Workflow code replays from its beginning; external durable effects belong inside
native steps. Changing names or step order requires an application replay/version
policy. The library does not serialize closures or pending promises, and does not
install a workflow scheduler. Repeated native names in valid loops are supported.

## KV and SQL authoring

Cell KV keys, batch-map keys, list records and range/prefix selectors use keywords
or namespaced keywords. Native storage receives the full spelling and lists
restore that spelling as keywords. Opaque continuation cursors remain strings.

Cell `sql/exec` and D1 `prepare` accept HoneySQL clause maps and an optional
keyword parameter map. Use `[:param :tenant/id]` references for statement values;
SQL text and positional arguments exist only inside the native boundary. DDL uses
HoneySQL `:create-table` / `:with-columns`. Raw SQL and `:raw` escapes are rejected.
Computed result columns require keyword aliases. Namespaced aliases use a
reversible native identifier, so two aliases such as `:user/id` and `:tenant/id`
remain distinct in returned rows. Cell execution and cursor draining stay synchronous.
D1 `exec!` accepts unparameterized HoneySQL DDL; use `prepare` and `run!` for values.

## Local development

`dev` starts a supervised native process after a successful build and rebuilds in
a fresh JVM when selected source/configuration/dependency inputs change. An
invalid revision is visibly stale and keeps the previous serving generation.
After correction, a successful build restarts the owned process while preserving
local durable state. `clean` removes generated build output only; adopted history,
dotenv inputs and `.celld/dev` state survive.

Advanced optimization is unavailable. Consult [qualification](qualification.md)
before relying on a member or overload: this checkout remains an in-progress
Phase 1 implementation, with unresolved native container process control and
member-level release gates.
