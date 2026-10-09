# Observed compatibility

This is an in-progress Phase 1 implementation. The complete C19 gate has not
passed. The [qualified catalog](api-catalog.edn), [case coverage](coverage.json)
and [retained evidence](qualification-evidence.json) define the exact scope.

| Input or target | Exercised version | Qualification |
| --- | --- | --- |
| Celld source | v0.6.2, `90b43017241f81189453d326d05948f388b34652` | Native Mac fixture families; experimental container fixture in an existing rootful Podman VM with official Linux ARM binary. |
| CLJS | `56a94ac13bfac22108e4d868dfd4763edaceb864` | Modern dictionaries, explicit globals and native async/await in simple release output. |
| JDK | OpenJDK 27; minimum 21 | Application compiler and fresh-JVM development/rebuild commands. |
| Clojure CLI | 1.12.6.1673 | Local build and test entry commands. |
| Node | 26.10.0 | Locked bundler and controlled local fixture drivers. |
| esbuild | 0.25.12 | ESM output with whitespace minification; unchanged builds are byte-identical. |
| Malli | 0.20.2 | Static/portable and release runtime contracts. |
| HoneySQL | 2.7.1479 | Native Cell SQL formatting and separate D1 statement binding. |
| Fast-Twitch | `a9f96bf90d0ded9282ac80a0f7fb808f1e5c719a` | Shared native HTTP/WS/SSE/TCP/port/stream contracts; published Git dependency; source contents match the previously verified local bridge changes. |
| Podman | 6.1.3, existing rootful VM | Fourteen container assertions passed; process kill remains unqualified. |
| Compiler optimization | `:simple` | Passed; advanced is explicitly rejected. |

The compatibility date is `2026-10-07`. Supported Node modules remain partial;
only specific recorded native assertions are qualified. The native platform,
fixture commands and workload budgets are local observations, not universal
application latency, throughput or cross-platform guarantees.

The final bridge corrections preserve native lifecycle semantics: accepted
resident open fires once, alarm retry metadata uses Celld's `isRetry`, invalid
or missing durable socket metadata closes with valid application code `4008`,
and partial upgrade cleanup uses `4011`. Build generations include relevant
source/configuration fingerprints and cannot overwrite an existing manifest.

No deployment, provisioning or Phase 2 work is
part of this implementation record.
