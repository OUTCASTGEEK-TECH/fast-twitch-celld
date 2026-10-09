(ns fast-twitch.celld.communication
  "Celld's event-scoped outbound TCP capability adapted to shared Fast-Twitch
  operations. SSE/EventSource/streams/ports use the existing shared namespaces."
  (:require [fast-twitch.celld.contracts :as contracts]
            [malli.experimental :as mx]
            [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.util.tcp :as tcp]
            [fast-twitch.client.sse.event-source :as event-source]
            [fast-twitch.client.messaging :as messaging])
  (:require-global ["cloudflare:sockets" :as sockets]))

(mx/defn ^{:dynamic true} adapt-socket
  "Retains native socket/readable/writable handles. Half-close owns one writer
  only for its call; full close preserves the native socket's terminal Promise."
  ([socket]
   (adapt-socket socket {}))
  ([socket options :- (:tcp-adapt contracts/schemas)]
   (let [writable (n/property socket "writable")
         half-close (^{:async true} fn
                     []
                     (let [writer (.getWriter writable)]
                       (try (await (.-ready writer))
                            (await (.close writer))
                            (finally (.releaseLock writer)))))
         close (fn []
                 (n/invoke socket "close" []))
         connection (tcp/connection-map socket
                                        (n/property socket "readable")
                                        writable
                                        {:half-close! half-close
                                         :close! close
                                         :opened (n/property socket "opened")
                                         :closed (n/property socket "closed")}
                                        (dissoc options :signal))]
     (when-let [signal (:signal options)]
       (let [cancel (fn [_]
                      (close))]
         (if (.-aborted signal)
           (close)
           (do (.addEventListener signal "abort" cancel #js {:once true})
               ((^{:async true} fn
                 []
                 (try (await (n/property socket "closed"))
                      (catch :default _ nil)
                      (finally (.removeEventListener signal "abort" cancel)))))))))
     connection)))

(mx/defn ^{:dynamic true} check-options!
  "Checks shared TCP options before opening any native socket; an already-aborted
  signal throws its native reason before connection effects."
  [options :- (:tcp-adapt contracts/schemas)]
  (when-let [signal (:signal options)]
    (when (.-aborted signal) (throw (.-reason signal))))
  options)

(def ^:private Address
  [:or [:and :string [:fn #(boolean (re-matches #"(?:\[[^\]]+\]|[^:]+):[0-9]+" %))]]
   [:map {:closed true} [:hostname :string] [:port [:int {:min 1 :max 65535}]]]])

(mx/defn ^{:dynamic true} connect!
  "Uses Celld's documented cloudflare:sockets connector, synchronously returning
  the shared connection map. Reconnect in each later event; this is not durable I/O."
  ([address :- Address]
   (connect! address {} {}))
  ([address :- Address native-options :- (:tcp-connect contracts/schemas) shared-options]
   (check-options! shared-options)
   (adapt-socket (sockets/connect (if (string? address) address (n/fields address))
                                  (n/option-fields native-options))
                 shared-options)))

(mx/defn ^{:dynamic true} start-tls
  "Consumes a native starttls socket and returns its new shared TLS connection."
  ([connection]
   (adapt-socket (n/invoke (req! connection :fast-twitch.tcp/socket) "startTls" [])))
  ([connection options :- (:tcp-start-tls contracts/schemas)]
   (adapt-socket (n/invoke (req! connection :fast-twitch.tcp/socket)
                           "startTls"
                           [(n/option-fields options)]))))

(defn native-socket
  "Returns the actual event-scoped TCP socket."
  [connection]
  (req! connection :fast-twitch.tcp/socket))

(defn event-source!
  "Uses shared native EventSource options/reconnection; retain and explicitly close its handle."
  [options]
  (event-source/connect! options))

(defn message-channel!
  "Creates shared port handles with explicit codecs. Celld v0.6.2 rejects nonempty transfer lists; cloned data is supported. Close owned ports."
  ([]
   (messaging/create-channel!))
  ([first-options second-options]
   (messaging/create-channel! first-options second-options)))

(v/instrument! adapt-socket check-options! connect! start-tls)
