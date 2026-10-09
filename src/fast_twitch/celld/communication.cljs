(ns fast-twitch.celld.communication
  "Celld's event-scoped outbound TCP capability adapted to shared Fast-Twitch
  operations. SSE/EventSource/streams/ports use the existing shared namespaces."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.util.tcp :as tcp]
            [fast-twitch.client.sse.event-source :as event-source]
            [fast-twitch.client.messaging :as messaging])
  (:refer-global :only [globalThis]))

(defn adapt-socket
  "Retains native socket/readable/writable handles. Half-close owns one writer
  only for its call; full close preserves the native socket's terminal Promise."
  ([socket]
   (adapt-socket socket {}))
  ([socket options]
   (n/options options #{:codec :streaming :signal} :tcp-adapt)
   (let [writable (n/property socket "writable")
         half-close (^:async fn
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
               ;; The connection stays synchronous; only terminal cleanup awaits the
               ;; native lifetime.
               ((^:async fn
                 []
                 (try (await (n/property socket "closed"))
                      (catch :default _ nil)
                      (finally (.removeEventListener signal "abort" cancel)))))))))
     connection)))

(defn check-options!
  "Checks shared TCP options before opening any native socket; an already-aborted
  signal throws its native reason before connection effects."
  [options]
  (n/options options #{:codec :streaming :signal} :tcp-adapt)
  (when-let [signal (:signal options)]
    (when (.-aborted signal) (throw (.-reason signal))))
  options)

(defn connect!
  "Uses Celld's documented cloudflare:sockets connector, synchronously returning
  the shared connection map. Reconnect in each later event; this is not durable I/O."
  ([address]
   (connect! address {} {}))
  ([address native-options shared-options]
   (if (string? address)
     (when-not (re-matches #"(?:\[[^\]]+\]|[^:]+):[0-9]+" address)
       (v/fail! :tcp-address :value "Use host:port or a checked hostname/port map."))
     (v/check! [:map {:closed true} [:hostname :string]
                [:port [:int {:min 1 :max 65535}]]]
               address
               :tcp-address))
   (check-options! shared-options)
   (let [connector (aget globalThis "__ft_connect")]
     (when-not (fn? connector)
       (v/fail! :tcp-connect :capability "Build with the Celld native connector import."))
     (adapt-socket (connector (if (string? address)
                                address
                                (n/options address #{:hostname :port} :tcp-address))
                              (n/options native-options
                                         #{:secureTransport :allowHalfOpen}
                                         :tcp-connect))
                   shared-options))))

(defn start-tls
  "Consumes a native starttls socket and returns its new shared TLS connection."
  ([connection]
   (adapt-socket (n/invoke (req! connection :fast-twitch.tcp/socket) "startTls" [])))
  ([connection options]
   (adapt-socket (n/invoke
                   (req! connection :fast-twitch.tcp/socket)
                   "startTls"
                   [(n/options options #{:expectedServerHostname} :tcp-start-tls)]))))

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
