(ns fast-twitch.celld.websocket
  "Celld socket acceptance and durable dispatch metadata over Fast-Twitch handles.
  Resident listeners and native hibernation callbacks use one delivery path each.
  Facet sockets remain resident according to Celld even with native acceptance."
  (:require [fast-twitch.celld.native :as n]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names]
            [fast-twitch.transports.websocket.connection :as connection]
            [fast-twitch.transports.websocket.listener :as listener]
            [fast-twitch.util.websocket.event :as event])
  (:refer-global :only
                 [WebSocketPair WebSocketRequestResponsePair Response TextEncoder
                  Object]))

(defn native
  "Returns the shared connection's original socket or an already-native socket."
  [handle]
  (or (:fast-twitch.websocket/socket handle) handle))

(defn pair
  "Creates the native pair synchronously, returning :client and :server native sockets."
  []
  (let [pair (WebSocketPair.)] {:client (aget pair "0") :server (aget pair "1")}))

(defn accept!
  "Accepts through ctx.acceptWebSocket exactly once; tags are native string vectors.
  Hibernating mode must have generated stable lifecycle handlers."
  ([ctx socket]
   (n/invoke (context/native ctx) "acceptWebSocket" [(native socket)])
   (connection/native (native socket)))
  ([ctx socket tags]
   (v/check! [:vector :string] tags :websocket-tags)
   (n/invoke (context/native ctx) "acceptWebSocket" [(native socket) (to-array tags)])
   (connection/native (native socket))))

(defn sockets
  "Returns shared native connection handles; optional tag omission is preserved."
  ([ctx]
   (mapv connection/native
     (array-seq (n/invoke (context/native ctx) "getWebSockets" []))))
  ([ctx tag]
   (mapv connection/native
     (array-seq (n/invoke (context/native ctx)
                          "getWebSockets"
                          [(v/check! :string tag :websocket-tag)])))))

(defn tags
  "Returns the native accepted socket tags synchronously."
  [ctx socket]
  (vec (array-seq (n/invoke (context/native ctx) "getTags" [(native socket)]))))

(defn serialize-attachment!
  "Stores an explicit codec envelope; native resources/functions are rejected.
  :json handles the documented CLJS JSON domain and version 2, with legacy version-1 reads."
  ([socket value]
   (serialize-attachment! socket value :native))
  ([socket value policy]
   (codec/persistence-policy! policy)
   (n/invoke (native socket) "serializeAttachment" [(codec/encode policy value)])))

(defn deserialize-attachment
  "Returns the explicitly decoded native attachment. Missing native data remains nil."
  ([socket]
   (deserialize-attachment socket :native))
  ([socket policy]
   (codec/persistence-policy! policy)
   (let [value (n/invoke (native socket) "deserializeAttachment" [])]
     (when (some? value) (codec/decode policy value)))))

(defn set-auto-response!
  "Installs a native pair or clears it by omitted argument; UTF-8 sides are bounded natively."
  ([ctx]
   (n/invoke (context/native ctx) "setWebSocketAutoResponse" []))
  ([ctx request response]
   (v/check! :string request :websocket-auto-response)
   (v/check! :string response :websocket-auto-response)
   (doseq [text [request response]]
     (when (> (.-byteLength (.encode (TextEncoder.) text)) 2048)
       (v/fail! :websocket-auto-response
                :limit
                "Each auto-response side must fit 2048 UTF-8 bytes.")))
   (n/invoke (context/native ctx)
             "setWebSocketAutoResponse"
             [(WebSocketRequestResponsePair. request response)])))

(defn auto-response
  "Returns native auto-response pair data, or nil when unset."
  [ctx]
  (when-let [pair (n/invoke (context/native ctx) "getWebSocketAutoResponse" [])]
    {:request (n/property pair "request") :response (n/property pair "response")}))

(defn auto-response-timestamp
  "Returns the native Date or nil; no timestamp is fabricated."
  [ctx socket]
  (n/invoke (context/native ctx) "getWebSocketAutoResponseTimestamp" [(native socket)]))

(defn attach-dispatch!
  "Writes stable dispatch ID/version plus application attachment payload before acceptance."
  [socket descriptor value policy]
  (codec/persistence-policy! policy)
  (let [{:keys! [id version mode]} descriptor]
    (when-not (= mode :hibernating)
      (v/fail! :websocket-dispatch
               :mode
               "Use a hibernating declaration for durable dispatch."))
    (n/invoke (native socket)
              "serializeAttachment"
              [#js {:fastTwitchSocket 1
                    :id (names/text id)
                    :version version
                    :codec (names/text (or policy :native))
                    :payload (codec/encode policy value)}])))

(defn- dispatch!
  [descriptor ctx type socket native-event data]
  (let [attachment (n/invoke socket "deserializeAttachment" [])
        {:keys! [id version mode]} descriptor]
    (if (and (= mode :hibernating)
             (not (and (= "object" (goog/typeOf attachment))
                       (= 1 (aget attachment "fastTwitchSocket"))
                       (= (names/text id) (aget attachment "id"))
                       (= version (aget attachment "version")))))
      ;; Rejected callbacks discard captured close frames in Celld. The policy
      ;; close is the terminal diagnostic; no application callback may follow it.
      (try (n/invoke socket "close" [4008 "Invalid dispatch attachment"])
           (catch :default _ nil))
      (when-let [handler (get descriptor type)]
        (handler
          ctx
          (connection/native socket)
          (cond-> (event/event->map type (connection/native socket) native-event data)
            (= mode :hibernating) (assoc :attachment
                                    (codec/decode (keyword (aget attachment "codec"))
                                                  (aget attachment "payload")))))))))

(defn install-handlers!
  "Installs stable native lifecycle methods on the actual Cell constructor.
  Rehydration reads dispatch metadata; no on-open is replayed and no listener is added."
  [constructor descriptor]
  (let [prototype (.-prototype constructor)]
    (aset prototype
          "webSocketMessage"
          (fn [socket data]
            (this-as this
                     (dispatch! descriptor (context/of this) :message socket nil data))))
    (aset prototype
          "webSocketClose"
          (fn [socket code reason clean]
            (this-as this
                     (dispatch! descriptor
                                (context/of this)
                                :close
                                socket
                                nil
                                {:code code :reason reason :was-clean? clean}))))
    (aset prototype
          "webSocketError"
          (fn [socket error]
            (this-as this
                     (dispatch! descriptor (context/of this) :error socket nil error))))
    constructor))

(defn upgrade-capability
  "Supplies the native Ring resident upgrade capability. A hibernating socket uses
  explicit pair/attachment/accept instead, avoiding duplicate resident dispatch."
  [_ctx]
  {:upgrade (fn [_ response]
              (let [{:keys! [client server]} (pair)
                    owned (atom nil)]
                (try
                  (reset! owned (listener/attach! server
                                                  (req! response :ring.websocket/listener)
                                                  {}))
                  (n/invoke server "accept" [])
                  ;; Pair acceptance need not dispatch native Cell hibernation
                  ;; handlers.
                  ((req! @owned :open!))
                  (Response. nil
                             #js {:status 101
                                  :webSocket client
                                  :headers (n/fields (cond-> (:headers response {})
                                                       (:ring.websocket/protocol response)
                                                         (assoc :sec-websocket-protocol
                                                           (:ring.websocket/protocol
                                                             response))))})
                  (catch :default error
                    (when-let [dispose (:dispose! @owned)] (dispose))
                    (doseq [socket [server client]]
                      (try (n/invoke socket "close" [4011 "Acceptance failed"])
                           (catch :default _ nil)))
                    (throw error)))))})
