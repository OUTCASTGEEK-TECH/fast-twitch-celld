(ns example.transport-app
  "Shared transports through actual Celld capabilities and direct lifecycle methods."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.websocket :as ws]
            [fast-twitch.celld.communication :as communication]
            [fast-twitch.celld.http :as http]
            [fast-twitch.celld.rpc :as rpc]
            [fast-twitch.celld.bindings :as bindings]
            [fast-twitch.celld.identity :as identity]
            [fast-twitch.celld.storage.kv :as kv]
            [fast-twitch.client.core :as client]
            [fast-twitch.client.sse.fetch :as sse-fetch]
            [fast-twitch.server.sse :as sse]
            [fast-twitch.util.streams.readers :as readers]
            [fast-twitch.util.websocket.message :as message]
            [fast-twitch.codecs.json :as json])
  (:require-macros [fast-twitch.celld.macros :refer
                    [defcell defrpc deffetch defworker defwebsocket-handlers]])
  (:refer-global :only
                 [Response Promise Uint8Array ArrayBuffer AbortController Date WebAssembly
                  crypto MessageChannel ReadableStream Error]))

(defn hibernated-message
  [_ctx connection event]
  (client/send! connection (req! event :message)))

(defn hibernated-close
  [ctx _connection event]
  (let [store (kv/native (context/storage ctx))]
    (kv/put! store
             :hibernated-close-count
             (inc (or (:value (kv/get-value store :hibernated-close-count)) 0)))
    (kv/put! store :hibernated-close (req! event :code))))

(defn hibernated-error
  [ctx connection event]
  (let [store (kv/native (context/storage ctx))]
    (kv/put! store
             :hibernated-error
             (inc (or (:value (kv/get-value store :hibernated-error)) 0)))
    (kv/put! store :hibernated-error-value (instance? Error (req! event :error)))
    (client/close! connection {:code 4011 :reason "Native error"})))

(defwebsocket-handlers durable-sockets
                       {:id :fixture-echo
                        :version 1
                        :mode :hibernating
                        :message hibernated-message
                        :close hibernated-close
                        :error hibernated-error})

(defrpc
  socket-state
  {:method :socketState :args [:cat] :returns [:map-of :keyword :any] :codec :json}
  [ctx]
  (let [sockets (ws/sockets ctx)]
    {:attachment-round-trip (:value (kv/get-value (kv/native (context/storage ctx))
                                                  :attachment-round-trip))
     :open (count sockets)
     :tags (mapv #(ws/tags ctx %) sockets)
     :resident-open (:value (kv/get-value (kv/native (context/storage ctx))
                                          :resident-open))
     :resident-close (:value (kv/get-value (kv/native (context/storage ctx))
                                           :resident-close))
     :hibernated-close (:value (kv/get-value (kv/native (context/storage ctx))
                                             :hibernated-close))
     :hibernated-close-count (:value (kv/get-value (kv/native (context/storage ctx))
                                                   :hibernated-close-count))
     :hibernated-error (:value (kv/get-value (kv/native (context/storage ctx))
                                             :hibernated-error))
     :hibernated-error-value (:value (kv/get-value (kv/native (context/storage ctx))
                                                   :hibernated-error-value))
     :id (identity/id-string (context/id ctx))
     :auto-timestamp (when-let [socket (first sockets)]
                       (when-let [date (ws/auto-response-timestamp ctx socket)]
                         (.getTime date)))
     :auto (let [value (ws/auto-response ctx)]
             (when value {:request (:request value) :response (:response value)}))}))

(defrpc
  ^:async communication-checks
  {:method :communicationChecks
   :args [:cat]
   :returns [:map-of :keyword :boolean]
   :codec :json}
  [_ctx]
  (let [outbound (await (client/request! {:url "http://127.0.0.1:18991/text"
                                          :request-method :get}
                                         {:codec :text}))
        tcp (communication/connect! {:hostname "127.0.0.1" :port 18995}
                                    {:allowHalfOpen true}
                                    {:codec :text})
        _ (await (req! tcp :opened))
        _ (await (client/send! tcp "bytes"))
        _ (await (client/half-close! tcp))
        received (await (client/receive! tcp))
        _ (await (client/close! tcp))
        _ (await (req! tcp :closed))
        plaintext (communication/connect! {:hostname "127.0.0.1" :port 18997}
                                          {:secureTransport :starttls}
                                          {:codec :text})
        _ (await (req! plaintext :opened))
        upgraded (communication/start-tls plaintext {:expectedServerHostname "localhost"})
        tls-error (try (await (req! upgraded :opened)) nil (catch :default error error))
        closed-error
          (try (await (req! upgraded :closed)) nil (catch :default error error))
        upgrade-once? (try (communication/start-tls plaintext)
                           false
                           (catch :default error
                             (boolean (re-find #"only be called once" (str error)))))
        consumed? (try (await (client/send! plaintext "late"))
                       false
                       (catch :default error (boolean (re-find #"upgraded" (str error)))))
        _ (await (client/close! plaintext))
        events (atom [])
        _ (await (sse-fetch/connect! {:url "http://127.0.0.1:18991/sse"
                                      :request-method :get}
                                     {:on-event #(swap! events conj %)}))
        resolve-event (atom nil)
        event-source-promise (Promise. (fn [resolve _]
                                         (reset! resolve-event resolve)))
        source (communication/event-source! {:url "http://127.0.0.1:18991/sse"
                                             :on-event (fn [event]
                                                         (when (= :message (:type event))
                                                           (@resolve-event
                                                            (:data event))))})
        event-data (await event-source-promise)
        _ (client/close! source)
        resolve-port (atom nil)
        port-promise (Promise. (fn [resolve _]
                                 (reset! resolve-port resolve)))
        channel (communication/message-channel! {}
                                                {:on-event (fn [event]
                                                             (@resolve-port
                                                              (:data event)))})
        bytes (Uint8Array. #js [9 8 7])
        transfer-rejected? (try (client/send! (:port1 channel)
                                              (.-buffer bytes)
                                              {:transfer [(.-buffer bytes)]})
                                false
                                (catch :default _ true))
        _ (client/send! (:port1 channel) (.-buffer bytes))
        port-data (await port-promise)
        _ (client/close! (:port1 channel))
        _ (client/close! (:port2 channel))
        aborted (AbortController.)
        _ (.abort aborted)
        cancellation? (try (communication/connect! {:hostname "127.0.0.1" :port 18995}
                                                   {}
                                                   {:signal (.-signal aborted)})
                           false
                           (catch :default _ true))
        cancelled (atom false)
        stream (ReadableStream. #js {:pull (fn [controller]
                                             (.enqueue controller (Uint8Array. 1024)))
                                     :cancel (fn [_]
                                               (reset! cancelled true))})
        overflow? (try (await (readers/read! stream {:codec :bytes :max-bytes 2048}))
                       false
                       (catch :default _ true))]
    {:bounded-stream (and overflow? @cancelled (not (.-locked stream)))
     :fetch (= "controlled" (:body outbound))
     :tcp-start-tls-handle
       (and (not (identical? (communication/native-socket plaintext)
                             (communication/native-socket upgraded)))
            (= "on" (.-secureTransport (communication/native-socket upgraded))))
     :tcp-start-tls-native-rejection
       (and (instance? Error tls-error)
            (boolean (re-find #"(?i)certificate|unknownissuer" (str tls-error)))
            (identical? tls-error closed-error))
     :tcp-start-tls-once upgrade-once?
     :tcp-start-tls-consumes-plaintext consumed?
     :tcp-opened-resolved true
     :tcp-closed-resolved true
     :tcp-half-close (= "echo:bytes" received)
     :sse-fetch (= "hello" (:data (first @events)))
     :event-source (= "hello" event-data)
     :port-data (= [9 8 7] (vec (array-seq (Uint8Array. port-data))))
     :port-transfer-rejected transfer-rejected?
     :tcp-pre-abort cancellation?
     :promise-with-resolvers (fn? (.-withResolvers Promise))}))

(deffetch
  ^:async cell-fetch
  [ctx request]
  (case (:uri request)
    "/edited" {:status 200
               :headers {:x-cell "edited"}
               :body (get-in request [:headers :x-fixture])}
    "/resident"
      {:ring.websocket/listener
         {:on-open (fn [_]
                     (let [store (kv/native (context/storage ctx))]
                       (kv/put! store
                                :resident-open
                                (inc (or (:value (kv/get-value store :resident-open))
                                         0)))))
          :on-message (fn [socket data]
                        (client/send! socket (message/message->map data)))
          :on-close (fn [_ _code _reason]
                      (let [store (kv/native (context/storage ctx))]
                        (kv/put! store
                                 :resident-close
                                 (inc (or (:value (kv/get-value store :resident-close))
                                          0)))))}}
    "/ws" (let [{:keys! [client server]} (ws/pair)]
            (ws/serialize-attachment! server {:session :fixture} :json)
            (let [same? (= {:session :fixture} (ws/deserialize-attachment server :json))]
              (when-not same? (throw (ex-info "Attachment round trip failed" {})))
              (kv/put! (kv/native (context/storage ctx)) :attachment-round-trip same?))
            (ws/attach-dispatch! server durable-sockets {:session "fixture"} :json)
            (ws/accept! ctx server ["fixture"])
            (ws/set-auto-response! ctx "ping" "pong")
            (Response. nil #js {:status 101 :webSocket client}))
    "/bad-version"
      (let [{:keys! [client server]} (ws/pair)]
        (ws/attach-dispatch! server (assoc durable-sockets :version 2) nil :native)
        (ws/accept! ctx server)
        (Response. nil #js {:status 101 :webSocket client}))
    "/missing-attachment" (let [{:keys! [client server]} (ws/pair)]
                            (ws/accept! ctx server)
                            (Response. nil #js {:status 101 :webSocket client}))
    "/sse" (let [counter (atom 0)]
             (sse/response (fn []
                             (when (= 1 (swap! counter inc)) {:data "served"}))))
    "/reject" (throw (ex-info "native failure" {}))
    {:status 200 :body "transport-cell"}))

(defcell TransportCell
         {:binding :TRANSPORTS
          :include [cell-fetch socket-state communication-checks durable-sockets]})

(deffetch
  ^:async worker-fetch
  [ctx request]
  (let [stub (identity/get-by-name (bindings/get-binding ctx :TRANSPORTS) "fixture")]
    (case (:uri request)
      "/state" {:status 200
                :headers {:content-type "application/json"}
                :body (json/encode (await
                                     (rpc/call! stub :socketState [] {:codec :json})))}
      "/communication"
        {:status 200
         :headers {:content-type "application/json"}
         :body (json/encode (await
                              (rpc/call! stub :communicationChecks [] {:codec :json})))}
      "/forward-edit" (await ((http/client-for stub
                                               {:response-middleware
                                                  [(fn [response]
                                                     (-> response
                                                         (assoc :status 201)
                                                         (assoc-in [:headers :x-response]
                                                                   "edited")))]})
                               (-> request
                                   (assoc :uri "/edited")
                                   (assoc-in [:headers :x-fixture] "changed"))))
      "/forward-reject" (try (await (http/forward! stub (assoc request :uri "/reject")))
                             {:status 500 :body "lost rejection"}
                             (catch :default _ {:status 200 :body "rejected"}))
      (await (http/forward! stub request)))))

(defworker App {:include [worker-fetch]})
