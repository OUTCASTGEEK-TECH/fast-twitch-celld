(ns example.loaded
  "Dynamic code is itself built from direct CLJS declarations."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.websocket :as ws]
            [fast-twitch.client.core :as client]
            [fast-twitch.util.websocket.message :as message]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.storage.kv :as kv])
  (:require-macros [fast-twitch.celld.macros :refer
                    [defcell defrpc deffetch defworker defwebsocket-handlers]])
  (:refer-global :only [Response Promise]))

(defrpc increment
        {:method :increment :args [:cat] :returns :int}
        [ctx]
        (let [store (kv/native (context/storage ctx))
              current (:value (kv/get-value store :count))
              next (inc (or current 0))]
          (kv/put! store :count next)
          next))

(defn facet-message
  [ctx connection event]
  (let [store (kv/native (context/storage ctx))
        n (or (:value (kv/get-value store :messages)) 0)]
    (kv/put! store :messages (inc n))
    (client/send! connection (message/map->message (req! event :message)))))

(defn facet-close
  [ctx _connection _event]
  (kv/put! (kv/native (context/storage ctx)) :closed true))

(defwebsocket-handlers facet-sockets
                       {:id :compiled-facet
                        :version 1
                        :mode :hibernating
                        :message facet-message
                        :close facet-close})

(defrpc ^:async outbound
        {:method :outbound :args [:cat] :returns :boolean}
        [_ctx]
        (let [received (Promise.withResolvers)
              connection (await (client/connect!
                                  {:url "ws://127.0.0.1:19015/facet-ws"}
                                  {:transport :websocket
                                   :on-event (fn [event]
                                               (when (= :message (:type event))
                                                 (.resolve received
                                                           (message/map->message
                                                             (req! event :message)))))}))]
          (await (client/send! connection "outbound-facet"))
          (let [message (await (.-promise received))]
            (await (client/close! connection))
            (= "outbound-facet" message))))

(deffetch child-fetch
          [ctx request]
          (case (:uri request)
            "/facet-ws" (let [{:keys! [client server]} (ws/pair)]
                          (ws/attach-dispatch! server facet-sockets {:facet true} :json)
                          (ws/accept! ctx server ["facet"])
                          (Response. nil #js {:status 101 :webSocket client}))
            "/facet-state" {:status 200
                            :body (str (:value (kv/get-value (kv/native (context/storage
                                                                          ctx))
                                                             :messages)))}
            {:status 200 :body "compiled-child"}))

(defcell Child {:facet? true :include [increment child-fetch facet-sockets outbound]})

(defrpc ping {:method :ping :args [:cat] :returns :string} [_ctx] "named-worker")

(deffetch ^:async named-fetch
          {:native? true}
          [_ctx request]
          (if (.endsWith (.-url request) "/slow")
            (await (client/request!
                     {:url "http://127.0.0.1:18991/slow" :request-method :get}
                     {:transport :http :request-init {:signal (.-signal request)}}))
            {:status 200 :body "named-fetch"}))

(defworker Named {:export :Named :include [ping named-fetch]})

(deffetch default-fetch [_ctx _request] {:status 200 :body "dynamic-default"})

(defworker App {:include [default-fetch]})
