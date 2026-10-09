(ns fixture.corrections
  "Pinned native checks for keyword wire values and Fetch preflight ownership."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.bindings :as bindings]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.http :as http]
            [fast-twitch.celld.identity :as identity]
            [fast-twitch.celld.rpc :as rpc]
            [fast-twitch.celld.native :as native]
            [fast-twitch.celld.names :as names]
            [fast-twitch.celld.storage.kv :as kv]
            [fast-twitch.celld.storage :as storage]
            [fast-twitch.celld.sql :as sql]
            [fast-twitch.celld.services.d1 :as d1]
            [fast-twitch.util.http.request :as request]
            [fast-twitch.codecs.json :as json])
  (:require-macros [fast-twitch.celld.macros :refer
                    [defcell defrpc deffetch defworker defwebsocket-handlers]])
  (:refer-global :only [Request BigInt Object]))

(defrpc echo
        {:method :record/echo :args [:cat :map] :returns :map :codec :json}
        [ctx value]
        (let [store (kv/native (context/storage ctx))]
          (kv/put! store :keyword-value value :json)
          (req! (kv/get-value store :keyword-value :json) :value)))

(defrpc
  ^:async storage-and-sql
  {:method :verification/storageAndSql :args [:cat] :returns :map :codec :json}
  [ctx]
  (let [store (context/storage ctx)
        payload {:tenant/value :record/ready}
        graph (native/fields {:integer (BigInt "9007199254740993")})
        _ (aset graph (names/text :self) graph)
        _ (await (storage/put! store :native/graph graph))
        restored (req! (await (storage/get! store :native/graph)) :value)
        sync (kv/native store)
        _ (kv/put! sync :native/sync-graph graph)
        sync-restored (req! (kv/get-value sync :native/sync-graph) :value)
        _ (await (storage/put! store :tenant/record payload {:codec :json}))
        bulk (await (storage/get! store [:tenant/record :tenant/missing] {:codec :json}))
        listed (await (storage/list-entries! store
                                             {:prefix (keyword "tenant/") :codec :json}))
        row (sql/exactly-one (sql/exec (sql/native store)
                                       {:select [[[:param :user/id] :user/id]
                                                 [[:param :tenant/id] :tenant/id]]}
                                       {:user/id 7 :tenant/id 9}))]
    {:native-null-prototype-storage (and (nil? (Object.getPrototypeOf graph))
                                         (identical? restored (aget restored "self"))
                                         (= (BigInt "9007199254740993")
                                            (aget restored "integer")))
     :native-null-prototype-sync-kv
       (and (identical? sync-restored (aget sync-restored "self"))
            (= (BigInt "9007199254740993") (aget sync-restored "integer")))
     :async-kv-namespaced-bulk (= {:tenant/record payload} bulk)
     :async-kv-namespaced-list (= [[:tenant/record payload]] listed)
     :cell-sql-namespaced-aliases (= {:user/id 7 :tenant/id 9} row)
     :cell-sql-named-collection
       (= {:result/id 2}
          (sql/exactly-one (sql/exec (sql/native store)
                                     {:select [[[:param :needle] :result/id]]
                                      :where [:in [:param :needle] [:param :filter/ids]]}
                                     {:needle 2 :filter/ids [1 2]})))}))

(defrpc fetch-count
        {:method :fetchCount :args [:cat] :returns :int}
        [ctx]
        (get @(req! ctx :state) :fetches 0))

(deffetch ^:async cell-fetch
          [ctx req]
          (swap! (req! ctx :state) update :fetches (fnil inc 0))
          {:status 200
           :body (if (= :post (:request-method req))
                   (await (.text (:fast-twitch.routing/request req)))
                   "native-ok")})

(defn socket-handler
  [_ctx _socket _event]
  nil)

(defwebsocket-handlers sockets
                       {:id :socket/fixture
                        :version 1
                        :mode :resident
                        :message socket-handler
                        :close socket-handler
                        :error socket-handler})

(defcell Verified
         {:binding :verification/CELLS
          :export :verification/Verified
          :include [echo storage-and-sql fetch-count cell-fetch sockets]})

(defn ^:async rejects-cache?
  [stub options]
  (let [owned (Request. "http://native/" #js {:method "POST" :body "still-owned"})
        req (assoc (request/request->map owned) :url "http://native/")
        error (try (await ((http/client-for stub options) req))
                   nil
                   (catch :default error error))]
    (and (= [:request-init :cache] (:path (ex-data error)))
         (= :omitted (:expected (ex-data error)))
         (false? (.-bodyUsed owned))
         (= "still-owned" (await (.text owned))))))

(deffetch
  ^:async worker-fetch
  [ctx _req]
  (let [stub (identity/get-by-name (bindings/get-binding ctx :verification/CELLS)
                                   :verification/fixture)
        payload {:record/id :tenant/one
                 :state :record/ready
                 :data [nil false 3 "opaque" :domain/value]
                 :same 1
                 :other 2
                 :left/value 3
                 :right/value 4}
        roundtrip (await (rpc/call! stub :record/echo [payload] {:codec :json}))
        storage-results (await
                          (rpc/call! stub :verification/storageAndSql [] {:codec :json}))
        database (bindings/get-binding ctx :verification/DB)
        statement (d1/prepare database
                              {:select [[[:param :user/id] :user/id]
                                        [[:param :tenant/id] :tenant/id]]}
                              {:user/id 7 :tenant/id 9})
        d1-row (await (d1/first! statement))
        d1-column (await (d1/first! statement :user/id))
        d1-in (await (d1/first! (d1/prepare database
                                            {:select [[[:param :needle] :result/id]]
                                             :where [:in [:param :needle]
                                                     [:param :filter/ids]]}
                                            {:needle 2 :filter/ids [1 2]})))
        before (await (rpc/call! stub :fetchCount []))
        initial (await (rejects-cache? stub {:request-init {:cache :no-store}}))
        middleware (await (rejects-cache? stub
                                          {:request-middleware
                                             [(fn [m]
                                                (assoc-in m
                                                  [:fast-twitch.client/options
                                                   :request-init :cache]
                                                  :no-store))]}))
        after (await (rpc/call! stub :fetchCount []))
        positive (await ((http/client-for stub {:codec :text})
                          {:url "http://native/" :request-method :get}))
        repaired (await ((http/client-for stub
                                          {:codec :text
                                           :request-init {:cache :no-store}
                                           :request-middleware
                                             [(fn [m]
                                                (update-in m
                                                           [:fast-twitch.client/options
                                                            :request-init]
                                                           dissoc
                                                           :cache))]})
                          {:url "http://native/" :request-method :get}))
        final-count (await (rpc/call! stub :fetchCount []))]
    {:status 200
     :body (json/encode (merge storage-results
                               {:d1-namespaced-aliases (= {:user/id 7 :tenant/id 9}
                                                          d1-row)
                                :d1-namespaced-column (= 7 d1-column)
                                :d1-named-collection (= {:result/id 2} d1-in)
                                :native-keyword-rpc-and-persistence (= payload roundtrip)
                                :initial-cache-rejected-before-body-use initial
                                :middleware-cache-rejected-before-body-use middleware
                                :zero-rejected-fetch-effects (= before after)
                                :native-no-cache-positive (= "native-ok" (:body positive))
                                :middleware-removes-cache (= "native-ok" (:body repaired))
                                :two-native-fetches (= (+ before 2) final-count)}))}))

(defworker App {:include [worker-fetch]})
