(ns fast-twitch.celld.test-main
  "Release-output tests for receiver/omission/guards; native fixtures remain separate."
  (:require
    [malli.core :as m]

    [cljs.core :refer [await]]
    [cljs.test :refer [deftest is run-tests async]]
    [fast-twitch.celld.context :as context]
    [fast-twitch.celld.worker]
    [fast-twitch.celld.native :as native]
    [fast-twitch.celld.storage :as storage :refer-macros [with-transaction-sync]]
    [fast-twitch.celld.sql :as sql]
    [fast-twitch.celld.validation :as validation]
    [fast-twitch.celld.storage.kv :as kv]
    [fast-twitch.celld.codec :as codec]
    [fast-twitch.celld.rpc :as rpc]
    [fast-twitch.celld.contracts :as contracts]
    [fast-twitch.celld.services.d1 :as d1]
    [fast-twitch.celld.services.kv :as service-kv]
    [fast-twitch.celld.services.r2 :as r2]
    [fast-twitch.celld.services.queues :as queues]
    [fast-twitch.celld.services.workflows :as workflows]
    [fast-twitch.celld.services.loaders :as loaders]
    [fast-twitch.celld.services.containers :as containers]
    [fast-twitch.celld.communication :as communication]
    [fast-twitch.celld.names :as names]
    [fast-twitch.celld.http :as http]
    [fast-twitch.util.http.request :as request]
    [fast-twitch.codecs.json :as json])
  (:require-macros [fast-twitch.celld.macros :refer [defworker]])
  (:refer-global :only
                 [Promise Object Uint8Array process BigInt TextEncoder Response Request
                  Error Map globalThis]))

(defn observed-worker-event
  [ctx event]
  [ctx event])

(defn worker-owner
  [ctx]
  (aget (context/env ctx) "owner"))

(defn native-worker-fetch
  [ctx _request]
  (Response. (worker-owner ctx)))

(defworker NamedWorker
           {:export :NamedWorker
            :queue observed-worker-event
            :scheduled observed-worker-event
            :fetch {:handler native-worker-fetch :native? true}
            :rpc {:owner {:handler worker-owner :args [:cat] :returns :string}}})

(defworker DefaultWorker
           {:queue observed-worker-event
            :scheduled observed-worker-event
            :fetch {:handler native-worker-fetch :native? true}})

(deftest declared-worker-contexts
  (async
    done
    ((^:async fn
      []
      (try
        (let [env-a #js {:owner "a"}
              env-b #js {:owner "b"}
              ctx #js {}
              event #js {}
              a (new NamedWorker ctx env-a)
              b (new NamedWorker ctx env-b)]
          (doseq [worker [a b]
                  method ["queue" "scheduled"]]
            (let [[observed observed-event] (native/invoke worker method [event])]
              (is (= :named-worker (:kind observed)))
              (is (identical? (context/of worker) observed))
              (is (identical? event observed-event))))
          (doseq [method ["queue" "scheduled"]]
            (let [[observed observed-event]
                    (native/invoke DefaultWorker method [event env-b ctx])]
              (is (= :worker (:kind observed)))
              (is (identical? env-b (context/env observed)))
              (is (identical? ctx (context/native observed)))
              (is (identical? event observed-event))))
          (is (= "a" (await (native/invoke a "owner" []))))
          (is (= "b" (await (native/invoke b "owner" []))))
          (let [request (Request. "https://fixture/")
                named (await (native/invoke a "fetch" [request]))
                default (await (native/invoke DefaultWorker "fetch" [request env-b ctx]))]
            (is (= "a" (await (.text named))))
            (is (= "b" (await (.text default)))))
          (is (thrown? Error
                       (.call (aget (.-prototype NamedWorker) "queue") #js {} event))))
        (catch :default error (is false (str error)))
        (finally (done)))))))

(deftest guards-and-codecs
  (is (m/validate (:queue-send contracts/schemas) {:delaySeconds 86400}))
  (is (not (m/validate (:queue-send contracts/schemas) {:delaySeconds 86401})))
  (is (= {:items [nil false 42]}
         (codec/decode :json (codec/encode :json {:items [nil false 42]}))))
  (is (thrown? js/Error (codec/encode :native {:unsafe "collection"})))
  (is (thrown? js/Error (codec/encode :native 9007199254740992)))
  (is (thrown? js/Error
               (codec/decode :json
                             "null"))))

(deftest native-structured-graph
  (let [integer (BigInt "9007199254740993")
        value (native/fields {:integer integer})]
    (aset value (names/text :self) value)
    (is (identical? value (codec/encode :native value)))
    (is (= integer (aget (codec/decode :native value) "integer")))
    (is (thrown? js/Error (codec/encode :json integer)))))

(deftest pinned-workflow-boundaries
  (is (= 10000 (workflows/duration-ms "10 seconds")))
  (is (= 1000 (workflows/duration-ms "1 second")))
  (is (thrown? Error (workflows/duration-ms "invalid duration")))
  (is (m/validate (:workflow-create contracts/schemas) {:locationHint :apac-ne}))
  (is (m/validate (:workflow-create contracts/schemas) {:locationHint :apac-se}))
  (is (m/validate (:workflow-step contracts/schemas)
                  {:retries {:limit 10000 :delay "1 second"}}))
  (is (not (m/validate (:workflow-step contracts/schemas)
                       {:retries {:limit 10001 :delay "1 second"}})))
  (is (not (m/validate (:workflow-step contracts/schemas) {:sensitive true}))))

(deftest transport-options-before-effects
  (let [calls (atom 0)]
    (aset (aget globalThis "cloudflare:sockets")
          "connect"
          (fn [& _]
            (swap! calls inc)))
    (is (thrown?
          js/Error
          (communication/connect! {:hostname "localhost" :port 1} {} {:codec :invalid})))
    (is (zero? @calls))))

(deftest sync-guard-inside-native-callback
  (let [committed (atom false)
        receiver #js {}]
    (aset receiver
          "transactionSync"
          (fn [callback]
            (let [result (callback)]
              (reset! committed true)
              result)))
    (is (= 7
           (with-transaction-sync receiver 7)))
    (reset! committed false)
    (is (thrown? js/Error
                 (with-transaction-sync receiver (Promise.resolve 1))))
    (is (false? @committed))))

(deftest receiver-and-omission
  (let [calls (atom [])
        receiver #js {}]
    (aset receiver
          "getBookmark"
          (fn [& args]
            (this-as this (swap! calls conj [(identical? this receiver) (count args)]))
            "opaque"))
    (is (= "opaque" (d1/bookmark receiver)))
    (is (= [[true 0]] @calls))))

(deftest rpc-before-effects
  (async
    done
    ((^:async fn
      []
      (let [effects (atom 0)
            handler (fn [_ n]
                      (swap! effects inc)
                      n)]
        (try (await
               (rpc/serve! handler {} ["wrong"] {:args-schema [:cat :int] :returns :int}))
             (is false "input rejected")
             (catch :default _ (is (zero? @effects))))
        (is (= 4
               (await
                 (rpc/serve! handler {} [4] {:args-schema [:cat :int] :returns :int}))))
        (is (= 1 @effects))
        (done))))))

(deftest persistent-policy-before-effects
  (let [calls (atom 0)
        receiver #js {}]
    (doseq [method ["get" "put" "list" "status" "create" "waitForEvent"]]
      (aset receiver
            method
            (fn [& _]
              (swap! calls inc))))
    (is (thrown? js/Error (kv/get-value receiver :absent :rpc)))
    (is (thrown? js/Error (kv/list-values receiver {} :rpc)))
    (is (thrown? js/Error (workflows/create! receiver {:codec :rpc})))
    (is (thrown? js/Error
                 (workflows/wait-for-event! receiver :wait {:type :event :codec :rpc})))
    (is (zero? @calls))))

(deftest default-persistence-policy
  (is (nil? (codec/persistence-policy! nil)))
  (is (= :native (codec/persistence-policy! :native)))
  (is (= :json (codec/persistence-policy! :json))))

(deftest service-kv-metadata-before-effects
  (let [calls (atom 0)
        receiver #js {:put (fn [& _]
                             (swap! calls inc))}]
    (is (thrown? js/Error
                 (service-kv/put! receiver
                                  :key
                                  "value"
                                  {:metadata {:kind (fn []
                                                      nil)}})))
    (is (thrown? js/Error
                 (service-kv/put! receiver
                                  :key
                                  "value"
                                  {:metadata #js {:nested {:kind "cljs"}}})))
    (is (zero? @calls))
    (service-kv/put! receiver :key "value" {:metadata #js {:kind "native"}})
    (is (= 1 @calls))))

(deftest unavailable-workflow-sensitive-before-effects
  (async done
         ((^:async fn
           []
           (let [calls (atom 0)
                 receiver #js {:do (fn [& _]
                                     (swap! calls inc))}]
             (try (await (workflows/do! receiver
                                        :step
                                        (fn []
                                          nil)
                                        {:sensitive "output"}))
                  (is false "Unavailable native option must reject")
                  (catch :default error
                    (is (= :unavailable (:boundary (ex-data error))))))
             (is (zero? @calls)))
           (done)))))

(deftest native-projection-and-proxy-call
  (let [object #js {:key "key"
                    :writeHttpMetadata (fn [_]
                                         nil)}
        receiver #js {}
        method (fn [x]
                 (this-as this [this x]))]
    (.defineProperty Object
                     object
                     "bodyUsed"
                     #js {:get (fn []
                                 false)})
    (is (false? (get (r2/object-map object) :bodyUsed)))
    (is (not (contains? (r2/object-map object) :writeHttpMetadata)))
    (.defineProperty Object
                     method
                     "apply"
                     #js {:value (fn [& _]
                                   (throw (js/Error. "Must not read pipelined apply")))})
    (aset receiver "call" method)
    (is (= [receiver 7] (native/invoke receiver "call" [7])))
    (let [instance #js {}]
      (context/activate! instance #js {} #js {} nil :named-worker)
      (is (= :named-worker (:kind (context/of instance)))))))

(deftest checked-service-options-before-effects
  (let [calls (atom 0)
        receiver #js {}]
    (aset receiver
          "sendBatch"
          (fn [& _]
            (swap! calls inc)))
    (aset receiver
          "connect"
          (fn [& _]
            (swap! calls inc)))
    (is (thrown? js/Error
                 (queues/send-batch! receiver [{:body "body" :delaySeconds 86401}])))
    (is (thrown? js/Error (containers/connect! receiver {} {:codec :wrong})))
    (is (zero? @calls))
    (let [payload {:tenant/value 1}
          seen (atom nil)
          pending (Promise.resolve nil)]
      (aset receiver
            "send"
            (fn [body options]
              (reset! seen [body options])
              pending))
      (is (identical? pending
                      (queues/send! receiver payload {:codec :json :contentType :json})))
      (is (= payload (codec/decode :json (first @seen))))
      (is (undefined? (aget (second @seen) "codec")))
      (is (thrown? Error
                   (queues/send-batch! receiver
                                       [{:body payload :codec :json} {:body payload}])))
      (is (zero? @calls)))
    (let [bytes (Uint8Array. #js [1 2])
          native-module #js {:data bytes}
          code (loaders/code {:mainModule :tenant/module
                              :compatibilityDate "2026-10-07"
                              :compatibilityFlags [:js_rpc]
                              :modules {:tenant/module {:wasm bytes}
                                        :native native-module}})]
      (is (= "tenant/module" (aget code "mainModule")))
      (is (= ["js_rpc"] (vec (aget code "compatibilityFlags"))))
      (is (identical? bytes (aget (aget (aget code "modules") "tenant/module") "wasm")))
      (is (identical? native-module (aget (aget code "modules") "native"))))))

(deftest workflow-map-and-absent-output
  (async
    done
    ((^:async fn
      []
      (let [instance #js {:status (fn []
                                    (Promise.resolve #js {:status :waiting}))}
            payload {:value 3}
            event #js {:payload (codec/encode :json payload) :instanceId "id"}
            seen (atom nil)]
        (is (= {:status :waiting} (await (workflows/status! instance :json))))
        (is (= payload
               (codec/decode :json
                             (await (workflows/run! (fn [_ event _]
                                                      (reset! seen event)
                                                      (req! event :payload))
                                                    {}
                                                    event
                                                    #js {}
                                                    {:codec :json
                                                     :args [:map-of :keyword :int]
                                                     :returns [:map-of :keyword
                                                               :int]})))))
        (is (= "id" (get @seen :instanceId)))
        (let [calls (atom 0)
              binding #js {:createBatch (fn [options]
                                          (swap! calls inc)
                                          (Promise.resolve options))}
              step #js {:do (fn [name options callback]
                              (is (= "project" name))
                              (is (undefined? (aget options "codec")))
                              (callback #js {:attempt 1}))
                        :waitForEvent (fn [_ options]
                                        (is (= "continue" (aget options "type")))
                                        (is (undefined? (aget options "codec")))
                                        (Promise.resolve
                                          #js {:type "continue"
                                               :payload (codec/encode :json payload)}))}]
          (is (= payload
                 (await (workflows/do! step
                                       :project
                                       (fn [ctx]
                                         (is (= 1 (.-attempt ctx)))
                                         (Promise.resolve payload))
                                       {:codec :json}))))
          (is (= {:type :continue :payload payload}
                 (await (workflows/wait-for-event! step
                                                   :event
                                                   {:type :continue :codec :json}))))
          (is (thrown? Error
                       (workflows/create-batch! binding
                                                [{:params payload :codec :json}
                                                 {:params payload}])))
          (is (zero? @calls))
          (let [batch (await (workflows/create-batch! binding
                                                      [{:params payload :codec :json}]))]
            (is (= payload (codec/decode :json (aget (aget batch 0) "params"))))
            (is (undefined? (aget (aget batch 0) "codec")))
            (is (= 1 @calls))))
        (done))))))

(deftest keyword-wire-budget-and-before-effects
  (let [value {:user/id :tenant/customer
               :other/id :other/value
               :items [{:ns/key false} nil [42 :status/ready]]}
        encoder (TextEncoder.)
        overhead (.-byteLength (.encode encoder (codec/write-json "")))
        exact (.repeat "a" (- 1048576 overhead))
        unicode (.repeat "🍁" (quot (- 1048576 overhead) 4))
        calls (atom 0)
        receiver #js {:put (fn [& _]
                             (swap! calls inc))}]
    (is (= value (codec/decode :json (codec/encode :json value))))
    (is (string? (codec/encode :json value)))
    (is (= 2 (nth (json/decode (codec/encode :json value)) 1)))
    (is (thrown? Error
                 (codec/decode
                   :json
                   (json/encode ["fast-twitch/cljs-json" 1 false]))))
    (is (= 1048576 (.-byteLength (.encode encoder (codec/write-json exact)))))
    (is (= exact (codec/read-json (codec/write-json exact))))
    (is (= unicode (codec/read-json (codec/write-json unicode))))
    (is (thrown? Error (codec/write-json (str exact "a"))))
    (is (thrown? Error
                 (codec/write-json (.repeat "🍁" (inc (quot (- 1048576 overhead) 4))))))
    (is (thrown? Error (kv/put! receiver :payload (str exact "a") :json)))
    (is (zero? @calls))
    (is (thrown? Error (native/fields {1 :invalid})))
    (is (not= (names/identifier :ns/key) (names/identifier :ns-key)))
    (is (= :ns/key (names/selector (names/identifier :ns/key))))
    (is (thrown? Error
                 (codec/decode
                   :json
                   (json/encode ["fast-twitch/cljs-json" 3 nil]))))))

(deftest closed-rpc-options-and-safe-diagnostics
  (async done
         ((^:async fn
           []
           (let [calls (atom 0)
                 receiver #js {}
                 method :counter/change
                 native-error (Error. "native rejection")]
             (aset receiver
                   (names/identifier method)
                   (fn [value]
                     (swap! calls inc)
                     value))
             (doseq [options [{:args-scehma [:cat :string]} {:codec :unknown}
                              {:args-schema [:cat :string]}]]
               (try (await (rpc/call! receiver method [1] options))
                    (is false "Invalid RPC options/arguments must reject")
                    (catch :default error
                      (let [data (ex-data error)]
                        (is (= method (:method data)))
                        (is (vector? (:path data)))
                        (is (keyword? (:expected data)))
                        (is (< (count (pr-str data)) 1024))))))
             (is (zero? @calls))
             (is (= {:tenant/value :status/ready}
                    (await (rpc/call! receiver
                                      method
                                      [{:tenant/value :status/ready}]
                                      {:codec :json}))))
             (is (= 1 @calls))
             (aset receiver
                   (names/identifier method)
                   (fn [& _]
                     (throw native-error)))
             (try (await (rpc/call! receiver method []))
                  (is false)
                  (catch :default error (is (identical? native-error error))))
             (done))))))

(deftest effective-fetch-options-before-request-construction
  (async
    done
    ((^:async fn
      []
      (let [calls (atom 0)
            receiver #js {:fetch (fn [_]
                                   (swap! calls inc)
                                   (Response. "ok"))}
            initial (Request. "http://fixture/" #js {:method "POST" :body "owned"})
            view (request/request->map initial)
            request (assoc view :url "http://fixture/")]
        (doseq [options [{:request-init {:cache :no-store}}
                         {:request-middleware [(fn [m]
                                                 (assoc-in m
                                                   [:fast-twitch.client/options
                                                    :request-init :cache]
                                                   :no-store))]}]]
          (try (await ((http/client-for receiver options) request))
               (is false)
               (catch :default error
                 (is (= [:request-init :cache] (:path (ex-data error))))
                 (is (= :unavailable (:boundary (ex-data error)))))))
        (is (zero? @calls))
        (is (false? (.-bodyUsed initial)))
        (is (false? (.-locked (.-body initial))))
        (is (= 200
               (:status (await ((http/client-for receiver)
                                 {:url "http://fixture/" :request-method :get})))))
        (is (= 1 @calls))
        (is (= 200
               (:status (await ((http/client-for receiver
                                                 {:request-init {:cache :no-store}
                                                  :request-middleware
                                                    [(fn [m]
                                                       (update-in
                                                         m
                                                         [:fast-twitch.client/options
                                                          :request-init]
                                                         dissoc
                                                         :cache))]})
                                 {:url "http://fixture/" :request-method :get})))))
        (is (= 2 @calls))
        (let [seen (atom nil)
              endpoint #js {:fetch (^:async fn
                                    [native]
                                    (reset! seen (await (.text native)))
                                    (Response. "{}"))}]
          (await ((http/client-for endpoint {:codec :json})
                   {:url "http://fixture/"
                    :request-method :post
                    :body {:name "x" :tenant/id :status/ready}}))
          (is (= {:name "x" :tenant/id "status/ready"}
                 (json/decode @seen)))
          (try (await
                 ((http/client-for receiver {:codec :json})
                   {:url "http://fixture/" :request-method :post :body {1 :invalid}}))
               (is false "Non-keyword JSON keys must reject")
               (catch :default error
                 (is (some? error))))
          (is (= 2 @calls)))
        (done))))))

(deftest keyword-metadata-and-null-projections
  (async
    done
    ((^:async fn
      []
      (let [saved (atom nil)
            receiver #js {:put (fn [_ _ options]
                                 (reset! saved (aget options "metadata")))
                          :list (fn []
                                  #js {:keys #js [#js {:name "first" :metadata @saved}]
                                       :list_complete true})
                          :getWithMetadata
                            (fn [keys]
                              (let [record #js {:value "value" :metadata @saved}]
                                (if (array? keys)
                                  (Map. #js [#js [(names/text :first) record]
                                             #js [(names/text :second) record]])
                                  record)))}]
        (is (nil? (:metadata (await (service-kv/get-with-metadata! receiver :first)))))
        (is (every? #(nil? (:metadata %))
                    (vals (await (service-kv/get-with-metadata! receiver
                                                                [:first :second])))))
        (await (service-kv/put! receiver
                                :first
                                "value"
                                {:metadata {:tenant/kind :record/customer}}))
        (is (= {:tenant/kind :record/customer}
               (:metadata (await (service-kv/get-with-metadata! receiver :first)))))
        (is (= {:tenant/kind :record/customer}
               (:metadata (get (await (service-kv/get-with-metadata! receiver
                                                                     [:first :second]))
                               :first))))
        (is (= {:tenant/kind :record/customer}
               (:metadata (first (:keys (await (service-kv/list! receiver)))))))
        (doseq [external ["external opaque metadata" false]]
          (reset! saved external)
          (is (= external
                 (:metadata (await (service-kv/get-with-metadata! receiver :first))))))
        (reset! saved (json/encode ["fast-twitch/cljs-json" 3 nil]))
        (try (await (service-kv/get-with-metadata! receiver :first))
             (is false "Malformed owned metadata must reject")
             (catch :default _ (is true)))
        (done))))))

(deftest honeysql-named-parameters-and-keyword-rows
  (let [calls (atom 0)
        seen (atom nil)
        cursor #js {:columnNames #js []}
        receiver #js {:exec (fn [& args]
                              (swap! calls inc)
                              (reset! seen (vec args))
                              cursor)
                      :prepare (fn [text]
                                 (swap! calls inc)
                                 (reset! seen [text])
                                 #js {:bind (fn [& values]
                                              (swap! seen into values))})}
        query {:select [[[:param :user/id] :user/id] [[:param :tenant/id] :tenant/id]]}
        params {:user/id 7 :tenant/id 9}]
    (is (identical? cursor (sql/exec receiver query params)))
    (is (= [7 9] (subvec @seen 1)))
    (is (.includes (first @seen) (names/identifier :user/id)))
    (is (.includes (first @seen) (names/identifier :tenant/id)))
    (d1/prepare receiver query params)
    (is (= [7 9] (subvec @seen 1)))
    (sql/exec receiver
              {:select [:id] :from [:items] :where [:in :id [:param :filter/ids]]}
              {:filter/ids [1 2]})
    (is (= [1 2] (subvec @seen 1)))
    (d1/prepare receiver
                {:select [:id] :from [:items] :where [:in :id [:param :filter/ids]]}
                {:filter/ids [1 2]})
    (is (= [1 2] (subvec @seen 1)))
    (reset! calls 0)
    (doseq [[query params] [[nil {}] [query {}]
                            [{:select [[[:param :id] :same] [[:param :other] :same]]}
                             {:id 1 :other 2}] [{:select [[1 :value]]} {}]
                            [query {:user/id 9007199254740992 :tenant/id 9}]
                            [{:raw [:unsupported]} {}]]]
      (is (thrown? Error (sql/exec receiver query params)))
      (is (thrown? Error (d1/prepare receiver query params))))
    (is (zero? @calls))
    (let [row #js {}]
      (aset row (names/identifier :user/id) 7)
      (aset row (names/identifier :tenant/id) 9)
      (is (= {:user/id 7 :tenant/id 9} (sql/row-map row))))
    (let [read (atom 0)
          cursor (fn []
                   #js {:next (fn []
                                (let [index (swap! read inc)]
                                  (if (> index 3)
                                    #js {:done true}
                                    #js {:done false :value #js {:id index}})))})
          failure (Error. "primary")]
      (is (thrown? Error (sql/all-rows (cursor) 1)))
      (is (= 4 @read))
      (reset! read 0)
      (is (= {:id 1}
             (sql/reduce-rows (cursor)
                              (fn [_ row]
                                (reduced row))
                              nil)))
      (is (= 4 @read))
      (reset! read 0)
      (try (sql/reduce-rows (cursor)
                            (fn [& _]
                              (throw failure))
                            nil)
           (is false)
           (catch :default error (is (identical? failure error))))
      (is (= 4 @read)))))

(deftest keyword-kv-preflight-and-full-namespace
  (let [calls (atom 0)
        seen (atom nil)
        receiver #js {:get (fn [key]
                             (swap! calls inc)
                             (reset! seen key)
                             7)
                      :put (fn [& args]
                             (swap! calls inc)
                             (reset! seen (vec args)))}]
    (is (= {:found? true :value 7} (kv/get-value receiver :tenant/id)))
    (is (= "tenant/id" @seen))
    (is (thrown? Error (kv/get-value receiver nil)))
    (is (= 1 @calls))
    (try (validation/at! {:method :secret/method}
                         #(codec/encode :json
                                        {:value (fn []
                                                  nil)}))
         (is false)
         (catch :default error
           (is (= :secret/method (:method (ex-data error))))
           (is (some? (ex-cause error)))))
    (let [raw #js {:first 1 :second 2}]
      (is (thrown? Error (native/data-map raw (constantly :duplicate)))))))

(deftest bounded-identity-and-missing-binding-diagnostics
  (async done
         ((^:async fn
           []
           (try (await (rpc/call! #js {} (.repeat "SECRET_SENTINEL_66af" 200) []))
                (is false)
                (catch :default error
                  (let [text (pr-str (ex-data error))]
                    (is (< (count text) 1024))
                    (is (not (.includes text "SECRET_SENTINEL_66af"))))))
           (done)))))

(deftest bounded-option-path-diagnostics
  (let [key (keyword (.repeat "REVIEW_SENTINEL_" 500))]
    (try (validation/check! [:map {:closed true} [:native? {:optional true} :boolean]]
                            {key true}
                            :fetch)
         (is false)
         (catch :default error
           (is (= [:redacted-segment] (:path (ex-data error))))
           (is (< (count (pr-str (ex-data error))) 1024))
           (is (not (.includes (pr-str (ex-data error)) "REVIEW_SENTINEL_"))))))
  (is (= [:native?]
         (:path (contracts/issue [:map [:native? :boolean]] {:native? "false"})))))

(deftest keyword-kv-selectors-reject-before-effects
  (async
    done
    ((^:async fn
      []
      (let [calls (atom 0)
            effect (fn [& _]
                     (swap! calls inc))
            receiver #js {:get effect :put effect :list effect :delete effect}]
        (doseq [f [#(kv/get-value receiver "tenant/id") #(kv/put! receiver "tenant/id" 1)
                   #(kv/list-values receiver {:prefix "tenant/"} :native)
                   #(service-kv/delete! receiver "tenant/id")]]
          (is (thrown? Error (f))))
        (doseq [f [#(storage/get! receiver [:tenant/id "tenant/invalid"])
                   #(storage/put! receiver "tenant/id" 1)
                   #(storage/list-entries! receiver {:start :tenant/id :end "tenant/z"})
                   #(service-kv/get! receiver "tenant/id")]]
          (try (await (f))
               (is false "String KV selectors must reject before native effects")
               (catch :default error (is (some? error)))))
        (is (zero? @calls)))
      (done)))))

(deftest optional-native-arguments
  (let [seen (atom [])
        pending (Promise.resolve nil)
        receiver #js {}]
    (doseq [method ["sendBatch" "start" "exec" "kill" "destroy"]]
      (aset receiver
            method
            (fn []
              (swap! seen conj [method (js* "arguments.length")])
              pending)))
    (is (identical? pending (queues/send-batch! receiver [{:body 1}])))
    (containers/start! receiver)
    (containers/start! receiver {})
    (containers/exec! receiver ["echo"])
    (containers/exec! receiver ["echo"] {})
    (containers/kill! receiver)
    (containers/kill! receiver 9)
    (containers/destroy! receiver)
    (containers/destroy! receiver nil)
    (is (= [["sendBatch" 1] ["start" 0] ["start" 1] ["exec" 1] ["exec" 2] ["kill" 0]
            ["kill" 1] ["destroy" 0] ["destroy" 1]]
           @seen))
    (doseq [invoke [#(queues/send-batch! receiver [{:body 1}] {} {})
                    #(containers/start! receiver nil)
                    #(containers/exec! receiver ["echo"] {:stdout :invalid})
                    #(containers/kill! receiver nil)
                    #(containers/destroy! receiver nil nil)
                    #(r2/create-multipart! receiver :object {:storageClass "Standard"})]]
      (is (thrown? js/Error (invoke))))
    (is (= 9 (count @seen)))))

(deftest r2-keyword-selectors-and-invalid-options-before-effects
  (async done
         ((^:async fn
           []
           (let [calls (atom 0)
                 seen (atom nil)
                 receiver #js {:list (fn [options]
                                       (swap! calls inc)
                                       (reset! seen options)
                                       #js {:objects #js []
                                            :truncated false
                                            :delimitedPrefixes #js []})}]
             (await (r2/list! receiver
                              {:prefix :tenant/path
                               :include [:httpMetadata :customMetadata]
                               :cursor "opaque-cursor"}))
             (is (= "tenant/path" (aget @seen "prefix")))
             (is (= ["httpMetadata" "customMetadata"] (vec (aget @seen "include"))))
             (is (= "opaque-cursor" (aget @seen "cursor")))
             (doseq [options [{:include nil} {:include [:invalid]} {:prefix false}
                              {:cursor :invalid}]]
               (try (await (r2/list! receiver options))
                    (is false "Invalid options must reject before native list")
                    (catch :default error (is (some? error)))))
             (is (= 1 @calls)))
           (done)))))

(defmethod cljs.test/report [:cljs.test/default :end-run-tests]
  [result]
  (set! (.-exitCode process) (if (cljs.test/successful? result) 0 1)))

(defn -main
  []
  (run-tests 'fast-twitch.celld.test-main))

(set! *main-cli-fn* -main)
