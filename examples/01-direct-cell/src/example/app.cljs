(ns example.app
  "Minimal directly exported Cell with native RPC and ordinary Fast-Twitch Fetch."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.storage.kv :as cell-kv]
            [fast-twitch.celld.native :as native]
            [malli.core :as m]
            [fast-twitch.celld.http :as http]
            [fast-twitch.celld.rpc :as rpc]
            [fast-twitch.celld.bindings :as bindings]
            [fast-twitch.celld.identity :as identity]
            [fast-twitch.client.core :as client]
            [fast-twitch.codecs.json :as json])
  (:require-macros [fast-twitch.celld.macros :refer
                    [defcell defrpc deffetch defworker defcell-init]])
  (:refer-global :only [Promise setTimeout Object crypto Reflect URLSearchParams]))

(declare Counter)

(defcell-init ^:async initialize
              [ctx]
              (when (= "init-failure" (identity/id-name (context/id ctx)))
                (throw (ex-info "Owned initialization failure" {})))
              (await (Promise. (fn [resolve _]
                                 (setTimeout resolve 20))))
              (swap! (req! ctx :state) assoc
                :ready true
                :initializations 1
                :activation-token (str (crypto.randomUUID))))

(defrpc count-value
        {:method :countValue :args [:cat] :returns :int}
        [ctx]
        (get @(req! ctx :state) :count 0))

(defrpc add
        {:method :add :args [:cat :int] :returns :int}
        [ctx value]
        (:count (swap! (req! ctx :state) update :count (fnil + 0) value)))

(defrpc unused {:method :unused :args [:cat]} [_ctx] nil)

(defrpc
  activation-state
  {:method :activationState :args [:cat] :returns [:map-of :keyword :any] :codec :json}
  [ctx]
  {:id (identity/id-string (context/id ctx))
   :token (:activation-token @(req! ctx :state))
   :persisted (= :retained
                 (:value (cell-kv/get-value (cell-kv/native (context/storage ctx))
                                            :abort/marker
                                            :json)))
   :ready (:ready @(req! ctx :state))
   :count (get @(req! ctx :state) :count 0)})

(defrpc
  abort-activation
  {:method :abortActivation :args [:cat]}
  [ctx]
  (cell-kv/put! (cell-kv/native (context/storage ctx)) :abort/marker :retained :json)
  (context/abort! ctx "Owned abort/reset fixture"))

(defrpc adapter-cost
        {:method :adapterCost
         :args [:cat [:enum :baseline :validation :direct-call :adapter-call :options]]
         :codec :json}
        [_ctx operation]
        (let [iterations 20000
              arguments [7]
              target #js {:echo (fn [value]
                                  value)}
              options {:secureTransport :off}
              direct-arguments (to-array arguments)
              validation-check (m/validator :int)
              action (case operation
                       :baseline (fn []
                                   7)
                       :validation #(do (validation-check 7) 7)
                       :direct-call
                         #(Reflect.apply (.-echo target) target direct-arguments)
                       :adapter-call #(native/invoke target :echo arguments)
                       :options #(do (native/option-fields options) 7))
              result (loop [n iterations
                            total 0]
                       (if (zero? n) total (recur (dec n) (+ total (action)))))
              first-projection (native/option-fields options)
              next-projection (native/option-fields options)]
          {:operation operation
           :iterations iterations
           :result result
           :fresh-option-object (not (identical? first-projection next-projection))
           :allocation-estimate {:invoke-js-argument-arrays-per-call 1
                                 :options-js-field-objects-per-call 1}}))

(defrpc
  ^:async identity-checks
  {:method :identityChecks :args [:cat] :returns [:map-of :keyword :boolean] :codec :json}
  [ctx]
  (let [gated (atom false)
        _ (await (context/block-concurrency!
                   ctx
                   (^:async fn
                    []
                    (await (Promise.resolve true))
                    (reset! gated true))))
        retained (Promise.resolve true)
        registered (context/wait-until! ctx retained)
        settled (await retained)]
    {:context-block-concurrency @gated
     :context-wait-until (and (nil? registered) settled)
     :context-props-absent (nil? (context/props ctx))
     :context-container-absent (nil? (context/container ctx))
     :context-exports-present (some? (context/exports ctx))
     :ready-after-gate (= true (:ready @(req! ctx :state)))
     :one-initialization (= 1 (:initializations @(req! ctx :state)))
     :context-id (string? (identity/id-string (context/id ctx)))
     :actual-constructor (identical? Counter (.-constructor (.-prototype Counter)))
     :plain-prototype (identical? (.-prototype Object)
                                  (Object.getPrototypeOf (.-prototype Counter)))
     :intended-methods-only (= #{"constructor" "countValue" "add" "fetch" "identityChecks"
                                 "activationState" "abortActivation" "adapterCost"}
                               (set (array-seq (Object.getOwnPropertyNames
                                                 (.-prototype Counter)))))}))

(deffetch cell-fetch
          [ctx _request]
          {:status 200
           :headers {:x-cell "direct"}
           :body (str "ready=" (:ready @(req! ctx :state)))})

(defcell Counter
         {:binding :COUNTERS
          :include [initialize count-value add cell-fetch identity-checks activation-state
                    abort-activation adapter-cost]})

(defn ^:async isolation-checks
  "Executes distinct native Cells concurrently and checks namespace-scoped identity and initialization failure isolation."
  [ctx]
  (let [namespace (bindings/get-binding ctx :COUNTERS)
        suffix (str (crypto.randomUUID))
        left-name (str "left-" suffix)
        right-name (str "right-" suffix)
        left-id (identity/id-from-name namespace left-name)
        parsed (identity/id-from-string namespace (identity/id-string left-id))
        unique (identity/new-unique-id namespace)
        left (identity/get-stub namespace left-id)
        right (identity/get-by-name namespace right-name)
        values (await (Promise.all #js [(rpc/call! left :add [3])
                                        (rpc/call! right :add [7])]))
        identity-result (await (rpc/call! left :identityChecks [] {:codec :json}))
        failed? (try (await (rpc/call! (identity/get-by-name namespace "init-failure")
                                       :add
                                       [1]))
                     false
                     (catch :default _ true))]
    (merge identity-result
           {:concurrent-state-isolated (= [3 7] (vec (array-seq values)))
            :identity-round-trip (identity/equals? left-id parsed)
            :unique-is-distinct (not (identity/equals? left-id unique))
            :stub-id-preserved (identity/equals? left-id (identity/stub-id left))
            :native-name (= left-name (identity/id-name left-id))
            :initialization-failure failed?
            :healthy-after-failure (= 3 (await (rpc/call! left :countValue [])))})))

(deffetch
  ^:async worker-fetch
  [ctx request]
  (let [stub (identity/get-by-name (bindings/get-binding ctx :COUNTERS) "sample")]
    (case (:uri request)
      "/abort-state" {:status 200
                      :body (json/encode (await (rpc/call!
                                                  (identity/get-by-name
                                                    (bindings/get-binding ctx :COUNTERS)
                                                    "abort-fixture")
                                                  :activationState
                                                  []
                                                  {:codec :json})))}
      "/abort-add" {:status 200
                    :body (str (await (rpc/call! (identity/get-by-name
                                                   (bindings/get-binding ctx :COUNTERS)
                                                   "abort-fixture")
                                                 :add
                                                 [1])))}
      "/abort" (do (await (rpc/call! (identity/get-by-name
                                       (bindings/get-binding ctx :COUNTERS)
                                       "abort-fixture")
                                     :abortActivation
                                     []))
                   {:status 500 :body "Abort unexpectedly returned"})
      "/adapter-cost" {:status 200
                       :body (json/encode
                               (await (rpc/call! stub
                                                 :adapterCost
                                                 [(keyword (or (.get (URLSearchParams.
                                                                       (:query-string
                                                                         request))
                                                                     "operation")
                                                               "options"))]
                                                 {:codec :json})))}
      "/rpc" {:status 200 :body (str (await (rpc/call! stub :add [1])))}
      "/isolation" {:status 200 :body (json/encode (await (isolation-checks ctx)))}
      "/activation" {:status 200
                     :body (json/encode (await (rpc/call!
                                                 (identity/get-by-name
                                                   (bindings/get-binding ctx :COUNTERS)
                                                   "activation-fixture")
                                                 :activationState
                                                 []
                                                 {:codec :json})))}
      "/activation-add" {:status 200
                         :body (str (await (rpc/call! (identity/get-by-name
                                                        (bindings/get-binding ctx
                                                                              :COUNTERS)
                                                        "activation-fixture")
                                                      :add
                                                      [1])))}
      "/outbound" (await (client/request! (assoc request
                                            :server-name "127.0.0.1"
                                            :server-port 18991)
                                          {:transport :http}))
      (await (http/forward! stub request)))))

(defworker App {:include [worker-fetch]})
