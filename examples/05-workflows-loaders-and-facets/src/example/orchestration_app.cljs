(ns example.orchestration-app
  "Native Workflows replay steps; native loaded CLJS Cells own separate facet storage."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.bindings :as bindings]
            [fast-twitch.celld.identity :as identity]
            [fast-twitch.celld.http :as http]
            [fast-twitch.celld.rpc :as rpc]
            [fast-twitch.celld.native :as native]
            [fast-twitch.celld.services.workflows :as workflows]
            [fast-twitch.celld.services.loaders :as loaders]
            [fast-twitch.celld.services.facets :as facets]
            [fast-twitch.celld.services.assets :as assets]
            [fast-twitch.util.http.response :as http-response]
            [fast-twitch.codecs.json :as json])
  (:require-macros [fast-twitch.celld.macros :refer
                    [defcell defrpc deffetch defworker defworkflow]])
  (:refer-global :only [Date Promise crypto URLSearchParams setTimeout AbortController]))

(defworkflow
  ExampleFlow
  {:binding :FLOW
   :codec :json
   :args [:map-of :keyword :any]
   :returns [:map-of :keyword :any]}
  [_ctx event step]
  (let [sensitive? (try (workflows/do! step
                                       "unavailable"
                                       {:sensitive :output}
                                       (fn []
                                         nil))
                        false
                        (catch :default error
                          (= :unavailable (:boundary (ex-data error)))))
        payload (req! event :payload)
        initial (await (workflows/do!
                         step
                         "initial"
                         {:retries {:limit 0 :delay "1 second"} :timeout "10 seconds"}
                         (fn [step-context]
                           {:value (get payload :value 7)
                            :native-step-context
                              (and (= 1 (.-attempt step-context))
                                   (= "initial" (.-name (.-step step-context)))
                                   (= "10 seconds" (.-timeout (.-config step-context))))})
                         :json))]
    (when (get payload :fail)
      (await (workflows/do! step
                            "failure"
                            (fn []
                              (throw (workflows/non-retryable-error
                                       "fixture permanent failure"))))))
    (doseq [n (range 2)]
      (await (workflows/do! step
                            "repeated"
                            (fn []
                              n))))
    (await (workflows/sleep! step "short-sleep" 20))
    (await (workflows/sleep-until! step "absolute-sleep" (Date.now)))
    (let [received (await (workflows/wait-for-event! step
                                                     "event"
                                                     {:type :continue :timeout "1 minute"}
                                                     :json))]
      {:value (get initial :value)
       :event (req! received :payload)
       :native-step-context (get initial :native-step-context)
       :sensitive-unavailable sensitive?})))

(defrpc
  ^:async loader-checks
  {:method :loaderChecks :args [:cat] :returns [:map-of :keyword :boolean] :codec :json}
  [ctx]
  (let [asset-binding (bindings/get-binding ctx :ASSETS)
        asset-request {:url "http://assets/loaded.mjs" :request-method :get}
        plain (await (assets/fetch! asset-binding asset-request))
        plain-native (req! plain :fast-twitch.routing/response)
        asset-identity (identical? plain-native (http-response/map->response plain))
        _ (await (.cancel (req! plain :body)))
        same-body (atom false)
        edited (await
                 ((assets/client-for
                    asset-binding
                    {:response-middleware
                       [(fn [response]
                          (reset! same-body
                            (identical? (req! response :body)
                                        (.-body (req! response
                                                      :fast-twitch.routing/response))))
                          (assoc-in response [:headers :x-asset-middleware] "edited"))]})
                   asset-request))
        response (http-response/map->response edited)
        source (await (.text response))
        ;; Celld caches text rereads. The shared converter still guards a consumed
        ;; carrier.
        repeated-source (await (.text response))
        consumed-guard?
          (try (http-response/map->response (http-response/response->map response))
               false
               (catch :default error
                 (= :fast-twitch.http/body-unavailable (:code (ex-data error)))))
        code (loaders/code {:mainModule "loaded.mjs"
                            :modules {:loaded.mjs source}
                            :compatibilityDate "2026-10-07"
                            :compatibilityFlags [:js_rpc]
                            :env {:explicit "only"}
                            :limits {:cpuMs 1000 :subRequests 10}})
        loader (bindings/get-binding ctx :LOADER)
        calls (atom 0)
        loaded (loaders/get-worker loader
                                   "fixture-v1"
                                   (fn []
                                     (swap! calls inc)
                                     code))
        default (loaders/get-entrypoint loaded)
        default-response (await (native/invoke default "fetch" ["http://dynamic/"]))
        named (loaders/get-entrypoint loaded
                                      "Named"
                                      {:props #js {:role "fixture"}
                                       :limits {:subRequests 5}})
        named-response (await (native/invoke named "fetch" ["http://dynamic/"]))
        controller (AbortController.)
        _ (setTimeout (fn []
                        (.abort controller))
                      25)
        abort? (try (await ((http/client-for named
                                             {:request-init {:signal (.-signal
                                                                       controller)}})
                             {:url "http://dynamic/slow" :request-method :get}))
                    false
                    (catch :default error (= "AbortError" (.-name error))))
        ping (await (rpc/call! named :ping [] {}))
        registry (context/facets ctx)
        child (facets/get-facet
                registry
                "loaded-child"
                (fn []
                  (facets/startup
                    (loaders/get-cell-class loaded "Child" {:props #js {:role "child"}})
                    (context/id ctx))))
        first (await (rpc/call! child :increment [] {}))
        second (await (rpc/call! child :increment [] {}))
        child-response (await (native/invoke child "fetch" ["http://child/"]))
        _ (facets/abort! registry "loaded-child" "fixture reset")
        resumed (facets/get-facet registry
                                  "loaded-child"
                                  (fn []
                                    (facets/startup (loaders/get-cell-class loaded
                                                                            "Child"))))
        third (await (rpc/call! resumed :increment [] {}))
        _ (await (facets/delete! registry "loaded-child"))
        fresh (facets/get-facet registry
                                "loaded-child"
                                (fn []
                                  (facets/startup (loaders/get-cell-class loaded
                                                                          "Child"))))
        reset-count (await (rpc/call! fresh :increment [] {}))
        _ (await (facets/delete! registry "loaded-child"))
        anonymous (loaders/load loader code)
        anonymous-entry (loaders/get-entrypoint anonymous)
        anonymous-response (await
                             (native/invoke anonymous-entry "fetch" ["http://dynamic/"]))
        _ (loaders/dispose! anonymous)
        disposed? (try (await (native/invoke anonymous-entry "fetch" ["http://dynamic/"]))
                       false
                       (catch :default _ true))
        _ (loaders/dispose! loaded)]
    {:loaded-caller-abort abort?
     :asset-source (> (count source) 1000)
     :asset-identity asset-identity
     :asset-middleware (and @same-body
                            (= "edited" (.get (.-headers response) "x-asset-middleware")))
     :asset-body-consumed (.-bodyUsed response)
     :asset-native-reread-observed (= source repeated-source)
     :asset-consumed-converter-guard consumed-guard?
     :default (= "dynamic-default" (await (.text default-response)))
     :named-fetch (= "named-fetch" (await (.text named-response)))
     :named-rpc (= "named-worker" ping)
     :memoized-code (= 1 @calls)
     :facet-fetch (= "compiled-child" (await (.text child-response)))
     :facet-storage (= second (inc first))
     :facet-abort-retains (= third (inc second))
     :facet-delete-resets (= 1 reset-count)
     :anonymous (= "dynamic-default" (await (.text anonymous-response)))
     :disposed disposed?}))

(deffetch ^:async facet-fetch
          [ctx request]
          (let [loader (bindings/get-binding ctx :LOADER)
                response (await (native/invoke (bindings/get-binding ctx :ASSETS)
                                               "fetch"
                                               ["http://assets/loaded.mjs"]))
                source (await (.text response))
                loaded (loaders/get-worker loader
                                           "socket-fixture"
                                           (fn []
                                             (loaders/code
                                               {:mainModule "loaded.mjs"
                                                :modules {:loaded.mjs source}
                                                :compatibilityDate "2026-10-07"
                                                :compatibilityFlags [:js_rpc]})))
                child (facets/get-facet (context/facets ctx)
                                        "socket-child"
                                        (fn []
                                          (facets/startup
                                            (loaders/get-cell-class loaded "Child"))))]
            (if (= "/facet-outbound" (:uri request))
              {:status 200 :body (str (await (rpc/call! child :outbound [])))}
              (await (http/forward! child request)))))

(defcell Supervisor {:binding :SUPERVISORS :include [loader-checks facet-fetch]})

(defn ^:async settled!
  [instance statuses]
  (loop [attempt 0]
    (let [status (await (workflows/status! instance :json))]
      (if (statuses (get status :status))
        status
        (if (< attempt 80)
          (do (await (Promise. (fn [resolve _]
                                 (setTimeout resolve 25))))
              (recur (inc attempt)))
          (throw (ex-info "Workflow did not settle"
                          {:status (get status :status) :error (:error status)})))))))

(defn ^:async workflow-checks
  [ctx]
  (let [flow (bindings/get-binding ctx :FLOW)
        id (str "controls-" (.randomUUID crypto))
        instance (await (workflows/create!
                          flow
                          {:id id :params {:value 11} :locationHint :apac-se}
                          :json))
        looked-up (await (workflows/get! flow id))
        waiting (await (settled! instance #{:waiting}))
        _ (await (workflows/pause! instance))
        paused (await (settled! instance #{:paused}))
        _ (await (workflows/resume! instance))
        _ (await (workflows/send-event! instance
                                        {:type :continue :payload {:ready true}}
                                        :json))
        complete (await (settled! instance #{:complete}))
        _ (await (workflows/restart! instance
                                     {:from {:name "initial" :count 1 :type :do}}))
        _ (await (workflows/send-event! instance
                                        {:type :continue :payload {:ready "restart"}}
                                        :json))
        restarted (await (settled! instance #{:complete}))
        _ (await (workflows/delete! instance))
        deleted? (try (await (workflows/get! flow id)) false (catch :default _ true))
        batch (await (workflows/create-batch! flow
                                              [{:id (str id "-batch") :params {:value 1}}]
                                              :json))
        batch-instance (aget batch 0)
        _ (await (workflows/terminate! batch-instance))
        terminated (await (settled! batch-instance #{:terminated}))
        _ (await (workflows/delete-batch! flow [(workflows/id batch-instance)]))
        batch-deleted? (try (await (workflows/get! flow (str id "-batch")))
                            false
                            (catch :default _ true))
        failed (await (workflows/create! flow
                                         {:id (str id "-error") :params {:fail true}}
                                         :json))
        failure (await (settled! failed #{:errored}))
        _ (await (workflows/delete! failed))
        invalid? (try (workflows/create! flow {:locationHint "unsupported"})
                      false
                      (catch :default _ true))
        empty-batch?
          (try (workflows/create-batch! flow []) false (catch :default _ true))]
    {:looked-up (= id (workflows/id looked-up))
     :batch-deleted batch-deleted?
     :instance-id (= (str id "-batch") (workflows/id batch-instance))
     :waiting-no-output (and (= :waiting (get waiting :status))
                             (not (contains? waiting :output)))
     :paused (= :paused (get paused :status))
     :output-codec (= {:value 11
                       :event {:ready true}
                       :native-step-context true
                       :sensitive-unavailable true}
                      (get complete :output))
     :restart-from (= "restart" (get-in restarted [:output :event :ready]))
     :deleted deleted?
     :batch (= 1 (.-length batch))
     :terminated (= :terminated (get terminated :status))
     :non-retryable (= "NonRetryableError" (get-in failure [:error :name]))
     :invalid-options invalid?
     :empty-batch-rejected empty-batch?}))

(deffetch
  ^:async worker-fetch
  [ctx request]
  (let [flow (bindings/get-binding ctx :FLOW)]
    (case (:uri request)
      "/facet-ws" (await (http/forward! (identity/get-by-name
                                          (bindings/get-binding ctx :SUPERVISORS)
                                          "socket-suite")
                                        request))
      "/facet-state" (await (http/forward! (identity/get-by-name
                                             (bindings/get-binding ctx :SUPERVISORS)
                                             "socket-suite")
                                           request))
      "/facet-outbound" (await (http/forward! (identity/get-by-name
                                                (bindings/get-binding ctx :SUPERVISORS)
                                                "socket-suite")
                                              request))
      "/workflows" {:status 200 :body (json/encode (await (workflow-checks ctx)))}
      "/loaders" (let [stub (identity/get-by-name (bindings/get-binding ctx :SUPERVISORS)
                                                  "fixture")]
                   {:status 200
                    :body (json/encode
                            (await (rpc/call! stub :loaderChecks [] {:codec :json})))})
      "/create" (let [instance (await (workflows/create!
                                        flow
                                        {:id (str "fixture-" (.randomUUID crypto))
                                         :params {:value 9}
                                         :locationHint :apac-ne
                                         :retention {:successRetention "1 day"}}
                                        :json))]
                  (await (workflows/send-event! instance
                                                {:type :continue :payload {:ready true}}
                                                :json))
                  {:status 200 :body (json/encode {:id (workflows/id instance)})})
      "/status"
        (let [instance (await (workflows/get!
                                flow
                                (.get (URLSearchParams. (:query-string request)) "id")))]
          {:status 200 :body (json/encode (await (workflows/status! instance :json)))})
      {:status 200 :body "orchestration"})))

(defworker App {:include [worker-fetch]})
