(ns example.container-app
  "Optional engine fixture; native process and socket resources stay event-scoped."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.bindings :as bindings]
            [fast-twitch.celld.identity :as identity]
            [fast-twitch.celld.rpc :as rpc]
            [fast-twitch.celld.native :as native]
            [fast-twitch.celld.services.containers :as containers]
            [fast-twitch.client.core :as client]
            [fast-twitch.codecs.json :as json]
            [fast-twitch.util.streams.readers :as readers])
  (:require-macros [fast-twitch.celld.macros :refer [defcell defrpc deffetch defworker]])
  (:refer-global :only [Promise setTimeout TextDecoder console Date Uint8Array]))

(defn ^:async listening!
  [port]
  (loop [attempt 0]
    (let [result (try (await ((containers/client-for port {:codec :text})
                               {:url "http://fixture/" :request-method :get}))
                      (catch :default _ nil))]
      (if result
        result
        (if (< attempt 40)
          (do (await (Promise. (fn [resolve _]
                                 (setTimeout resolve 100))))
              (recur (inc attempt)))
          (throw (ex-info "Container did not become ready" {})))))))

(defrpc phase
        {:method :phase :args [:cat] :returns :string}
        [ctx]
        (get @(req! ctx :state) :phase "idle"))

(defrpc
  ^:async container-checks
  {:method :containerChecks
   :args [:cat]
   :returns [:map-of :keyword :boolean]
   :codec :json}
  [ctx]
  (try
    (let [container (context/container ctx)]
      (when (containers/running? container) (await (containers/destroy! container)))
      (swap! (req! ctx :state) assoc :phase "starting")
      (containers/start!
        container
        {:enableInternet true :env {:FIXTURE "native"} :labels {:fixture "phase-one"}})
      (let [port (containers/tcp-port container 8080)
            response (await (listening! port))
            _ (swap! (req! ctx :state) assoc :phase "port-ready")
            running (containers/running? container)
            _ (await (containers/set-inactivity-timeout! container 60000))
            signal-return (containers/signal! container 18)
            signal-running (containers/running? container)
            process (await (containers/exec!
                             container
                             ["sh" "-c" "printf $FIXTURE; printf stderr >&2"]
                             {:stdout :pipe :stderr :pipe :env {:FIXTURE "exec"}}))
            process-view (containers/process-map process)
            _ (swap! (req! ctx :state) assoc
                :debug
                {:native-pid (native/property process "pid")
                 :mapped-pid (:pid process-view)})
            output (await (containers/output! process))
            _ (swap! (req! ctx :state) assoc :phase "exec-output")
            stdout (.decode (TextDecoder.) (req! output :stdout))
            stderr (.decode (TextDecoder.) (req! output :stderr))
            once? (try (await (containers/output! process)) false (catch :default _ true))
            combined (await (containers/exec! container
                                              ["sh" "-c" "printf out; printf err >&2"]
                                              {:stderr :combined}))
            combined-output (await (containers/output! combined))
            ignored (await (containers/exec! container
                                             ["sh" "-c"
                                              "printf ignored; printf ignored >&2"]
                                             {:stdout :ignore :stderr :ignore}))
            ignored-output (await (containers/output! ignored))
            input (await (containers/exec! container ["cat"] {:stdin :pipe}))
            writer (.getWriter (:stdin (containers/process-map input)))
            _ (try (await (.write writer (Uint8Array. #js [105 110 112 117 116])))
                   (finally (await (.close writer)) (.releaseLock writer)))
            input-output (await (containers/output! input))
            streamed (await (containers/exec! container ["printf" "streamed"]))
            streamed-view (containers/process-map streamed)
            streamed-output (await (readers/read! (:stdout streamed-view)
                                                  {:codec :text :max-bytes 1024}))
            streamed-exit (await (:exit-code streamed-view))
            killed (await (containers/exec! container ["sleep" "3"]))
            kill-start (Date.now)
            _ (swap! (req! ctx :state) update
                :debug assoc
                :killed-pid (native/property killed "pid"))
            _ (containers/kill! killed 15)
            killed-output (await (containers/output! killed))
            _ (swap! (req! ctx :state) update
                :debug assoc
                :kill-exit-code (req! killed-output :exitCode)
                :kill-elapsed-ms (- (Date.now) kill-start))
            _ (swap! (req! ctx :state) assoc :phase "exec-killed")
            socket (containers/connect! port {} {:codec :text})
            _ (await (client/send! socket "GET / HTTP/1.0\r\nHost: fixture\r\n\r\n"))
            bytes (await (client/receive! socket))
            _ (await (client/close! socket))
            _ (swap! (req! ctx :state) assoc :phase "tcp-received")
            monitor ((^:async fn
                      []
                      (try (await (containers/monitor! container))
                           nil
                           (catch :default error error))))
            _ (await (containers/destroy! container))
            _ (swap! (req! ctx :state) assoc :phase "destroyed")
            monitored (= 137 (.-exitCode (await monitor)))]
        {:inactivity-configured true
         :signal-requested (and (nil? signal-return) signal-running)
         :running running
         :http (= "container-ready" (:body response))
         :exec-stdout (= "exec" stdout)
         :exec-stderr (= "stderr" stderr)
         :exec-exit (= 0 (req! output :exitCode))
         :process-pid (= (:pid process-view) (native/property process "pid"))
         :exec-combined (= "outerr"
                           (.decode (TextDecoder.) (req! combined-output :stdout)))
         :exec-ignore (and (= 0 (.-byteLength (req! ignored-output :stdout)))
                           (= 0 (.-byteLength (req! ignored-output :stderr))))
         :exec-stdin-pipe (= "input" (.decode (TextDecoder.) (req! input-output :stdout)))
         :exec-streamed-default-options (and (= "streamed" streamed-output)
                                             (= 0 streamed-exit))
         :output-once once?
         :kill (not= 0 (req! killed-output :exitCode))
         :tcp (boolean (re-find #"HTTP/1" bytes))
         :monitor monitored
         :destroy (not (containers/running? container))}))
    (catch :default error (throw (js/Error. (str error " " (pr-str (ex-data error))))))))

(defrpc debug
        {:method :debug :args [:cat] :codec :json}
        [ctx]
        (get @(req! ctx :state) :debug {}))

(defrpc ^:async cleanup
        {:method :cleanup :args [:cat] :returns :boolean}
        [ctx]
        (let [container (context/container ctx)]
          (when (containers/running? container) (await (containers/destroy! container)))
          (not (containers/running? container))))

(defcell ContainerCell
         {:binding :CONTAINERS :include [container-checks phase debug cleanup]})

(deffetch
  ^:async worker-fetch
  [ctx request]
  (let [stub (identity/get-by-name (bindings/get-binding ctx :CONTAINERS) "fixture")]
    (try
      (case (:uri request)
        "/debug" {:status 200
                  :body (json/encode (await (rpc/call! stub :debug [] {:codec :json})))}
        "/phase" {:status 200 :body (await (rpc/call! stub :phase []))}
        "/cleanup" {:status 200 :body (str (await (rpc/call! stub :cleanup [])))}
        {:status 200
         :body (json/encode (await
                              (rpc/call! stub :containerChecks [] {:codec :json})))})
      (catch :default error
        {:status 500 :body (str error " " (pr-str (ex-data error)))}))))

(defworker App {:include [worker-fetch]})
