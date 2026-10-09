(ns native-qualification
  "Builds and executes final Celld v0.6.2 fixture generations using owned local processes."
  (:require [babashka.fs :as fs]
            [cheshire.core :as json]
            [clojure.string :as str]
            [fast-twitch.celld.tooling :as t]))

(def defaults
  #{:direct-cell :storage :transports :services :orchestration :interop
    :generated-clients})

(def choices
  (conj defaults :corrections))

(defn build!
  [{:keys [local out artifacts]} app]
  (let [start (t/now)
        result (t/run! (t/clojure-command local "build" "build" app))
        fixture (str (fs/file-name (fs/parent app)))
        config-path (t/path (fs/parent app) ".celld-build/wrangler.json")]
    (t/write! (str (fs/path out (str fixture "-build.log")))
              (str (:out result) (:err result)))
    (t/success! result)
    (let [{:keys [bundle manifest fingerprint input-fingerprint]} (t/generation
                                                                    config-path)]
      (swap! artifacts conj
        {:fixture fixture
         :app app
         :fingerprint fingerprint
         :bundle-bytes (fs/size bundle)
         :gzip-bytes (t/gzip-bytes bundle)
         :build-seconds (t/rounded (t/elapsed start))
         :manifest (t/relative manifest)
         :manifest-sha256 (t/sha256 manifest)
         :bundle-sha256 (t/sha256 bundle)
         :input-fingerprint input-fingerprint}))
    config-path))

(defn native!
  [{:keys [out binary artifacts]} config port fixture f]
  (let [fingerprint (:fingerprint (peek @artifacts))
        log (str (fs/path out (str fixture "-native.log")))
        start (t/now)
        process (t/logged! [binary "dev" config "--host" "127.0.0.1" "--port" port
                            "--no-watch" "--logs"]
                           log
                           {:extra-env {"RUST_LOG" "warn"}})
        internal (atom nil)]
    (try
      (t/wait-for! #(let [text (or (t/text log) "")]
                      (reset! internal
                        (second (re-find #"celld internal listening on (127\.0\.0\.1:\d+)"
                                         text)))
                      (and (str/includes? text "ready  http://") @internal))
                   (str "Native readiness " fixture "; inspect " log)
                   30
                   process)
      (swap! artifacts update
        (dec (count @artifacts))
        assoc
        :startup-seconds
        (t/rounded (t/elapsed start)))
      (f {:origin (str "http://127.0.0.1:" port)
          :internal (str "http://" @internal)
          :fingerprint fingerprint})
      (finally (t/stop! process)))))

(defn record!
  [{:keys [results]} {:keys [fingerprint]} record]
  (swap! results conj (assoc record :artifact-fingerprint fingerprint)))

(defn checked!
  ([state context fixture]
   (checked! state context fixture "/" nil))
  ([state context fixture route]
   (checked! state context fixture route nil))
  ([state {:keys [origin] :as context} fixture route expected]
   (let [[text milliseconds] (t/get! origin route 30000)
         value (when-not expected (json/parse-string text true))]
     (t/ensure!
       (if expected (= text expected) (and (seq value) (every? true? (vals value))))
       (str fixture " " text))
     (record! state
              context
              {:fixture fixture :passed true :milliseconds milliseconds :result text})
     (println fixture "PASS")
     text)))

(defn node!
  [{:keys [origin internal]} script extra-env]
  (let [result (t/success! (t/run! ["node" script]
                                   {:timeout 30000
                                    :extra-env (merge {"CELLD_FIXTURE_ORIGIN" origin
                                                       "CELLD_FIXTURE_INTERNAL_ORIGIN"
                                                         internal}
                                                      extra-env)}))]
    (print (:out result))
    (flush)))

(defn direct!
  [state {:keys [origin] :as context}]
  (if (= "measurement" (System/getenv "CELLD_DIRECT_CASES"))
    (do (node! context "test/native_resource_budget.mjs" {"CELLD_MEASUREMENT_ONLY" "1"})
        (record! state
                 context
                 (t/read-json (t/path "target/native-resource-evidence.json"))))
    (do
      (let [before (json/parse-string (first (t/get! origin "/abort-state" 30000)) true)]
        (t/get! origin "/abort-add" 30000)
        (try
          (t/get! origin "/abort" 30000)
          (throw (ex-info "Native abort did not terminate the call" {}))
          (catch Exception error
            (t/ensure! (str/includes? (.getMessage error) "Owned abort/reset fixture")
                       (.getMessage error))))
        (let [after (json/parse-string (first (t/get! origin "/abort-state" 30000)) true)]
          (t/ensure! (and (= (:id before) (:id after))
                          (not= (:token before) (:token after))
                          (zero? (:count after))
                          (:ready after)
                          (:persisted after))
                     (pr-str after)))
        (record! state
                 context
                 {:fixture "context-abort-reset"
                  :passed true
                  :result (json/generate-string {:call-terminated true
                                                 :new-activation true
                                                 :transient-reset true
                                                 :durable-retained true})})
        (println "context-abort-reset PASS"))
      (checked! state context "direct-cell-init-fetch" "/" "ready=true")
      (checked! state context "outbound-native-fetch" "/outbound" "controlled")
      (checked! state
                context
                "direct-constructor-context-identity-and-init-failure"
                "/isolation")
      (node! context "test/native_activation.mjs" {})
      (record! state
               context
               (t/read-json (t/path "target/native-activation-evidence.json")))
      (let [first-call (parse-long (first (t/get! origin "/rpc" 30000)))
            second-call (parse-long (first (t/get! origin "/rpc" 30000)))]
        (t/ensure! (and first-call second-call (= second-call (inc first-call)))
                   "RPC calls did not increment"))
      (node! context "test/native_resource_budget.mjs" {})
      (record! state
               context
               (t/read-json (t/path "target/native-resource-evidence.json")))
      (let [timings (repeatedly 20 #(second (t/get! origin "/rpc" 30000)))]
        (record! state
                 context
                 {:fixture "native-rpc-repeated"
                  :passed true
                  :calls 22
                  :warm-mean-ms (/ (reduce + timings) (count timings))
                  :warm-max-ms (apply max timings)})))))

(defn storage!
  [state {:keys [origin] :as context}]
  (doseq [[fixture route expected]
            [["storage-sql-alarm-scheduled" "/"]
             ["bounded-cursors-write-drain-and-primary-error" "/resources"]
             ["native-storage-graph-options-and-lifetimes" "/coverage"]
             ["owned-storage-members-and-sql-results" "/members"]
             ["native-rpc-graph" "/native-rpc-graph"]
             ["rpc-invalid-input" "/bad-input" "rejected"]
             ["rpc-invalid-output" "/bad-output" "rejected"]]]
    (checked! state context fixture route expected))
  (t/wait-for! #(= "true" (first (t/get! origin "/alarm" 30000))) "Native alarm" 5 nil)
  (checked! state context "native-alarm-fired" "/alarm" "true")
  (checked! state context "native-alarm-info" "/alarm-metadata" "true"))

(defn services!
  [state {:keys [origin] :as context}]
  (let [selected (some-> (not-empty (System/getenv "CELLD_SERVICE_CASES"))
                         (str/split #",")
                         set)]
    (doseq [[fixture route] [["d1" "/"] ["kv" "/kv"] ["r2" "/r2"] ["queue-send" "/queue"]
                             ["queue-metrics" "/queue-metrics"]]
            :when (or (nil? selected) (selected fixture))]
      (checked! state context fixture route))
    (when (or (nil? selected) (selected "queue-send"))
      (t/wait-for! #(every? true?
                            (vals (json/parse-string
                                    (first (t/get! origin "/queue-state" 30000))
                                    true)))
                   "Queue state"
                   5
                   nil)
      (checked! state context "queue-consumed" "/queue-state")
      (checked! state context "prepare-new-cron-marker" "/prepare-cron")
      (t/wait-for! #(:fired (json/parse-string (first (t/get! origin "/cron" 30000))
                                               true))
                   "Native UTC cron"
                   65
                   nil)
      (checked! state context "native-utc-cron" "/cron"))))

(defn qualify!
  [{:keys [local fixtures reuse-origin]}]
  (let [selected (if fixtures (set (map keyword fixtures)) defaults)
        _ (t/ensure! (every? choices selected)
                     (str "Unknown fixture; choose from "
                          (str/join ", " (sort (map name choices)))))
        out (t/path (t/env "CELLD_EVIDENCE_DIR" "target/native-qualification"))
        evidence-path (str (fs/path out "evidence.json"))
        prior (when (and fixtures (fs/exists? evidence-path)) (t/read-json evidence-path))
        binary (t/path (t/env "CELLD_BIN" ".tools/celld"))
        version (:out (t/success! (t/run! [binary "--version"])))
        _ (t/ensure! (re-find #"(?m)^celld 0\.6\.2$" (str/trim version)) version)
        state {:local local
               :out out
               :binary binary
               :results (atom (vec (:fixtures prior)))
               :artifacts (atom (vec (:artifacts prior)))}
        origin-process (atom nil)]
    (fs/create-dirs out)
    (try
      (when-not reuse-origin
        (reset! origin-process (t/logged! ["node" "test/controlled-origin.mjs"]
                                          (str (fs/path out "controlled-origin.log"))
                                          {:extra-env
                                             {"CELLD_TLS_FIXTURE"
                                                (if (selected :transports) "1" "0")}}))
        (t/wait-for! #(= "controlled" (t/response "http://127.0.0.1:18991" "/"))
                     "Controlled origin; --reuse-origin requires an already-owned fixture"
                     5
                     @origin-process))
      (when (selected :corrections)
        (native! state
                 (build! state "test/fixtures/corrections/app.edn")
                 19019
                 "corrections"
                 #(checked! state % "correction-native-boundaries")))
      (when (selected :direct-cell)
        (native! state
                 (build! state "examples/01-direct-cell/app.edn")
                 19011
                 "direct-cell"
                 #(direct! state %)))
      (when (selected :storage)
        (native! state
                 (build! state "examples/02-rpc-sql-and-alarms/app.edn")
                 19012
                 "storage"
                 #(storage! state %)))
      (when (selected :transports)
        (native! state
                 (build! state "examples/03-websockets-and-streams/app.edn")
                 19013
                 "transports"
                 (fn [context]
                   (node! context "test/native-transport.mjs" {})
                   (doseq [record (t/read-json
                                    (t/path "target/native-transport-evidence.json"))]
                     (record! state context record)))))
      (when (selected :services)
        (native! state
                 (build! state "examples/04-native-services/app.edn")
                 19014
                 "services"
                 #(services! state %)))
      (when (selected :orchestration)
        (let [loaded (build! state
                             "examples/05-workflows-loaders-and-facets/loaded/app.edn")]
          (t/write-bytes! (t/path "target/example05-assets/loaded.mjs")
                          (t/bytes (:bundle (t/generation loaded)))))
        (native!
          state
          (build! state "examples/05-workflows-loaders-and-facets/app.edn")
          19015
          "orchestration"
          (fn [context]
            (checked! state context "compiled-loader-named-worker-facets" "/loaders")
            (checked! state
                      context
                      "workflow-instance-controls-durable-steps"
                      "/workflows")
            (node! context "test/native-facet.mjs" {})
            (record! state
                     context
                     {:fixture "accepted-and-outbound-facet-websockets" :passed true}))))
      (when (selected :interop)
        (native! state
                 (build! state "examples/07-native-interop/app.edn")
                 19017
                 "interop"
                 #(checked! state % "ordinary-native-interop")))
      (when (selected :generated-clients)
        (let [server (build! state "test/fixtures/clients/app.edn")
              generated (str (fs/path (fs/parent (:bundle (t/generation server)))
                                      "generated-src"))
              app "target/native-clients/app.edn"]
          (t/write-edn! (t/path app)
                        {:name :client-fixture
                         :entry 'fixture.client-consumer
                         :paths ["test/fixtures/clients/src" generated]
                         :output "target/native-clients/.celld-build"
                         :history "test/fixtures/clients/class-history.edn"})
          (native! state
                   (build! state app)
                   19018
                   "generated-clients"
                   #(checked! state % "compiled-generated-rpc-clients"))))
      (finally
        (try (t/stop! @origin-process)
             (finally (t/write-json! evidence-path
                                     {:target "celld-0.6.2"
                                      :complete false
                                      :fixtures @(:results state)
                                      :artifacts @(:artifacts state)
                                      :container
                                        "separate engine qualification required"})))))))

(t/main!
  *file*
  {"--local" :flag "--reuse-origin" :flag "--fixtures" :many}
  "bb test/native_qualification.clj [--local] [--reuse-origin] [--fixtures direct-cell storage transports services orchestration interop generated-clients corrections]; CELLD_EVIDENCE_DIR, CELLD_BIN, CELLD_DIRECT_CASES, CELLD_SERVICE_CASES apply."
  qualify!)
