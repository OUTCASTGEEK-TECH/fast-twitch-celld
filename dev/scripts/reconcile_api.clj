(ns reconcile-api
  "Reconciles source signatures and named native assertions; missing evidence never passes."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [fast-twitch.celld.tooling :as t]))

(def prefix
  "fast-twitch.celld.")

(def owners
  {:identity "C04"
   :context "C04"
   :bindings "C04"
   :http "C04"
   :worker "C13"
   :rpc "C06"
   :storage "C07"
   :storage.kv "C07"
   :sql "C07"
   :websocket "C09"
   :communication "C10"
   :standard "C10"
   :d1 "C11"
   :kv "C12"
   :r2 "C12"
   :queues "C13"
   :cron "C13"
   :workflows "C14"
   :assets "C15"
   :loaders "C15"
   :facets "C15"
   :containers "C16"
   :config "C17"})

(def container-checks
  {:signal :signal-requested
   :setInactivityTimeout :inactivity-configured
   :start :running
   :running :running
   :monitor :monitor
   :destroy :destroy
   :getTcpPort :http
   :exec :exec-stdout
   :fetch :http
   :connect :tcp
   :output :output-once
   :exitCode :exec-exit
   :pid :process-pid
   :stdin :exec-stdin-pipe
   :stdout :exec-streamed-default-options
   :stderr :exec-stderr})

(defn signatures
  [source]
  (reduce
    (fn [result [index line]]
      (if-let [[_ member args]
                 (re-find
                   #"^\s+(?:async\s+)?(?:get\s+)?([A-Za-z_$][\w$]*)\(([^)]*)\)\s*\{"
                   line)]
        (update result
                member
                (fnil conj [])
                {:signature (str member "(" args ")") :line (inc index)})
        result))
    {}
    (map-indexed vector (str/split-lines source))))

(defn wrapper-entry
  [entry api by-function mappings source-signatures]
  (let
    [id (keyword (:id entry))
     {:keys [overrides macro-paths shared native-options results]} mappings
     member (get-in entry [:native :member])
     candidates (filter #(and (= (:namespace %) (get-in entry [:wrapper :namespace]))
                              (some #{member} (:native-members %)))
                  api)
     chosen (or (by-function (str prefix (overrides id ""))) (first candidates))
     entry
       (cond
         chosen (assoc entry
                  :wrapper {:namespace (:namespace chosen)
                            :symbol (:name chosen)
                            :source (:file chosen)
                            :arities (:arglists chosen)
                            :access "ergonomic-wrapper"}
                  :implementation "implemented"
                  :contract-test {:coverage "no-member-specific-contract-case-recorded"})
         (macro-paths id) (assoc entry
                            :wrapper {:namespace (str prefix "macros")
                                      :symbol (macro-paths id)
                                      :source "src/fast_twitch/celld/macros.clj"
                                      :access "native-declaration"}
                            :implementation "implemented")
         (shared id)
           (let [[namespace symbol] (shared id)]
             (assoc entry
               :wrapper {:namespace namespace :symbol symbol :access "shared-fast-twitch"}
               :implementation "shared-adapter-implemented"))
         :else (-> entry
                   (assoc :implementation "coverage-gap")
                   (assoc-in [:wrapper :access] "unresolved-member-mapping")))
     native
       (cond->
         (assoc (:native entry)
           :overloads
             {:candidate-source-signatures (source-signatures member [])
              :candidate-policy
                "Lexical same-name matches may belong to another receiver; not certified overloads."
              :wrapper-arities (get-in entry [:wrapper :arities] [])
              :coverage
                "source-signatures-and-wrapper-arities; named evidence below defines observed cases"}
           :optional-arguments
             "Shortest wrapper arity omits native options; explicit options are checked and projected. See source for codec-only arities."
           :default-policy "Native defaults; wrapper does not retry or invent settlement."
           :result
             (get results id (if chosen (:doc chosen) (get-in entry [:native :result]))))
         (contains? native-options id) (assoc :options (native-options id))
         (= id :facets/abort) (assoc :execution "sync")
         (= id :containers/setInactivityTimeout) (assoc :execution "promise"))]
    (assoc entry
      :native native
      :qualification "unqualified"
      :validation
        {:runtime "Source-linked member guards; dynamic native rejection retained."
         :build
           "Selected declarations/config are mandatory validated; member option parity remains separately qualified."})))

(defn member-evidence
  [mappings evidence container]
  (let
    [fixtures (t/indexed
                (map #(assoc %
                        :assertions
                          (try (json/parse-string (:result %) true)
                               (catch Exception _ nil)))
                  (:fixtures evidence))
                :fixture)
     artifacts (t/indexed (:artifacts evidence) :fingerprint)
     native
       (for [kind [:checks :invocation-checks]
             :let [invocation? (= kind :invocation-checks)]]
         (into
           {}
           (for [[id [fixture assertion]] (kind mappings)
                 :let [record (fixtures fixture)
                       artifact (artifacts (:artifact-fingerprint record))]
                 :when (and (:passed record)
                            artifact
                            (or invocation?
                                (if assertion
                                  (true? (get (:assertions record) (keyword assertion)))
                                  (and (contains? (:scalar-checks mappings) id)
                                       (= (:result record)
                                          (second (get-in mappings
                                                          [:scalar-checks id])))))))]
             [id
              (cond->
                {:qualification (if (and invocation? (not= id :communication/SSE))
                                  "native-invocation-passed"
                                  "native-case-passed")
                 :final-bundle-evidence
                   (merge
                     {:record "docs/qualification-evidence.json"
                      :fixture fixture
                      :assertion assertion
                      :artifact-fingerprint (:artifact-fingerprint record)
                      :coverage
                        (if invocation?
                          "The named assertion or invocation only; no claim of all settlement or cron semantics."
                          "this case only; remaining options/overloads are not certified")}
                     (select-keys artifact
                                  (cond-> [:manifest :bundle-sha256 :manifest-sha256]
                                    (not invocation?) (conj :input-fingerprint))))}
                (not invocation?)
                  (assoc :contract-test
                    {:source "test/native_qualification.clj"
                     :fixture fixture
                     :assertion assertion}))])))
     container-bundle
       {:record "docs/qualification-evidence.json"
        :fixture "container"
        :artifact-fingerprint (:artifact-fingerprint container)
        :manifest (str "examples/06-containers/.celld-build/generations/"
                       (:artifact-fingerprint container)
                       "/manifest.edn")
        :scope (:scope
                 container
                 "Historical experimental VM case only; native kill limitation recorded.")
        :manifest-sha256 (get-in container [:artifact-integrity :manifest-sha256])
        :bundle-sha256 (get-in container [:artifact-integrity :bundle-sha256])}
     container-cases
       (into {}
             (for [[member assertion] container-checks
                   :when (and (:artifact-fingerprint container)
                              (true? (get-in container [:checks assertion])))]
               [(keyword "containers" (name member))
                {:qualification (if (:adapter-qualified container)
                                  "native-case-passed"
                                  "historical-native-case-passed")
                 :final-bundle-evidence (assoc container-bundle
                                          :assertion (name assertion))
                 :contract-test {:source "test/native_container_qualification.clj"
                                 :assertion (name assertion)}}]))
     limited
       (when (and (:adapter-qualified container) (seq (:native-limits container)))
         {:containers/kill
            {:qualification "native-limited"
             :final-bundle-evidence
               (merge
                 (select-keys container-bundle [:record :fixture :artifact-fingerprint])
                 (select-keys container [:native-limits :diagnostics])
                 {:scope
                    "Wrapper invokes native kill synchronously; bounded process termination remains limited by the pinned native/engine path."})}})]
    (apply merge-with merge (concat native [container-cases limited]))))

(defn reconcile-entry
  [entry api by-function mappings source-signatures evidence]
  (let
    [id (keyword (:id entry))
     family (keyword (namespace id))
     entry (cond-> (assoc entry
                     :final-bundle-evidence {:status "no-final-native-case-recorded"}
                     :documentation-path "docs/api-reference.md")
             (owners family) (assoc :owner (owners family)))
     entry
       (case family
         :excluded (assoc entry :qualification "excluded-with-pinned-rationale")
         :standard
           (assoc entry
             :implementation "native-interop-documented"
             :wrapper {:access "native-interop"
                       :syntax (if (re-find #"^(node|cloudflare):" (name id))
                                 ":require-global"
                                 ":refer-global")}
             :qualification "native-interop-documented"
             :final-bundle-evidence
               {:scope
                  "Documented pinned native interop; no exhaustive export audit is required or claimed."})
         :config
           (assoc entry
             :implementation "mandatory-build-validation"
             :wrapper {:namespace (str prefix "build")
                       :symbol "build!"
                       :source "dev/fast_twitch/celld/build.clj"}
             :qualification "shared-validation-implemented"
             :final-bundle-evidence
               {:scope
                  "Shared mandatory schema implementation; this entry has no per-entry case claim."
                :source "dev/fast_twitch/celld/config.clj"})
         (wrapper-entry entry api by-function mappings source-signatures))]
    (merge entry (evidence id))))

(defn additional-entries
  [entries modules]
  (let [ids (set (map :id entries))]
    (into
      entries
      (concat
        (for [module modules
              :when (not (ids (str "standard/" module)))]
          {:id (str "standard/" module)
           :status "partial-native-module"
           :implementation "native-interop-documented"
           :wrapper {:access "native-interop" :syntax ":require-global"}
           :qualification "native-interop-documented"
           :documentation-path "docs/native-semantics.md"
           :owner "C10"
           :source
             "crates/celld/js/modules.rs at 90b43017241f81189453d326d05948f388b34652"
           :limits
             "Native partial-module support; module inclusion does not certify every export."
           :final-bundle-evidence {:status "no-final-native-case-recorded"}})
        (when-not (ids "excluded/workflows.sensitive")
          [{:id "excluded/workflows.sensitive"
            :status "unsupported"
            :implementation "excluded"
            :qualification "excluded-with-pinned-rationale"
            :reason
              "The native config validator recognizes output, but doStep explicitly rejects execution as unimplemented."
            :source "crates/celld/js/harness.js:8467"
            :documentation-path "docs/native-semantics.md"}])))))

(defn api-reference
  [api]
  (str/join
    "\n"
    (concat
      ["# Public ClojureScript API" ""
       "Target: Celld v0.6.2. Source signatures describe implemented wrappers. [Native semantics](native-semantics.md), the member catalog and [qualification](qualification.md) define the limits of each verified case."
       ""]
      (mapcat (fn [functions]
                (concat [(str "## " (:namespace (first functions))) ""]
                        (mapcat
                          (fn [{:keys [name arglists doc]}]
                            (concat
                              [(str "### `" name "`") "" "```clojure"]
                              (for [args arglists
                                    :let [args (subs args 1 (dec (count args)))]]
                                (str "(" name (when (seq args) (str " " args)) ")"))
                              ["```" ""
                               (or
                                 (not-empty doc)
                                 "Public signature; refer to the linked source contract.")
                               ""]))
                          functions)))
        (partition-by :namespace api))
      ["## Declaration macros" ""
       "`defcontract` records a portable literal schema. `defcell-init`, `deffetch`, `defrpc`, `defalarm`, `defqueue-handler`, `defscheduled-handler` and `defwebsocket-handlers` define focused declarations selected by an owner’s `:include`. `defcell`, `defworker` and `defworkflow` emit direct exports. Require `with-transaction-sync` from `fast-twitch.celld.storage` with `:refer-macros`; it checks thenables inside the native callback."
       ""
       "Initialization takes `[ctx]`; fetch `[ctx request]`; RPC `[ctx & arguments]`; alarm `[ctx alarm-info]`; queue/scheduled `[ctx native-event]`; Workflow `[ctx event step]`. Literal keys, scope, names, codec/schema references, arity and export/binding/event identity are mandatory checked before publication."
       ""])))

(defn reconcile!
  [{:keys [output-dir]}]
  (let
    [api (t/read-json (t/path "target/public-api.json"))
     catalog (t/read-json (t/path "target/catalog.json"))
     mappings (t/read-edn (t/path "dev/scripts/api_evidence.edn"))
     evidence-path (t/path (t/env "CELLD_EVIDENCE_FILE"
                                  "target/native-qualification/evidence.json"))
     evidence (when (t/text evidence-path) (t/read-json evidence-path))
     source-signatures (signatures (if-let [harness (System/getenv
                                                      "CELLD_SOURCE_HARNESS")]
                                     (slurp harness)
                                     ""))
     by-function (t/indexed api #(str (:namespace %) "/" (:name %)))
     container (when (t/text (t/path "target/native-container/evidence.json"))
                 (t/read-json (t/path "target/native-container/evidence.json")))
     member-evidence (member-evidence mappings evidence container)
     entries
       (mapv
         #(reconcile-entry % api by-function mappings source-signatures member-evidence)
         (:entries catalog))
     modules (map second
               (re-seq #"\"((?:node|cloudflare):[^\" ]+)\""
                       (first (str/split (slurp (t/path
                                                  "dev/fast_twitch/celld/build.clj"))
                                         #"\(defn resolve-path"
                                         2))))
     entries (additional-entries entries modules)
     qualified (set (map :id
                      (filter #(#{"native-case-passed" "native-limited"}
                                 (:qualification %))
                        entries)))
     scoped ["context/abort" "queues/metrics" "communication/startTls" "assets/fetch"
             "containers/signal" "containers/setInactivityTimeout" "containers/kill"]
     measurement (when (t/text (t/path "target/native-resource-evidence.json"))
                   (t/read-json (t/path "target/native-resource-evidence.json")))
     remaining
       (cond-> (filterv (complement qualified) scoped)
         (not (and (:passed measurement)
                   (= "Node performance.now (external monotonic)"
                      (get-in measurement [:adapter-validation-measurement :clock]))
                   (seq (get-in measurement [:adapter-validation-measurement :samples]))))
           (conj "bounded-validation-and-allocation-measurement"))
     limits (mapv :id (filter #(= "native-limited" (:qualification %)) entries))
     catalog
       (assoc catalog
         :entries entries
         :qualification (if (seq remaining) "in-progress" "scoped-follow-up-complete")
         :scoped-complete (empty? remaining)
         :native-limits limits
         :release-ready (and (empty? remaining) (empty? limits))
         :remaining-scoped-paths remaining
         :source-inventory-count (count api)
         :coverage-policy
           "A native-case pass applies only to its named assertion; no family-wide or full overload qualification is inferred.")
     inventory
       (->
         catalog
         (assoc :qualification "source-inventory"
                :entries (mapv #(-> %
                                    (dissoc :contract-test)
                                    (assoc :qualification
                                             (if (str/starts-with? (:id %) "excluded/")
                                               "excluded-with-pinned-rationale"
                                               "source-inventory")
                                           :final-bundle-evidence
                                             {:record "docs/api-catalog.edn"
                                              :member-id (:id %)}))
                           entries))
         (dissoc :remaining-scoped-paths :release-ready :scoped-complete :native-limits))
     coverage
       (merge (select-keys catalog
                           [:target :release-ready :scoped-complete :native-limits
                            :remaining-scoped-paths])
              {:source-functions (count api)
               :counts (into (sorted-map) (frequencies (map :qualification entries)))
               :entries (mapv #(assoc (select-keys % [:id :implementation :qualification])
                                 :evidence (:final-bundle-evidence %))
                          entries)})
     output #(t/path (or output-dir "") %)]
    (t/write-edn! (output "docs/api-catalog.edn") catalog)
    (t/write-json! (output "docs/coverage.json") coverage)
    (t/write! (output "docs/api-reference.md") (api-reference api))
    (when-not (not-empty (System/getenv "CELLD_RECONCILE_DOCS_ONLY"))
      (t/write-edn! (output "resources/fast_twitch/celld/api-catalog.edn") inventory)
      (t/write-json! (output "resources/fast_twitch/celld/coverage.json")
                     {:source-inventory "resources/fast_twitch/celld/api-catalog.edn"
                      :qualified-catalog "docs/api-catalog.edn"
                      :case-report "docs/coverage.json"}))
    (println (json/generate-string
               {:members (count entries)
                :functions (count api)
                :coverage (:counts coverage)
                :unmapped (mapv :id
                            (filter #(= "coverage-gap" (:implementation %)) entries))}))))

(t/main!
  *file*
  {"--output-dir" :value}
  "bb dev/scripts/reconcile_api.clj [--output-dir DIR]; CELLD_EVIDENCE_FILE, CELLD_SOURCE_HARNESS, CELLD_RECONCILE_DOCS_ONLY apply."
  reconcile!)
