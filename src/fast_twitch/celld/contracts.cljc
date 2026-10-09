(ns fast-twitch.celld.contracts
  "Immutable portable option/value contracts; shared transport schemas stay in Fast-Twitch."
  (:require [malli.core :as m]
            [fast-twitch.util.contracts :as shared]
            [fast-twitch.celld.names :as names]
            #?(:clj [clojure.edn :as edn])
            #?(:clj [clojure.java.io :as io])))

#?(:clj (defmacro payload-limit
          "Embeds the pinned target profile's UTF-8 JSON payload budget."
          []
          (get-in (edn/read-string (slurp (io/resource
                                            "fast_twitch/celld/runtime-profiles.edn")))
                  [:limits :payload-bytes])))

(def positive-int
  [:int {:min 1}])

(def safe-int
  [:int {:min -9007199254740991 :max 9007199254740991}])

(def identity-value
  :keyword)

(def schema-value
  :any)

(def reference
  [:fn symbol?])

(def extensions
  [:map-of [:fn #(and (keyword? %) (namespace %))] :any])

(defn closed
  "Constructs a closed portable map of optional native option fields."
  [entries]
  (into [:map {:closed true}]
        (map (fn [[key schema]] [key {:optional true} schema])
          entries)))

(def schemas
  {:get-stub (closed {:locationHint [:enum "wnam" "enam" "sam" "weur" "eeur" "apac" "oc"
                                     "afr" "me"]})
   :get-by-name (closed {:locationHint [:enum "wnam" "enam" "sam" "weur" "eeur" "apac"
                                        "oc" "afr" "me"]})
   :rpc-options (closed {:args-schema schema-value
                         :returns schema-value
                         :codec [:maybe [:enum :native :json :rpc]]})
   :storage-list (closed {:start :keyword
                          :startAfter :keyword
                          :end :keyword
                          :prefix :keyword
                          :reverse :boolean
                          :limit positive-int})
   :kv-list (closed {:start :keyword
                     :startAfter :keyword
                     :end :keyword
                     :prefix :keyword
                     :reverse :boolean
                     :limit positive-int})
   :storage-get (closed {:codec [:enum :native :json]})
   :storage-put (closed {:codec [:enum :native :json]})
   :storage-put-many (closed {:codec [:enum :native :json]})
   :d1-raw (closed {:columnNames :boolean})
   :service-kv-read (closed {:type [:enum "text" "json" "arrayBuffer" "stream"]})
   :service-kv-put
     (closed {:expiration [:int {:min 0}] :expirationTtl [:int {:min 60}] :metadata :any})
   :service-kv-list (closed
                      {:prefix :keyword :cursor :string :limit [:int {:min 1 :max 1000}]})
   :queue-send (closed {:contentType [:enum "v8" "json" "text" "bytes"]
                        :delaySeconds [:int {:min 0 :max 86400}]})
   :queue-message (closed {:body :any
                           :contentType [:enum "v8" "json" "text" "bytes"]
                           :delaySeconds [:int {:min 0 :max 86400}]})
   :queue-batch (closed {:delaySeconds [:int {:min 0 :max 86400}]})
   :queue-retry (closed {:delaySeconds [:int {:min 0 :max 86400}]})
   :tcp-connect (closed {:secureTransport [:enum "off" "on" "starttls"]
                         :allowHalfOpen :boolean})
   :tcp-start-tls (closed {:expectedServerHostname :string})
   :tcp-adapt (closed {:codec [:enum :native :bytes :text]
                       :streaming shared/TCPStreaming
                       :signal shared/Signal})
   :loader-limits (closed {:cpuMs [:int {:min 1}] :subRequests [:int {:min 0}]})
   :workflow-retention (closed {:successRetention [:or number? :string]
                                :errorRetention [:or number? :string]})
   :workflow-create (closed {:id [:string {:min 1 :max 100}]
                             :params :any
                             :retention [:map-of :keyword [:or number? :string]]
                             :locationHint [:enum "wnam" "enam" "sam" "weur" "eeur" "apac"
                                            "apac-ne" "apac-se" "oc" "afr" "me"]})
   :workflow-retries (closed {:limit [:and number? [:>= 0] [:<= 10000]]
                              :delay [:or number? :string [:fn fn?]]
                              :backoff [:enum "constant" "linear" "exponential"]})
   :workflow-step (closed {:retries :any :timeout [:or number? :string]})
   :workflow-wait (closed {:type [:string {:min 1 :max 100}]
                           :timeout [:or number? :string]})
   :workflow-restart-from (closed {:name [:string {:max 256}]
                                   :count [:int {:min 1}]
                                   :type [:enum "do" "sleep" "waitForEvent"]})
   :container-start (closed {:entrypoint [:vector :string]
                             :env [:map-of :keyword :string]
                             :enableInternet :boolean
                             :labels [:map-of :keyword :string]})
   :container-exec (closed {:env [:map-of :keyword :string]
                            :cwd :string
                            :user :string
                            :stdin :any
                            :stdout [:enum "pipe" "ignore"]
                            :stderr [:enum "pipe" "ignore" "combined"]})})

(def validators
  (into {}
        (map (fn [[key schema]] [key (m/validator schema)])
          schemas)))

(defn option-values
  "Projects keyword selectors only for native string enums; codec/data values retain their domain."
  [operation value]
  (let [fields (into {}
                     (map (fn [[k _ schema]] [k schema])
                       (drop 2 (get schemas operation))))]
    (reduce-kv (fn [out key item]
                 (let [schema (get fields key)]
                   (assoc out
                     key (if (and (keyword? item)
                                  (sequential? schema)
                                  (= :enum (first schema))
                                  (every? string? (rest schema)))
                           (names/text item)
                           item))))
               {}
               value)))

(defn options-valid?
  "Checks a cached operation contract; key checks remain required for uncataloged operations."
  [operation value]
  (if-let [validator (get validators operation)]
    (validator value)
    true))

(defn safe-path
  "Retains ordinary option locations while redacting oversized or opaque path segments."
  [path]
  (mapv #(if (or (and (keyword? %) (<= (count (str %)) 128))
                 (and (integer? %) (<= -9007199254740991 % 9007199254740991)))
           %
           :redacted-segment)
    (take 8 path)))

(defn issue
  "Returns the first bounded schema location/shape, never the rejected payload."
  [schema value]
  (when-let [error (first
                     (sort-by #(count (:in %)) > (:errors (m/explain schema value))))]
    {:type (:type error)
     :path (safe-path (:in error))
     :schema-path (safe-path (:path error))
     :expected (let [t (m/type (:schema error))] (if (keyword? t) t :predicate))}))

(def declarations
  (let [base {:extensions extensions}
        fetch (closed (assoc base :native? :boolean))
        rpc-fields (merge base
                          {:args schema-value
                           :returns schema-value
                           :codec [:enum :native :json :rpc]})
        rpc (closed (assoc rpc-fields :method identity-value))
        event (fn [schema] [:or reference (into schema [[:handler reference]])])
        socket (closed (merge base
                              {:id identity-value
                               :version positive-int
                               :mode [:enum :resident :hibernating]
                               :message reference
                               :close reference
                               :error reference}))
        owner (merge base
                     {:export identity-value
                      :include [:vector reference]
                      :fetch (event fetch)
                      :rpc [:map-of identity-value (event (closed rpc-fields))]})]
    {:init (closed base)
     :fetch fetch
     :rpc rpc
     :alarm (closed base)
     :websocket socket
     :queue (closed base)
     :scheduled (closed (assoc base :crons [:vector :string]))
     :cell (closed (merge owner
                          {:binding identity-value
                           :facet? :boolean
                           :init (event (closed base))
                           :alarm (event (closed base))
                           :websocket [:or reference socket]}))
     :worker (closed (merge owner
                            {:queue (event (closed base))
                             :scheduled (event (closed (assoc base
                                                         :crons [:vector :string])))}))
     :workflow (closed (merge base
                              {:export identity-value
                               :binding identity-value
                               :name identity-value
                               :args schema-value
                               :returns schema-value
                               :codec [:enum :native :json]}))}))

(def configuration
  "Static native configuration shapes. Cross-resource/history agreement is checked by build."
  (let [entry (fn [fields] [:vector (closed fields)])
        strings [:vector :string]
        binding {:binding :string}
        producer (closed (merge binding
                                {:queue :string
                                 :delivery_delay [:int {:min 0 :max 86400}]}))
        consumer (closed {:queue :string
                          :max_batch_size [:int {:min 1 :max 100}]
                          :max_batch_timeout [:int {:min 0 :max 60}]
                          :max_retries [:int {:min 0 :max 100}]
                          :max_concurrency [:int {:min 1 :max 250}]
                          :retry_delay [:int {:min 0 :max 86400}]
                          :dead_letter_queue :string})]
    (closed
      {:$schema :string
       :name :string
       :main :string
       :no_bundle :boolean
       :compatibility_date :string
       :compatibility_flags strings
       :durable_objects (closed {:bindings (entry {:name :string
                                                   :class_name :string
                                                   :script_name :string})})
       :migrations (entry {:tag :string :new_sqlite_classes strings})
       :assets (closed {:directory :string
                        :binding :string
                        :html_handling [:enum "auto-trailing-slash" "force-trailing-slash"
                                        "drop-trailing-slash" "none"]
                        :not_found_handling [:enum "none" "single-page-application"
                                             "404-page"]
                        :run_worker_first [:or :boolean strings]})
       :services (entry (merge binding {:service :string :entrypoint :string}))
       :triggers (closed {:crons strings})
       :vars [:map-of :keyword :any]
       :d1_databases (entry (merge binding {:database_name :string :database_id :string}))
       :kv_namespaces (entry (merge binding {:id :string}))
       :r2_buckets (entry (merge binding {:bucket_name :string}))
       :worker_loaders (entry binding)
       :workflows (entry (merge binding
                                {:name :string :class_name :string :script_name :string}))
       :queues (closed {:producers [:vector producer] :consumers [:vector consumer]})
       :containers (entry {:class_name :string
                           :image :string
                           :name :string
                           :instance_type [:enum "lite" "dev" "basic" "standard"
                                           "standard-1" "standard-2" "standard-3"
                                           "standard-4"]
                           :max_instances [:int {:min 0}]
                           :runtime :string})
       :define [:map-of :keyword :string]
       :rules [:vector
               (closed {:type [:enum "Text" "Data" "CompiledWasm"]
                        :globs [:vector {:min 1} :string]})]})))
