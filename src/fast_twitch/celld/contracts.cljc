(ns fast-twitch.celld.contracts
  "Immutable portable option/value contracts; shared transport schemas stay in Fast-Twitch."
  (:require [malli.core :as m]
            [fast-twitch.util.contracts :as shared]
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

(def identity-value
  :keyword)

(def schema-value
  :any)

(def reference
  [:fn symbol?])

(def extensions
  [:map-of [:fn #(and (keyword? %) (namespace %))] :any])

(defn closed
  "Constructs a closed portable map; native options are optional unless required explicitly."
  ([entries]
   (closed entries #{}))
  ([entries required]
   (into [:map {:closed true}]
         (map (fn [[key schema]] [key {:optional (not (contains? required key))} schema])
           entries))))

(def schemas
  (let [location (closed {:locationHint [:enum :wnam :enam :sam :weur :eeur :apac :oc :afr
                                         :me]})
        listing (closed {:start :keyword
                         :startAfter :keyword
                         :end :keyword
                         :prefix :keyword
                         :reverse :boolean
                         :limit positive-int})
        storage-codec (closed {:codec [:enum :native :json]})
        delay {:delaySeconds [:int {:min 0 :max 86400}]}
        queue-body (assoc delay
                     :contentType [:enum :v8 :json :text :bytes]
                     :codec [:enum :native :json])]
    {:tcp-start-tls (closed {:expectedServerHostname :string})
     :storage-put-many storage-codec
     :workflow-wait (closed {:type :keyword
                             :timeout [:or number? :string]
                             :codec [:enum :native :json]}
                            #{:type})
     :workflow-step (closed {:retries (closed {:limit [:and number? [:>= 0] [:<= 10000]]
                                               :delay [:or number? :string [:fn fn?]]
                                               :backoff [:enum :constant :linear
                                                         :exponential]}
                                              #{:limit :delay})
                             :timeout [:or number? :string]
                             :codec [:enum :native :json]})
     :queue-send (closed queue-body)
     :tcp-adapt (closed {:codec [:enum :native :bytes :text]
                         :streaming shared/TCPStreaming
                         :signal shared/Signal})
     :kv-list listing
     :get-stub location
     :storage-put storage-codec
     :service-kv-list
       (closed {:prefix :keyword :cursor :string :limit [:int {:min 1 :max 1000}]})
     :queue-message (closed (assoc queue-body :body :any) #{:body})
     :rpc-options (closed {:args-schema schema-value
                           :returns schema-value
                           :codec [:maybe [:enum :native :json :rpc]]})
     :service-kv-put (closed {:expiration [:int {:min 0}]
                              :expirationTtl [:int {:min 60}]
                              :metadata :any})
     :storage-get storage-codec
     :loader-limits (closed {:cpuMs [:int {:min 1}] :subRequests [:int {:min 0}]})
     :get-by-name location
     :storage-list listing
     :container-exec (closed {:env [:map-of :keyword :string]
                              :cwd :string
                              :user :string
                              :stdin :any
                              :stdout [:enum :pipe :ignore]
                              :stderr [:enum :pipe :ignore :combined]})
     :tcp-connect (closed {:secureTransport [:enum :off :on :starttls]
                           :allowHalfOpen :boolean})
     :d1-raw (closed {:columnNames :boolean})
     :workflow-create (closed {:id [:string {:min 1 :max 100}]
                               :params :any
                               :codec [:enum :native :json]
                               :retention (closed {:successRetention [:or number? :string]
                                                   :errorRetention [:or number? :string]})
                               :locationHint [:enum :wnam :enam :sam :weur :eeur :apac
                                              :apac-ne :apac-se :oc :afr :me]})
     :container-start (closed {:entrypoint [:vector :string]
                               :env [:map-of :keyword :string]
                               :enableInternet :boolean
                               :labels [:map-of :keyword :string]})
     :queue-batch (closed delay)
     :queue-retry (closed delay)
     :service-kv-read (closed {:type [:enum :text :json :arrayBuffer :stream]})}))

(defonce ^:private validators
  (atom {}))

(defn ^:no-doc validator
  "Compiles each immutable contract once; dynamic schema retention stays bounded."
  [schema]
  (or (get @validators schema)
      (let [check (m/validator schema)]
        (swap! validators #(assoc (if (< (count %) 128) % {}) schema check))
        check)))

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

#?(:clj
     (do
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
                                      :error reference})
                              #{:id :version :mode})
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
                                                                :crons [:vector
                                                                        :string])))}))
            :workflow (closed (merge base
                                     {:export identity-value
                                      :binding identity-value
                                      :name identity-value
                                      :args schema-value
                                      :returns schema-value
                                      :codec [:enum :native :json]}))}))

       (def configuration
         "Static native configuration shapes. Cross-resource/history agreement is checked by build."
         (let [entry (fn [fields required] [:vector (closed fields required)])
               nonempty [:string {:min 1}]
               binding-name [:and [:string {:min 1 :max 128}]
                             [:re #"^[A-Za-z_$][A-Za-z0-9_$]*(?![\s\S])"]]
               scope [:re #"^[A-Za-z0-9_.:$-]+(?![\s\S])"]
               resource [:and [:string {:min 1 :max 64}]
                         [:re #"^[A-Za-z0-9_][A-Za-z0-9_-]*(?![\s\S])"]]
               strings [:vector :string]
               binding {:binding binding-name}
               producer (closed (merge binding
                                       {:queue scope
                                        :delivery_delay [:int {:min 0 :max 86400}]})
                                #{:binding :queue})
               consumer (closed {:queue scope
                                 :max_batch_size [:int {:min 1 :max 100}]
                                 :max_batch_timeout [:int {:min 0 :max 60}]
                                 :max_retries [:int {:min 0 :max 100}]
                                 :max_concurrency [:int {:min 1 :max 250}]
                                 :retry_delay [:int {:min 0 :max 86400}]
                                 :dead_letter_queue scope}
                                #{:queue})]
           (closed
             {:$schema :string
              :name :string
              :main :string
              :no_bundle :boolean
              :compatibility_date :string
              :compatibility_flags strings
              :durable_objects (closed {:bindings (entry {:name binding-name
                                                          :class_name nonempty
                                                          :script_name :string}
                                                         #{:name :class_name})})
              :migrations (entry {:tag nonempty :new_sqlite_classes strings} #{:tag})
              :assets (closed {:directory nonempty
                               :binding binding-name
                               :html_handling [:enum "auto-trailing-slash"
                                               "force-trailing-slash"
                                               "drop-trailing-slash" "none"]
                               :not_found_handling [:enum "none" "single-page-application"
                                                    "404-page"]
                               :run_worker_first [:or :boolean
                                                  [:vector {:min 1 :max 100}
                                                   [:string {:min 1 :max 100}]]]}
                              #{:directory})
              :services (entry (merge binding {:service nonempty :entrypoint nonempty})
                               #{:binding :service})
              :triggers (closed {:crons strings})
              :vars [:map-of :keyword :any]
              :d1_databases (entry (merge binding
                                          {:database_name nonempty :database_id nonempty})
                                   #{:binding :database_name})
              :kv_namespaces (entry (merge binding {:id scope}) #{:binding :id})
              :r2_buckets (entry (merge binding {:bucket_name resource})
                                 #{:binding :bucket_name})
              :worker_loaders (entry binding #{:binding})
              :workflows (entry
                           (merge
                             binding
                             {:name resource :class_name nonempty :script_name :string})
                           #{:binding :name :class_name})
              :queues (closed {:producers [:vector producer]
                               :consumers [:vector consumer]})
              :containers (entry {:class_name :string
                                  :image nonempty
                                  :name :string
                                  :instance_type [:enum "lite" "dev" "basic" "standard"
                                                  "standard-1" "standard-2" "standard-3"
                                                  "standard-4"]
                                  :max_instances [:int {:min 0 :max 9223372036854775807}]
                                  :runtime nonempty}
                                 #{:class_name :image})
              :define [:map-of :keyword :string]
              :rules [:vector
                      [:map {:closed true} [:type [:enum "Text" "Data" "CompiledWasm"]]
                       [:globs [:vector {:min 1} :string]]]]})))))
