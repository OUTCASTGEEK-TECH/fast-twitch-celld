(ns fast-twitch.celld.config
  "Whole-app Celld v0.6.2 value, identity and nested configuration rules."
  (:require [clojure.string :as str]
            [fast-twitch.celld.definition :as d]
            [fast-twitch.celld.names :as names]
            [clojure.walk :as walk]))

(defn- fail!
  [path value repair]
  (d/fail! :configuration nil :application path :celld-v062 value repair))

(defn- nonempty!
  [value path]
  (when-not (and (string? value) (seq value)) (fail! path value "Use a nonempty string."))
  value)

(defn- binding!
  [value path]
  (when-not (and (string? value)
                 (<= (count value) 128)
                 (re-matches #"[A-Za-z_$][A-Za-z0-9_$]*" value))
    (fail! path value "Use a native binding identifier of at most 128 characters."))
  value)

(defn- bounds!
  [value low high path]
  (when-not (and (integer? value) (<= low value high))
    (fail! path value (str "Use an integer from " low " to " high ".")))
  value)

(defn- enum!
  [value allowed path]
  (when-not (allowed value) (fail! path value "Select a supported native value."))
  value)

(defn- scope!
  [value path]
  (nonempty! value path)
  (when-not (re-matches #"[A-Za-z0-9_.:$-]+" value)
    (fail! path value "Use native cell-scope characters."))
  value)

(defn- resource!
  [value path]
  (when-not (and (string? value)
                 (<= 1 (count value) 64)
                 (re-matches #"[A-Za-z0-9_][A-Za-z0-9_-]*" value))
    (fail! path value "Use a stable resource name of at most 64 characters."))
  value)

(defn- closed!
  [value allowed path]
  (when-not (map? value) (fail! path value "Supply a map."))
  (doseq [key (keys value)]
    (when-not (allowed key)
      (fail! (conj path key) key "Remove the unsupported nested option.")))
  value)

(defn- vector!
  [value path]
  (when-not (vector? value) (fail! path value "Supply a vector."))
  value)

(def month-names
  (zipmap ["JAN" "FEB" "MAR" "APR" "MAY" "JUN" "JUL" "AUG" "SEP" "OCT" "NOV" "DEC"]
          (range 1 13)))

(def week-names
  (zipmap ["SUN" "MON" "TUE" "WED" "THU" "FRI" "SAT"] (range 1 8)))

(defn- digits
  [text]
  (when (and (string? text) (re-matches #"[0-9]+" text))
    (try (Long/parseLong text) (catch Exception _ nil))))

(defn- cron-value!
  [text low high names]
  (let [v (or (names (str/upper-case text)) (digits text))]
    (bounds! v low high [:triggers :crons])
    v))

(defn- cron-field!
  [text low high names]
  (let [items (str/split text #"," -1)]
    (doseq [item items]
      (when (and (> (count items) 1) (= item "*"))
        (fail! [:triggers :crons] text "Use a wildcard alone."))
      (let [[spec step & extra] (str/split item #"/" -1)
            _step (if step (bounds! (digits step) 1 (- high low) [:triggers :crons]) 1)]
        (when (seq extra)
          (fail! [:triggers :crons] text "Use one native step separator."))
        (when-not (= spec "*")
          (let [[from to & extra] (str/split spec #"-" -1)
                first (cron-value! from low high names)
                last (if to (cron-value! to low high names) first)]
            (when (or (seq extra) (> first last))
              (fail! [:triggers :crons] text "Use an ascending native range."))))))))

(defn- cron!
  [expression]
  (nonempty! expression [:triggers :crons])
  (let [fields (str/split (str/trim expression) #"\s+")
        [minute hour dom month dow] fields]
    (when-not (= 5 (count fields))
      (fail! [:triggers :crons] expression "Use five UTC cron fields."))
    (cron-field! minute 0 59 {})
    (cron-field! hour 0 23 {})
    (cron-field! month 1 12 month-names)
    (cond (#{"L" "LW"} dom) nil
          (re-matches #"L-[0-9]+W?" dom)
            (bounds! (digits (str/replace dom #"[^0-9]" "")) 1 30 [:triggers :crons])
          (re-matches #"[0-9]+W" dom)
            (bounds! (digits (subs dom 0 (dec (count dom)))) 1 31 [:triggers :crons])
          :else (cron-field! dom 1 31 {}))
    (cond (= dow "L") nil
          (str/ends-with? dow "L")
            (cron-value! (subs dow 0 (dec (count dow))) 1 7 week-names)
          (str/includes? dow "#")
            (let [[day nth & extra] (str/split dow #"#" -1)]
              (when (seq extra)
                (fail! [:triggers :crons] expression "Use one ordinal separator."))
              (cron-value! day 1 7 week-names)
              (bounds! (digits nth) 1 5 [:triggers :crons]))
          :else (cron-field! dow 1 7 week-names)))
  expression)

(defn- distinct!
  [values path]
  (when-not (= (count values) (count (distinct values)))
    (fail! path values "Use distinct native identities.")))

(defn native-values
  "Projects keyword identities/selectors once at native configuration serialization; opaque strings stay native."
  [config]
  (walk/postwalk
    (fn [value]
      (if (map? value)
        (reduce-kv
          (fn [out key item]
            (assoc out
              key
                (cond
                  (and (#{:vars :define} key) (map? item))
                    (reduce-kv (fn [fields k v]
                                 (let [k (keyword (names/identifier k))]
                                   (when (contains? fields k)
                                     (fail!
                                       [key k]
                                       item
                                       "Use distinct native binding/define identities."))
                                   (assoc fields k v)))
                               {}
                               item)
                  (and (keyword? item)
                       (#{:name :binding :class_name :entrypoint :service :database_name
                          :bucket_name :queue :dead_letter_queue}
                        key))
                    (names/identifier item)
                  (keyword? item) (names/text item)
                  (and (#{:compatibility_flags :new_sqlite_classes} key) (vector? item))
                    (mapv names/identifier item)
                  :else item)))
          {}
          value)
        value))
    config))

(defn validate!
  "Checks supported native configuration values, resource identities and declaration agreement before publication."
  [config descriptors]
  (when-let [date (:compatibility_date config)]
    (when-not (try (java.time.LocalDate/parse date) (catch Exception _ false))
      (fail! [:compatibility_date] date "Supply an ISO calendar date.")))
  (let [groups [:services :d1_databases :kv_namespaces :r2_buckets :worker_loaders
                :workflows]
        bindings (concat (map :name (get-in config [:durable_objects :bindings]))
                         (mapcat #(map :binding (% config)) groups)
                         (map :binding (get-in config [:queues :producers]))
                         (when-let [binding (get-in config [:assets :binding])] [binding])
                         (map name (keys (:vars config))))]
    (doseq [binding bindings] (binding! binding [:bindings]))
    (distinct! bindings [:bindings]))
  (doseq [binding (get-in config [:durable_objects :bindings])]
    (nonempty! (:class_name binding) [:durable_objects :class_name])
    (when-not (some #(and (= :cell (:kind %)) (= (:export %) (:class_name binding)))
                    descriptors)
      (fail! [:durable_objects :class_name] binding "Reference an exported Cell."))
    (when (and (:script_name binding) (not= (:script_name binding) (:name config)))
      (fail! [:durable_objects :script_name]
             binding
             "Cross-script Cell bindings are unavailable.")))
  (doseq [db (:d1_databases config)]
    (nonempty! (:database_name db) [:d1_databases :database_name])
    (when (:database_id db) (nonempty! (:database_id db) [:d1_databases :database_id])))
  (doseq [kv (:kv_namespaces config)] (scope! (:id kv) [:kv_namespaces :id]))
  (distinct! (map :id (:kv_namespaces config)) [:kv_namespaces :id])
  (doseq [bucket (:r2_buckets config)]
    (resource! (:bucket_name bucket) [:r2_buckets :bucket_name]))
  (distinct! (map :bucket_name (:r2_buckets config)) [:r2_buckets :bucket_name])
  (doseq [workflow (:workflows config)]
    (resource! (:name workflow) [:workflows :name])
    (when-not (some #(and (= :workflow (:kind %)) (= (:export %) (:class_name workflow)))
                    descriptors)
      (fail! [:workflows :class_name]
             workflow
             "Reference an exported Workflow declaration."))
    (when (and (:script_name workflow) (not= (:script_name workflow) (:name config)))
      (fail! [:workflows :script_name]
             workflow
             "Workflows must belong to the declaring script.")))
  (distinct! (map :name (:workflows config)) [:workflows :name])
  (doseq [service (:services config)]
    (nonempty! (:service service) [:services :service])
    (when (:entrypoint service)
      (nonempty! (:entrypoint service) [:services :entrypoint])))
  (doseq [producer (get-in config [:queues :producers])]
    (closed! producer #{:binding :queue :delivery_delay} [:queues :producers])
    (scope! (:queue producer) [:queues :producers :queue])
    (when (contains? producer :delivery_delay)
      (bounds! (:delivery_delay producer) 0 86400 [:queues :producers :delivery_delay])))
  (doseq [consumer (get-in config [:queues :consumers])]
    (closed! consumer
             #{:queue :max_batch_size :max_batch_timeout :max_retries :max_concurrency
               :retry_delay :dead_letter_queue}
             [:queues :consumers])
    (scope! (:queue consumer) [:queues :consumers :queue])
    (doseq [[key low high] [[:max_batch_size 1 100] [:max_batch_timeout 0 60]
                            [:max_retries 0 100] [:max_concurrency 1 250]
                            [:retry_delay 0 86400]]
            :when (contains? consumer key)]
      (bounds! (key consumer) low high [:queues :consumers key]))
    (when-let [dead (:dead_letter_queue consumer)]
      (scope! dead [:queues :consumers :dead_letter_queue])
      (when (= dead (:queue consumer))
        (fail! [:queues :consumers :dead_letter_queue]
               dead
               "Use a different dead-letter queue."))))
  (distinct! (map :queue (get-in config [:queues :consumers])) [:queues :consumers])
  (when (seq (get-in config [:queues :consumers]))
    (when-not (some #(some (fn [h]
                             (= :queue (:kind h)))
                           (:handlers %))
                    descriptors)
      (fail! [:queues :consumers] nil "Export a Worker queue handler.")))
  (doseq [cron (get-in config [:triggers :crons])] (cron! cron))
  (when (seq (get-in config [:triggers :crons]))
    (when-not (some #(some (fn [h]
                             (= :scheduled (:kind h)))
                           (:handlers %))
                    descriptors)
      (fail! [:triggers :crons] nil "Export a scheduled Worker handler.")))
  (doseq [container (:containers config)]
    (nonempty! (:image container) [:containers :image])
    (when-not (some #(= (:class_name container) (:class_name %))
                    (get-in config [:durable_objects :bindings]))
      (fail! [:containers :class_name] container "Reference a same-script SQLite Cell."))
    (when-not ((set (mapcat :new_sqlite_classes (:migrations config)))
                (:class_name container))
      (fail! [:containers :class_name] container "Adopt the SQLite class history first."))
    (when (contains? container :instance_type)
      (enum! (:instance_type container)
             #{"lite" "dev" "basic" "standard" "standard-1" "standard-2" "standard-3"
               "standard-4"}
             [:containers :instance_type]))
    (when (contains? container :max_instances)
      (bounds! (:max_instances container) 0 Long/MAX_VALUE [:containers :max_instances]))
    (when (contains? container :runtime)
      (nonempty! (:runtime container) [:containers :runtime])))
  (distinct! (map :class_name (:containers config)) [:containers :class_name])
  (when-let [assets (:assets config)]
    (nonempty! (:directory assets) [:assets :directory])
    (when (:html_handling assets)
      (enum! (:html_handling assets)
             #{"auto-trailing-slash" "force-trailing-slash" "drop-trailing-slash" "none"}
             [:assets :html_handling]))
    (when (:not_found_handling assets)
      (enum! (:not_found_handling assets)
             #{"none" "single-page-application" "404-page"}
             [:assets :not_found_handling]))
    (when (contains? assets :run_worker_first)
      (let [value (:run_worker_first assets)]
        (when-not (boolean? value)
          (vector! value [:assets :run_worker_first])
          (bounds! (count value) 1 100 [:assets :run_worker_first])
          (distinct! value [:assets :run_worker_first])
          (doseq [route value]
            (nonempty! route [:assets :run_worker_first])
            (when (or (> (count route) 100)
                      (re-find #"[\\\x00]" route)
                      (not (or (str/starts-with? route "/")
                               (str/starts-with? route "!/"))))
              (fail! [:assets :run_worker_first]
                     route
                     "Use a native positive/negative project route.")))
          (when-not (some #(str/starts-with? % "/") value)
            (fail! [:assets :run_worker_first] value "Include a positive rule."))))))
  (when-let [defines (:define config)]
    (when-not (map? defines)
      (fail! [:define] defines "Supply a map of native JavaScript expressions."))
    (doseq [[key value] defines]
      (let [key (name key)]
        (when (or (empty? key) (re-find #"[=\s\x00-\x1f]" key))
          (fail! [:define] key "Use a native define reference without separators."))
        (when-not (string? value)
          (fail! [:define] value "Use a JavaScript expression string.")))))
  (when-let [rules (:rules config)]
    (vector! rules [:rules])
    (let [extensions (atom {:wasm "CompiledWasm"})]
      (doseq [rule rules]
        (closed! rule #{:type :globs} [:rules])
        (enum! (:type rule) #{"Text" "Data" "CompiledWasm"} [:rules :type])
        (vector! (:globs rule) [:rules :globs])
        (when (empty? (:globs rule))
          (fail! [:rules :globs] nil "Supply at least one extension glob."))
        (doseq [glob (:globs rule)]
          (let [extension (when (string? glob)
                            (second (re-matches #"(?:\*\*/)?\*\.([^*?./\\\[{]+)" glob)))]
            (when-not extension (fail! [:rules :globs] glob "Use *.ext or **/*.ext."))
            (when-let [old (@extensions (keyword extension))]
              (when-not (= old (:type rule))
                (fail! [:rules :globs] glob "Use one native loader per extension.")))
            (swap! extensions assoc (keyword extension) (:type rule)))))))
  (distinct! (map :tag (:migrations config)) [:migrations :tag])
  (distinct! (mapcat :new_sqlite_classes (:migrations config))
             [:migrations :new_sqlite_classes])
  (doseq [migration (:migrations config)]
    (nonempty! (:tag migration) [:migrations :tag])
    (when (contains? migration :new_sqlite_classes)
      (vector! (:new_sqlite_classes migration) [:migrations :new_sqlite_classes])))
  config)
