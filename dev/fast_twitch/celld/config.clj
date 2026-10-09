(ns fast-twitch.celld.config
  "Whole-app Celld v0.6.2 value, identity and nested configuration rules."
  (:require [clojure.string :as str]
            [fast-twitch.celld.definition :as d]
            [fast-twitch.celld.contracts :as contracts]
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
  (d/check! contracts/configuration config nil :application [:config])
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
    (doseq [binding (map name (keys (:vars config)))] (binding! binding [:vars]))
    (distinct! bindings [:bindings]))
  (let [exports (set (map (juxt :kind :export) descriptors))
        handlers (set (map :kind (mapcat :handlers descriptors)))]
    (doseq [[kind path scope export-repair script-repair]
              [[:cell [:durable_objects :bindings] :durable_objects
                "Reference an exported Cell."
                "Cross-script Cell bindings are unavailable."]
               [:workflow [:workflows] :workflows
                "Reference an exported Workflow declaration."
                "Workflows must belong to the declaring script."]]
            binding (get-in config path)]
      (when-not (exports [kind (:class_name binding)])
        (fail! [scope :class_name] binding export-repair))
      (when (and (:script_name binding) (not= (:script_name binding) (:name config)))
        (fail! [scope :script_name] binding script-repair)))
    (doseq [[kind path repair]
              [[:queue [:queues :consumers] "Export a Worker queue handler."]
               [:scheduled [:triggers :crons] "Export a scheduled Worker handler."]]
            :when (and (seq (get-in config path)) (not (handlers kind)))]
      (fail! path nil repair)))
  (distinct! (map :id (:kv_namespaces config)) [:kv_namespaces :id])
  (distinct! (map :bucket_name (:r2_buckets config)) [:r2_buckets :bucket_name])
  (distinct! (map :name (:workflows config)) [:workflows :name])
  (doseq [consumer (get-in config [:queues :consumers])]
    (when-let [dead (:dead_letter_queue consumer)]
      (when (= dead (:queue consumer))
        (fail! [:queues :consumers :dead_letter_queue]
               dead
               "Use a different dead-letter queue."))))
  (distinct! (map :queue (get-in config [:queues :consumers])) [:queues :consumers])
  (doseq [cron (get-in config [:triggers :crons])] (cron! cron))
  (doseq [container (:containers config)]
    (when-not (some #(= (:class_name container) (:class_name %))
                    (get-in config [:durable_objects :bindings]))
      (fail! [:containers :class_name] container "Reference a same-script SQLite Cell."))
    (when-not ((set (mapcat :new_sqlite_classes (:migrations config)))
                (:class_name container))
      (fail! [:containers :class_name]
             container
             "Adopt the SQLite class history first.")))
  (distinct! (map :class_name (:containers config)) [:containers :class_name])
  (when-let [assets (:assets config)]
    (when (contains? assets :run_worker_first)
      (let [value (:run_worker_first assets)]
        (when-not (boolean? value)
          (distinct! value [:assets :run_worker_first])
          (doseq [route value]
            (when (or (re-find #"[\\\x00]" route)
                      (not (or (str/starts-with? route "/")
                               (str/starts-with? route "!/"))))
              (fail! [:assets :run_worker_first]
                     route
                     "Use a native positive/negative project route.")))
          (when-not (some #(str/starts-with? % "/") value)
            (fail! [:assets :run_worker_first] value "Include a positive rule."))))))
  (when-let [defines (:define config)]
    (doseq [key (keys defines)]
      (let [key (name key)]
        (when (or (empty? key) (re-find #"[=\s\x00-\x1f]" key))
          (fail! [:define] key "Use a native define reference without separators.")))))
  (when-let [rules (:rules config)]
    (let [extensions (atom {:wasm "CompiledWasm"})]
      (doseq [rule rules]
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
  config)
