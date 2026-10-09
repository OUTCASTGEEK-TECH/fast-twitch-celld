(ns fast-twitch.celld.sql
  "Synchronous Cell SQL, explicit bindings and fully drained write cursors.
  HoneySQL formats SQL only; it does not execute or own application migrations."
  (:require [honey.sql :as honey]
            [clojure.walk :as walk]
            [fast-twitch.celld.names :as names]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v])
  (:refer-global :only [ArrayBuffer]))

(defn native
  "Returns storage.sql, the live synchronous SQL receiver."
  [storage]
  (n/property storage "sql"))

(defn ^:no-doc binding!
  "Checks one SQL positional value without converting native byte identity or rounding numbers."
  [value]
  (when-not (or (nil? value)
                (string? value)
                (number? value)
                (instance? ArrayBuffer value)
                (ArrayBuffer.isView value))
    (v/fail! :sql-binding
             :data
             "Supply null, a string, finite safe number, or native bytes."))
  (v/safe-number! value :sql-binding))

(defn- named-data!
  "Authored bound values live in the parameter map. HoneySQL owns collection
  expansion; inline constants and DDL type/default data retain HoneySQL semantics."
  [form ddl?]
  (cond
    (map? form) (doseq [[clause value] form]
                  (named-data! value (or ddl? (= :with-columns clause))))
    (sequential? form) (when-not (#{:param :inline :default} (first form))
                         (doseq [value form] (named-data! value ddl?)))
    (or (string? form) (and (number? form) (not ddl?)))
      (v/fail!
        :sql-query
        :parameters
        "Use keyword named parameters for values, and HoneySQL :inline for SQL constants.")))

(defn- result-identities
  "Projects result aliases, not qualified table references, to reversible native names."
  [query]
  (walk/postwalk
    (fn [form]
      (if (map? form)
        (reduce (fn [out clause]
                  (if-let [columns (get out clause)]
                    (let [aliases (atom #{})
                          columns
                            (mapv
                              (fn [column]
                                (if (= :* column)
                                  column
                                  (let [[expression alias]
                                          (if (keyword? column) [column column] column)]
                                    (v/check! :keyword alias :sql-result-alias)
                                    (when (@aliases alias)
                                      (v/fail! :sql-query
                                               :collision
                                               "Use distinct keyword result aliases."))
                                    (swap! aliases conj alias)
                                    [expression (keyword (names/identifier alias))])))
                              columns)]
                      (assoc out clause columns))
                    out))
          form
          [:select :select-distinct :returning])
        form))
    query))

(defn ^:no-doc row-map
  "Internal native row projection; reversible aliases retain keyword namespaces."
  [row]
  (n/data-map row names/selector))

(defn ^:no-doc formatted
  "Internal HoneySQL boundary. Public queries are keyword clause maps; raw escapes
  are rejected. Only HoneySQL emits the native SQL text; params are application data."
  ([query]
   (formatted query {}))
  ([query params]
   (v/check! [:map-of :keyword :any] query :sql-query)
   (v/check! [:map-of :keyword :any] params :sql-params)
   (doseq [node (tree-seq coll? seq query)
           :when (and (sequential? node) (= :param (first node)))]
     (v/check! [:tuple [:= :param] :keyword] node :sql-parameter))
   (when (or (empty? query)
             (some #{:raw 'raw} (tree-seq coll? seq query)))
     (v/fail! :sql-query
              :authoring
              "Use HoneySQL clauses and parameters without raw SQL escapes."))
   (named-data! query false)
   (try
     (let [query (result-identities query)
           formatted (honey/format query {:params params :dialect :ansi :quoted true})]
       formatted)
     (catch :default error
       (if (:code (ex-data error))
         (throw error)
         (throw (ex-info
                  "Supply a supported HoneySQL clause map and named parameters."
                  {:code :fast-twitch.celld/invalid-boundary
                   :operation :sql-query
                   :boundary :format
                   :path []
                   :expected :honeysql
                   :repair
                     "Use supported HoneySQL data and supply every named parameter."}
                  error)))))))

(defn exec
  "Executes HoneySQL query/DDL data synchronously, returning the actual native cursor.
  Optional keyword params resolve HoneySQL named parameters before any native effect."
  ([sql query]
   (exec sql query {}))
  ([sql query params]
   (let [[text & values] (formatted query params)
         values (mapv binding! values)]
     (n/invoke sql "exec" (into [text] values)))))

(defn cursor-native
  "Returns the actual live native cursor. It has no public close operation."
  [cursor]
  cursor)

(defn metadata
  "Projects ordered column names and current row counters; no rows are consumed."
  [cursor]
  {:column-names (mapv names/selector (array-seq (n/property cursor "columnNames")))
   :rows-read (n/property cursor "rowsRead")
   :rows-written (n/property cursor "rowsWritten")})

(defn database-size
  "Returns the native database size in bytes synchronously."
  [sql]
  (n/property sql "databaseSize"))

(defn next-row
  "Steps one native row; returns :done? or keyword :row data synchronously."
  [cursor]
  (let [result (n/invoke cursor "next" [])]
    (if (aget result "done")
      {:done? true}
      {:done? false :row (row-map (aget result "value"))})))

(defn raw
  "Returns the native raw-row iterator. Consume while the cursor is valid."
  [cursor]
  (n/invoke cursor "raw" []))

(defn- drain-remaining!
  [cursor]
  (loop [] (when-not (:done? (next-row cursor)) (recur))))

(defn reduce-rows
  "Reduces synchronously. Reduced values stop the reducer but still drain the native
  cursor so write/RETURNING output is safe. A throwing reducer drains remaining rows while preserving its primary error."
  [cursor reducer initial]
  (loop [result initial
         stopped? false]
    (let [row (next-row cursor)]
      (if (req! row :done?)
        (unreduced result)
        (let [next (if stopped?
                     result
                     (try (v/synchronous! (reducer result (req! row :row)) :sql-reducer)
                          (catch :default error
                            (try (drain-remaining! cursor) (catch :default _ nil))
                            (throw error))))]
          (recur next (or stopped? (reduced? next))))))))

(defn all-rows
  "Collects at most limit rows, drains any excess before reporting a cardinality error.
  Prefer a HoneySQL :limit for large queries; draining is required for write cursors."
  ([cursor]
   (all-rows cursor 10000))
  ([cursor limit]
   (v/check! [:int {:min 0}] limit :sql-limit)
   (loop [rows []
          overflow? false]
     (let [result (next-row cursor)]
       (if (:done? result)
         (if overflow?
           (v/fail! :sql-all
                    :cardinality
                    "Add a HoneySQL :limit or increase the explicit row bound.")
           rows)
         (recur (if (< (count rows) limit) (conj rows (req! result :row)) rows)
                (or overflow? (>= (count rows) limit))))))))

(defn zero-or-one
  "Drains the cursor and returns nil or one row; excess rows fail after drain."
  [cursor]
  (first (all-rows cursor 1)))

(defn exactly-one
  "Drains and requires exactly one row, including write RETURNING completion."
  [cursor]
  (let [rows (all-rows cursor 1)]
    (when-not (= 1 (count rows))
      (v/fail! :sql-one :cardinality "The query must return exactly one row."))
    (first rows)))

(defn drain!
  "Consumes every row without retaining data, then returns final native row counters."
  [cursor]
  (drain-remaining! cursor)
  (metadata cursor))
