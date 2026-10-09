(ns fast-twitch.celld.sql
  "Synchronous Cell SQL, explicit bindings and fully drained write cursors.
  HoneySQL formats SQL only; it does not execute or own application migrations."
  (:require [malli.experimental :as mx]
            [honey.sql :as honey]
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

(defn- result-columns
  [columns]
  (let [columns (mapv (fn [column]
                        (if (= :* column)
                          column
                          (let [[expression alias]
                                  (if (keyword? column) [column column] column)]
                            (v/check! :keyword alias :sql-result-alias)
                            [expression (keyword (names/identifier alias))])))
                  columns)
        aliases (map second (remove #{:*} columns))]
    (when-not (= (count aliases) (count (distinct aliases)))
      (v/fail! :sql-query :collision "Use distinct keyword result aliases."))
    columns))

(defn- prepare
  "One traversal checks authored values/raw escapes/params and projects result aliases."
  [form context]
  (cond
    (#{:raw 'raw} form)
      (v/fail! :sql-query
               :authoring
               "Use HoneySQL clauses and parameters without raw SQL escapes.")
    (map? form)
      (reduce-kv
        (fn [out clause value]
          (let [clause (prepare clause context)
                value (prepare value
                               (if (and (= :with-columns clause) (not= :literal context))
                                 :ddl
                                 context))]
            (assoc out
              clause (if (and value (#{:select :select-distinct :returning} clause))
                       (result-columns value)
                       value))))
        {}
        form)
    (sequential? form)
      (do
        (when (= :param (first form))
          (v/check! [:tuple [:= :param] :keyword] form :sql-parameter))
        (let [context (if (#{:param :inline :default} (first form)) :literal context)
              values (map #(prepare % context) form)]
          (if (vector? form) (vec values) (doall values))))
    (set? form) (into #{} (map #(prepare % :literal) form))
    (and (not= :literal context)
         (or (string? form) (and (number? form) (not= :ddl context))))
      (v/fail!
        :sql-query
        :parameters
        "Use keyword named parameters for values, and HoneySQL :inline for SQL constants.")
    :else form))

(defn ^:no-doc row-map
  "Internal native row projection; reversible aliases retain keyword namespaces."
  [row]
  (n/data-map row names/selector))

(mx/defn ^{:dynamic true :no-doc true} formatted
  "Internal HoneySQL boundary. Public queries are keyword clause maps; raw escapes
  are rejected. Only HoneySQL emits the native SQL text; params are application data."
  ([query :- [:map-of :keyword :any]]
   (formatted query {}))
  ([query :- [:map-of :keyword :any] params :- [:map-of :keyword :any]]
   (when (empty? query)
     (v/fail! :sql-query
              :authoring
              "Use HoneySQL clauses and parameters without raw SQL escapes."))
   (try
     (honey/format (prepare query :query) {:params params :dialect :ansi :quoted true})
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

(mx/defn ^:dynamic all-rows
  "Collects at most limit rows, drains any excess before reporting a cardinality error.
  Prefer a HoneySQL :limit for large queries; draining is required for write cursors."
  ([cursor]
   (all-rows cursor 10000))
  ([cursor limit :- [:int {:min 0}]]
   (reduce-rows cursor
                (fn [rows row]
                  (when (= (count rows) limit)
                    (v/fail! :sql-all
                             :cardinality
                             "Add a HoneySQL :limit or increase the explicit row bound."))
                  (conj rows row))
                [])))

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

(v/instrument! formatted all-rows)
