(ns fast-twitch.celld.services.d1
  "Async D1 statement/session operations. Native statements and bookmarks retain
  receiver identity; Cell SQL cursor/transaction semantics do not apply here."
  (:refer-clojure :exclude [run!])
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.sql :as sql]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names])
  (:refer-global :only [Array]))

(defn native
  "Returns the native database, session or statement handle."
  [handle]
  handle)

(defn ^:no-doc binding!
  "Checks D1 positional values separately from Cell SQL. Native booleans and
  numeric byte arrays retain native D1 coercion; typed views retain identity.
  Nonfinite or imprecise numeric scalar values reject before statement effects."
  [value]
  (cond
    (boolean? value) value
    (Array.isArray value)
      (do (when-not (.every value
                            (fn [byte]
                              (and (number? byte) (<= 0 byte) (< byte 256))))
            (v/fail! :d1-binding
                     :bytes
                     "Use native numeric byte arrays with values from 0 up to 256."))
          value)
    :else (sql/binding! value)))

(defn prepare
  "Creates and binds a native D1 statement from HoneySQL data synchronously.
  Optional keyword params resolve named placeholders; all values validate before prepare."
  ([database query]
   (prepare database query {}))
  ([database query params]
   (let [[text & values] (sql/formatted query params)
         values (mapv binding! values)
         statement (n/invoke database "prepare" [text])]
     (if (seq values) (n/invoke statement "bind" values) statement))))

(defn- result-map
  [native]
  {:success? (aget native "success")
   :meta (n/data-map (aget native "meta"))
   :results (mapv #(sql/row-map %) (array-seq (aget native "results")))})

(defn ^:async all!
  "Returns a checked async result map with keyword row columns."
  [statement]
  (result-map (await (n/invoke statement "all" []))))

(defn ^:async run!
  "Returns native success/meta/rows without manufacturing a transaction."
  [statement]
  (result-map (await (n/invoke statement "run" []))))

(defn ^:async first!
  "No column gives a keyword row or nil. Column form returns its native value;
  a missing column rejects natively, while an absent row remains nil."
  ([statement]
   (sql/row-map (await (n/invoke statement "first" []))))
  ([statement column]
   (await (n/invoke statement
                    "first"
                    [(names/identifier (v/check! :keyword column :d1-column))]))))

(defn ^:async raw!
  "Returns vectors in native column order; :columnNames prepends keyword selectors."
  ([statement]
   (mapv vec (array-seq (await (n/invoke statement "raw" [])))))
  ([statement options]
   (let [rows (mapv vec
                (array-seq (await (n/invoke
                                    statement
                                    "raw"
                                    [(n/options options #{:columnNames} :d1-raw)]))))]
     (if (and (:columnNames options) (seq rows))
       (update rows 0 #(mapv names/selector %))
       rows))))

(defn ^:async batch!
  "Runs native ordered batch/atomicity and preserves each result's metadata."
  [database statements]
  (v/check! [:vector :any] statements :d1-batch)
  (mapv result-map
    (array-seq (await (n/invoke database "batch" [(to-array statements)])))))

(defn ^:async exec!
  "Runs unparameterized HoneySQL DDL through native exec and returns keyword metadata.
  Queries carrying values use prepare/run! so D1 binds parameters natively."
  [database query]
  (let [[text & values] (sql/formatted query)]
    (when (seq values)
      (v/fail! :d1-exec :parameters "Use prepare/run! for parameterized HoneySQL."))
    (n/data-map (await (n/invoke database "exec" [text])))))

(defn with-session
  "Returns a native session synchronously. Bookmark/constraint omission is preserved."
  ([database]
   (n/invoke database "withSession" []))
  ([database constraint-or-bookmark]
   (n/invoke database
             "withSession"
             [(v/check! :string (names/text constraint-or-bookmark) :d1-session)])))

(defn bookmark
  "Returns the native opaque bookmark or nil before a successful session query."
  [session]
  (n/invoke session "getBookmark" []))
