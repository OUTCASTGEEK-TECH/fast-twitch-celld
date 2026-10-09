(ns fast-twitch.celld.storage
  "Asynchronous native Cell storage. Scalar reads are tagged to distinguish missing
  from stored null. Transaction views stay scoped to their native callback."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.validation :as v]
  )
  (:refer-global :only [Array]))

(defn native
  "Returns the original native storage or transaction view."
  [storage]
  storage)

(defn- key!
  [key]
  (n/key-name key :storage-key))

(defn- keys!
  [keys]
  (mapv key! (v/check! [:vector :keyword] keys :storage-keys)))

(defn- tagged
  [value policy]
  (if (undefined? value)
    {:found? false}
    {:found? true :value (codec/decode policy value)}))

(defn ^:async get!
  "Reads one key as {:found? ... :value ...}, or vector of keys as a keyword-keyed
  map containing only present entries. :codec selects :native or versioned :json."
  ([storage key]
   (get! storage key {}))
  ([storage key options]
   (n/options options #{:codec} :storage-get)
   (let [value (await (n/invoke storage
                                "get"
                                [(if (vector? key) (to-array (keys! key)) (key! key))]))
         policy (:codec options)]
     (if (vector? key)
       (let [original-keys (zipmap (mapv key! key) key)]
         (reduce (fn [out entry]
                   (assoc out
                     (get original-keys (aget entry 0)) (codec/decode policy
                                                                      (aget entry 1))))
           {}
           (array-seq (Array.from value))))
       (tagged value policy)))))

(declare put-many!)

(defn ^:async put!
  "Writes one value or a keyword-keyed map. Batch values are all encoded before
  the native mutation. Native optional storage flags have no effect in this target."
  ([storage entries]
   (put-many! storage entries {:codec :native}))
  ([storage key value]
   (put! storage key value {}))
  ([storage key value options]
   (n/options options #{:codec} :storage-put)
   (await (n/invoke storage "put" [(key! key) (codec/encode (:codec options) value)]))))

(defn ^:async put-many!
  "Encodes an entire keyword-keyed batch before invoking native put; returns its Promise."
  [storage entries options]
  (n/options options #{:codec} :storage-put-many)
  (let [entries (v/check! [:map-of :keyword :any] entries :storage-put-many)
        encoded (reduce-kv (fn [out key value]
                             (assoc out key (codec/encode (:codec options) value)))
                           {}
                           entries)]
    (await (n/invoke storage "put" [(n/fields encoded)]))))

(defn delete!
  "Deletes a keyword key (boolean) or vector of keys (count), preserving native results."
  [storage key]
  (n/invoke storage "delete" [(if (vector? key) (to-array (keys! key)) (key! key))]))

(def list-options
  #{:start :startAfter :end :prefix :reverse :limit})

(defn ^:async list-entries!
  "Realizes native traversal order as a vector of [keyword decoded-value] pairs.
  Reverse/range/limit order is retained beyond the CLJS lookup-map threshold."
  ([storage]
   (list-entries! storage nil))
  ([storage options]
   (let [policy (codec/persistence-policy! (:codec options))
         native (await (n/invoke storage
                                 "list"
                                 (if (nil? options)
                                   []
                                   [(n/options (dissoc options :codec)
                                               list-options
                                               :storage-list)])))]
     (mapv (fn [entry] [(keyword (aget entry 0)) (codec/decode policy (aget entry 1))])
       (array-seq (Array.from native))))))

(defn ^:async list!
  "Returns a keyword-keyed lookup map, with explicit value decoding. Map traversal
  order is unspecified; use list-entries! for ordered reverse/range results."
  ([storage]
   (into {} (await (list-entries! storage))))
  ([storage options]
   (into {} (await (list-entries! storage options)))))

(defn delete-all!
  "Returns the native deleteAll Promise; alarm deletion follows the pinned compatibility flag."
  [storage]
  (n/invoke storage "deleteAll" []))

(defn sync!
  "Waits for native durability; rejects inside a transaction or after abort."
  [storage]
  (n/invoke storage "sync" []))

(defn transaction!
  "Calls callback with the actual async transaction view; do not retain it."
  [storage callback]
  (v/check! [:fn fn?] callback :transaction)
  (n/invoke storage "transaction" [callback]))

(defn transaction-sync!
  "Calls a zero-argument callback synchronously, checking its result inside native scope."
  [storage callback]
  (v/check! [:fn fn?] callback :transaction-sync)
  (n/invoke storage
            "transactionSync"
            [(fn []
               (v/synchronous! (callback) :transaction-sync))]))

(defn rollback!
  "Rolls back the actual native transaction view; terminal use rejects natively."
  [transaction]
  (n/invoke transaction "rollback" []))
