(ns fast-twitch.celld.storage
  "Asynchronous native Cell storage. Scalar reads are tagged to distinguish missing
  from stored null. Transaction views stay scoped to their native callback."
  (:require [fast-twitch.celld.contracts :as contracts]
            [malli.experimental :as mx]
            [cljs.core :refer [await]]
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

(mx/defn ^{:dynamic true :private true} keys!
  [keys :- [:vector :keyword]]
  (mapv key! keys))

(defn- tagged
  [value policy]
  (if (undefined? value)
    {:found? false}
    {:found? true :value (codec/decode policy value)}))

(mx/defn ^{:async true :dynamic true} get!
  "Reads one key as {:found? ... :value ...}, or vector of keys as a keyword-keyed
  map containing only present entries. :codec selects :native or versioned :json."
  [storage key & [options] :- [:? (:storage-get contracts/schemas)]]
  (let [value (await (n/invoke storage
                               "get"
                               [(if (vector? key) (to-array (keys! key)) (key! key))]))
        policy (:codec options)]
    (if (vector? key)
      (let [original-keys (zipmap (mapv key! key) key)]
        (reduce (fn [out entry]
                  (assoc out
                    (get original-keys (aget entry 0))
                      (codec/decode policy (aget entry 1))))
          {}
          (array-seq (Array.from value))))
      (tagged value policy))))

(mx/defn ^{:async true :dynamic true} put!
  "Writes one keyword key/value after encoding. Native optional storage flags have no effect in this target."
  [storage key value & [options] :- [:? (:storage-put contracts/schemas)]]
  (await (n/invoke storage "put" [(key! key) (codec/encode (:codec options) value)])))

(mx/defn ^{:dynamic true :async true} put-many!
  "Encodes an entire keyword-keyed batch before invoking native put; returns its Promise."
  [storage entries :- [:map-of :keyword :any] & [options] :-
   [:? (:storage-put-many contracts/schemas)]]
  (let [encoded (reduce-kv (fn [out key value]
                             (assoc out key (codec/encode (:codec options) value)))
                           {}
                           entries)]
    (await (n/invoke storage "put" [(n/fields encoded)]))))

(defn delete!
  "Deletes a keyword key (boolean) or vector of keys (count), preserving native results."
  [storage key]
  (n/invoke storage "delete" [(if (vector? key) (to-array (keys! key)) (key! key))]))

(def ^:private StorageList
  (conj (:storage-list contracts/schemas)
        [:codec {:optional true} [:maybe [:enum :native :json]]]))

(mx/defn ^{:async true :dynamic true} list-entries!
  "Realizes native traversal order as a vector of [keyword decoded-value] pairs.
  Reverse/range/limit order is retained beyond the CLJS lookup-map threshold."
  [storage & [options] :- [:? [:maybe StorageList]]]
  (let [policy (codec/persistence-policy! (:codec options))
        native (await (n/invoke storage
                                "list"
                                (if (nil? options)
                                  []
                                  [(n/option-fields (dissoc options :codec))])))]
    (mapv (fn [entry] [(keyword (aget entry 0)) (codec/decode policy (aget entry 1))])
      (array-seq (Array.from native)))))

(defn ^:async list!
  "Returns a keyword-keyed lookup map. Map traversal order is unspecified;
  use list-entries! for ordered reverse/range results."
  [storage & options]
  (into {} (await (apply list-entries! storage options))))

(defn delete-all!
  "Returns the native deleteAll Promise; alarm deletion follows the pinned compatibility flag."
  [storage]
  (n/invoke storage "deleteAll" []))

(defn sync!
  "Waits for native durability; rejects inside a transaction or after abort."
  [storage]
  (n/invoke storage "sync" []))

(mx/defn ^:dynamic transaction!
  "Calls callback with the actual async transaction view; do not retain it."
  [storage callback :- [:fn fn?]]
  (n/invoke storage "transaction" [callback]))

(mx/defn ^:dynamic transaction-sync!
  "Calls a zero-argument callback synchronously, checking its result inside native scope."
  [storage callback :- [:fn fn?]]
  (n/invoke storage
            "transactionSync"
            [(fn []
               (v/synchronous! (callback) :transaction-sync))]))

(defn rollback!
  "Rolls back the actual native transaction view; terminal use rejects natively."
  [transaction]
  (n/invoke transaction "rollback" []))

(v/instrument! keys! get! put! put-many! list-entries! transaction! transaction-sync!)
