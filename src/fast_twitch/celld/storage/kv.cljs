(ns fast-twitch.celld.storage.kv
  "Synchronous Cell KV. Iterator realization happens before another list invalidates it."
  (:require [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.contracts :as contracts]
            [malli.experimental :as mx]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.codec :as codec]
  )
  (:refer-global :only [Array]))

(defn native
  "Returns storage.kv, the synchronous native view of Cell storage."
  [storage]
  (n/property storage "kv"))

(mx/defn ^:dynamic get-value
  "Synchronously reads a keyword key; tagged :found? distinguishes undefined from null."
  [kv key & [policy] :- [:? [:maybe [:enum :native :json]]]]
  (let [value (n/invoke kv "get" [(n/key-name key :kv-get)])]
    (if (undefined? value)
      {:found? false}
      {:found? true :value (codec/decode policy value)})))

(mx/defn ^:dynamic put!
  "Synchronously writes one keyword key and encoded native/JSON value; returns the native result."
  [kv key value & [policy] :- [:? [:maybe [:enum :native :json]]]]
  (n/invoke kv "put" [(n/key-name key :kv-put) (codec/encode policy value)]))

(defn delete!
  "Synchronously deletes one key and returns the native boolean."
  [kv key]
  (n/invoke kv "delete" [(n/key-name key :kv-delete)]))

(mx/defn ^:dynamic list-native
  "Returns the live iterator. A subsequent list on this object invalidates it."
  [kv & [options :as supplied] :- [:? (:kv-list contracts/schemas)]]
  (n/invoke kv "list" (if supplied [(n/option-fields options)] [])))

(mx/defn ^:dynamic list-values
  "Realizes keyword/value pairs while the native iterator is valid; optional codec decodes values."
  [kv & [options policy :as supplied] :-
   [:cat [:? :any] [:? [:maybe [:enum :native :json]]]]]
  (mapv (fn [entry] [(keyword (aget entry 0)) (codec/decode policy (aget entry 1))])
    (array-seq (Array.from (apply list-native kv (when supplied [options]))))))

(v/instrument! get-value put! list-native list-values)
