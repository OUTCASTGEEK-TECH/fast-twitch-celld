(ns fast-twitch.celld.storage.kv
  "Synchronous Cell KV. Iterator realization happens before another list invalidates it."
  (:require [fast-twitch.celld.native :as n]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.storage :as storage]
  )
  (:refer-global :only [Array]))

(defn native
  "Returns storage.kv, the synchronous native view of Cell storage."
  [storage]
  (n/property storage "kv"))

(defn get-value
  "Synchronously reads a keyword key; tagged :found? distinguishes undefined from null. An optional explicit codec decodes the value."
  ([kv key]
   (get-value kv key :native))
  ([kv key policy]
   (codec/persistence-policy! policy)
   (let [value (n/invoke kv "get" [(n/key-name key :kv-get)])]
     (if (undefined? value)
       {:found? false}
       {:found? true :value (codec/decode policy value)}))))

(defn put!
  "Synchronously writes one keyword key and explicitly encoded native/JSON value. Returns the native result."
  ([kv key value]
   (put! kv key value :native))
  ([kv key value policy]
   (codec/persistence-policy! policy)
   (n/invoke kv "put" [(n/key-name key :kv-put) (codec/encode policy value)])))

(defn delete!
  "Synchronously deletes one key and returns the native boolean."
  [kv key]
  (n/invoke kv "delete" [(n/key-name key :kv-delete)]))

(defn list-native
  "Returns the live iterator. A subsequent list on this object invalidates it."
  ([kv]
   (n/invoke kv "list" []))
  ([kv options]
   (n/invoke kv "list" [(n/options options storage/list-options :kv-list)])))

(defn list-values
  "Realizes keyword/value pairs immediately while the native iterator is valid; decoding is explicit."
  ([kv]
   (mapv (fn [entry] [(keyword (aget entry 0)) (codec/decode :native (aget entry 1))])
     (array-seq (Array.from (list-native kv)))))
  ([kv options policy]
   (codec/persistence-policy! policy)
   (mapv (fn [entry] [(keyword (aget entry 0)) (codec/decode policy (aget entry 1))])
     (array-seq (Array.from (list-native kv options))))))
