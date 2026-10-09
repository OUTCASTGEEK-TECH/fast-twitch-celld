(ns fast-twitch.celld.services.kv
  "Service KV modes, metadata and bulk read conversion. A JSON null and missing
  JSON value are indistinguishable in native get; metadata does not prove presence."
  (:require [fast-twitch.celld.contracts :as contracts]
            [malli.experimental :as mx]
            [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.names :as names]
            [fast-twitch.codecs.json :as json])
  (:refer-global :only [Array TextEncoder ArrayBuffer ReadableStream]))

(defn native
  "Returns the original native service KV namespace."
  [binding]
  binding)

(mx/defn ^{:dynamic true :private true} key!
  :-
  [:string {:min 1}]
  [value :- :keyword]
  (let [value (n/key-name value :service-kv-key)]
    (when (or (#{"." ".."} value) (> (.-byteLength (.encode (TextEncoder.) value)) 512))
      (v/fail! :service-kv-key
               :value
               "Use a native KV key of at most 512 UTF-8 bytes, excluding dot names."))
    value))

(defn- body!
  [value]
  (when-not (or (string? value)
                (instance? ArrayBuffer value)
                (ArrayBuffer.isView value)
                (instance? ReadableStream value))
    (v/fail! :service-kv-body
             :type
             "Use a native string, byte buffer or ReadableStream."))
  value)

(mx/defn ^{:dynamic true :private true} keys-native
  [value :- [:or :keyword [:vector {:max 100} :keyword]]]
  (if (vector? value)
    (to-array
      (mapv key! value))
    (key! value)))

(def ^:private ReadOptions
  [:maybe
   [:or [:enum :text :json :arrayBuffer :stream] (:service-kv-read contracts/schemas)]])

(defn- read-options
  [options]
  [(when options (if (keyword? options) (names/text options) (n/option-fields options)))
   (if (= :json (if (keyword? options) options (:type options)))
     json/decode-native
     identity)])

(defn- project-result
  [key result project]
  (if (vector? key)
    (reduce (fn [out entry]
              (assoc out
                (keyword (aget entry 0))
                  (project (aget entry 1))))
      {}
      (array-seq (Array.from result)))
    (project result)))

(mx/defn ^{:dynamic true :async true} get!
  "Returns native text/JSON/bytes/stream or nil. Bulk maps retain keyword keys and null holes.
  JSON mode projects received fields to keywords; lossless CLJS decoding uses get-json!."
  [binding key & [options] :- [:? ReadOptions]]
  (let [[native project] (read-options options)]
    (project-result
      key
      (await (n/invoke binding "get" (cond-> [(keys-native key)] native (conj native))))
      project)))

(defn- metadata-map
  [record]
  (let [record (n/data-map record)
        metadata (:metadata record)]
    (cond-> record
      (some? metadata) (update :metadata
                               #(if (and (string? %)
                                         (re-find #"^\s*\[\s*\"fast-twitch/cljs-json\""
                                                  %))
                                  (codec/decode :json %)
                                  (json/decode-native %))))))

(mx/defn ^{:dynamic true :async true} get-with-metadata!
  "Returns value/metadata/cacheStatus data, retaining native streamed values."
  [binding key & [options] :- [:? ReadOptions]]
  (let [[native project] (read-options options)]
    (project-result key
                    (await (n/invoke binding
                                     "getWithMetadata"
                                     (cond-> [(keys-native key)] native (conj native))))
                    (fn [record]
                      (let [record (metadata-map record)]
                        (cond-> record
                          (contains? record :value) (update :value project)))))))

(mx/defn ^:dynamic put!
  "Writes string/native bytes/stream, preserving ownership. Expiration units are seconds.
  CLJS metadata uses versioned JSON; native metadata stays native. All values are checked before put."
  [binding key value & [options :as supplied] :- [:? (:service-kv-put contracts/schemas)]]
  (let [options (cond-> options
                  (contains? options :metadata)
                    (update :metadata
                            #(if (or (coll? %) (keyword? %))
                               (codec/encode :json %)
                               (codec/native-value! %))))]
    (n/invoke binding
              "put"
              (cond-> [(key! key) (body! value)]
                supplied (conj (n/option-fields options))))))

(defn put-json!
  "Explicitly serializes supported CLJS JSON to text before writing."
  [binding key value options]
  (put! binding key (codec/write-json value) options))

(defn ^:async get-json!
  "Decodes lossless CLJS JSON text; missing and stored nil both return nil."
  [binding key]
  (when-let [text (await (get! binding key :text))] (codec/read-json text)))

(defn delete!
  "Returns the native delete Promise for a keyword key."
  [binding key]
  (n/invoke binding "delete" [(key! key)]))

(mx/defn ^{:async true :dynamic true} list!
  "Returns keyword metadata records and an opaque native continuation cursor."
  [binding & [options] :- [:? [:maybe (:service-kv-list contracts/schemas)]]]
  (let [result (await
                 (n/invoke binding "list" (if options [(n/option-fields options)] [])))]
    {:keys (mapv #(update (metadata-map %) :name keyword)
             (array-seq (aget result "keys")))
     :list-complete? (aget result "list_complete")
     :cursor (aget result "cursor")}))

(v/instrument! key! keys-native get! get-with-metadata! put! list!)
