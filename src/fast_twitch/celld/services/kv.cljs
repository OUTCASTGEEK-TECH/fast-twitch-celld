(ns fast-twitch.celld.services.kv
  "Service KV modes, metadata and bulk read conversion. A JSON null and missing
  JSON value are indistinguishable in native get; metadata does not prove presence."
  (:require [cljs.core :refer [await]]
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

(defn- key!
  [value]
  (let [value (n/key-name value :service-kv-key)]
    (v/check! [:string {:min 1}] value :service-kv-key)
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

(defn- keys-native
  [value]
  (if (vector? value)
    (to-array
      (mapv key!
        (v/check! [:vector {:max 100} :keyword] value :service-kv-bulk)))
    (key! value)))

(defn- read-options
  [options]
  (if (or (keyword? options) (string? options))
    (v/check! [:enum "text" "json" "arrayBuffer" "stream"]
              (names/text options)
              :service-kv-mode)
    (n/options options #{:type} :service-kv-read)))

(defn- read-result
  ([key result]
   (read-result key result false))
  ([key result json?]
   (if (vector? key)
     (reduce (fn [out entry]
               (assoc out
                 (keyword (aget entry 0))
                   (if json? (json/decode-native (aget entry 1)) (aget entry 1))))
       {}
       (array-seq (Array.from result)))
     (if json? (json/decode-native result) result))))

(defn ^:async get!
  "Returns native text/JSON/bytes/stream or nil. Bulk maps retain the caller’s key identities and preserve null holes.
  JSON mode projects received fields to keywords; lossless CLJS decoding uses get-json!."
  ([binding key]
   (read-result key (await (n/invoke binding "get" [(keys-native key)]))))
  ([binding key options]
   (read-result key
                (await
                  (n/invoke binding "get" [(keys-native key) (read-options options)]))
                (#{:json "json"} (if (map? options) (:type options) options)))))

(defn- metadata-map
  [record]
  (let [record (n/data-map record)
        metadata (:metadata record)]
    (cond-> record
      (some? metadata) (update :metadata
                               #(if (= "json" (aget % "fastTwitchCodec"))
                                  (codec/decode :json %)
                                  (json/decode-native %))))))

(defn- metadata-result
  ([key result]
   (metadata-result key result false))
  ([key result json?]
   (let [project (fn [value]
                   (cond-> (metadata-map value)
                     json? (update :value json/decode-native)))]
     (if (vector? key)
       (into {}
             (map (fn [[key value]] [key (project value)])
               (read-result key result)))
       (project result)))))

(defn ^:async get-with-metadata!
  "Returns value/metadata/cacheStatus data, retaining native streamed values."
  ([binding key]
   (metadata-result key (await (n/invoke binding "getWithMetadata" [(keys-native key)]))))
  ([binding key options]
   (let [result (await (n/invoke binding
                                 "getWithMetadata"
                                 [(keys-native key) (read-options options)]))]
     (metadata-result key
                      result
                      (#{:json "json"} (if (map? options) (:type options) options))))))

(defn put!
  "Writes string/native bytes/stream, preserving stream ownership. Expiration units are seconds.
  CLJS metadata uses the lossless versioned JSON codec; native metadata stays native.
  Resources and oversized encoded metadata reject before invoking put."
  ([binding key value]
   (n/invoke binding "put" [(key! key) (body! value)]))
  ([binding key value options]
   (let [metadata (:metadata options)
         options (cond-> options
                   (or (coll? metadata) (keyword? metadata))
                     (update :metadata #(codec/encode :json %)))
         native-options
           (n/options options #{:expiration :expirationTtl :metadata} :service-kv-put)]
     (when (contains? options :metadata) (codec/native-value! (:metadata options)))
     (n/invoke binding "put" [(key! key) (body! value) native-options]))))

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

(defn ^:async list!
  "Returns keyword metadata records and an opaque native continuation cursor."
  ([binding]
   (list! binding nil))
  ([binding options]
   (let [result (await (n/invoke binding
                                 "list"
                                 (if options
                                   [(n/options options
                                               #{:prefix :cursor :limit}
                                               :service-kv-list)]
                                   [])))]
     {:keys (mapv #(update (metadata-map %) :name keyword)
              (array-seq (aget result "keys")))
      :list-complete? (aget result "list_complete")
      :cursor (aget result "cursor")})))
