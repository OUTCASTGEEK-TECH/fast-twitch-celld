(ns fast-twitch.celld.services.r2
  "Native R2 objects remain live handles with streamed bodies. Missing, unmet
  read conditions and refused writes are represented separately. Multipart
  handles belong to the node/load that opened them and cannot survive a restart."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names]
            [fast-twitch.codecs.json :as json])
  (:refer-global :only [Object Headers Date TextEncoder ArrayBuffer Blob ReadableStream]))

(defn native
  "Returns the actual bucket/object/multipart handle."
  [handle]
  handle)

(defn object-map
  "Projects object metadata while retaining the original object and body stream."
  [object]
  (when object
    (reduce (fn [out field]
              (let [value (n/property object field)]
                (if (undefined? value)
                  out
                  (assoc out
                    (keyword field)
                      (if (#{"httpMetadata" "customMetadata" "checksums" "range"} field)
                        (n/data-map value)
                        value)))))
      {:fast-twitch.r2/object object}
      ["key" "version" "size" "etag" "httpEtag" "uploaded" "httpMetadata" "customMetadata"
       "checksums" "storageClass" "range" "body" "bodyUsed"])))

(defn- key!
  [key]
  (let [key (names/text key)]
    (v/check! [:string {:min 1}] key :r2-key)
    (when (> (.-byteLength (.encode (TextEncoder.) key)) 1024)
      (v/fail! :r2-key :bytes "Use a key of at most 1024 UTF-8 bytes."))
    key))

(defn- body!
  [value]
  (when-not (or (nil? value)
                (string? value)
                (instance? ArrayBuffer value)
                (ArrayBuffer.isView value)
                (instance? Blob value)
                (instance? ReadableStream value))
    (v/fail! :r2-body :type "Use a native string, byte buffer, Blob or ReadableStream."))
  value)

(defn- nested!
  [key value]
  (let [date [:or number? [:fn #(instance? Date %)]]
        schemas {:httpMetadata [:or [:fn #(instance? Headers %)]
                                [:map {:closed true}
                                 [:contentType {:optional true} :string]
                                 [:contentLanguage {:optional true} :string]
                                 [:contentDisposition {:optional true} :string]
                                 [:contentEncoding {:optional true} :string]
                                 [:cacheControl {:optional true} :string]
                                 [:cacheExpiry {:optional true} date]]]
                 :onlyIf [:or [:fn #(instance? Headers %)]
                          [:map {:closed true} [:etagMatches {:optional true} :string]
                           [:etagDoesNotMatch {:optional true} :string]
                           [:uploadedAfter {:optional true} date]
                           [:uploadedBefore {:optional true} date]]]
                 :range [:or [:fn #(instance? Headers %)]
                         [:map {:closed true} [:offset {:optional true} [:int {:min 0}]]
                          [:length {:optional true} [:int {:min 0}]]
                          [:suffix {:optional true} [:int {:min 0}]]]]}]
    (when-let [schema (get schemas key)] (v/check! schema value :r2-options))
    (when (= key :storageClass)
      (v/check! [:enum "Standard" "InfrequentAccess"] value :r2-storage-class))
    (when (= key :limit) (v/check! [:int {:min 1 :max 1000}] value :r2-list-limit))
    (when (#{:prefix :delimiter :cursor :startAfter} key)
      (v/check! :string value :r2-list))
    (when (#{:md5 :sha1 :sha256 :sha384 :sha512} key)
      (when-not (or (and (string? value) (re-matches #"[0-9a-fA-F]+" value))
                    (instance? ArrayBuffer value)
                    (ArrayBuffer.isView value))
        (v/fail! :r2-checksum :type "Use hex or a native byte buffer.")))
    value))

(def write-options
  #{:httpMetadata :customMetadata :onlyIf :md5 :sha1 :sha256 :sha384 :sha512
    :storageClass})

(defn- options-native
  [options allowed operation]
  ;; Nested native options retain Headers/resources; maps are explicitly projected.
  (let [options (cond-> options
                  (contains? options :storageClass) (update :storageClass names/text)
                  (contains? options :include)
                    (update :include
                            #(mapv names/text
                               (v/check! [:vector [:enum :httpMetadata :customMetadata]]
                                         %
                                         :r2-list-include)))
                  (contains? options :prefix) (update :prefix names/text)
                  (contains? options :delimiter) (update :delimiter names/text)
                  (contains? options :startAfter) (update :startAfter names/text))
        nested {:httpMetadata #{:contentType :contentLanguage :contentDisposition
                                :contentEncoding :cacheControl :cacheExpiry}
                :onlyIf #{:etagMatches :etagDoesNotMatch :uploadedAfter :uploadedBefore}
                :range #{:offset :length :suffix}}]
    (n/options (reduce-kv (fn [out key value]
                            (nested! key value)
                            (assoc out
                              key (if (and (map? value) (get nested key))
                                    (n/options value (get nested key) operation)
                                    (if (and (= key :customMetadata) (map? value))
                                      (n/fields (v/check! [:map-of :keyword :string]
                                                          value
                                                          operation))
                                      (if (= key :include) (to-array value) value)))))
                          {}
                          options)
               allowed
               operation)))

(defn ^:async head!
  "Returns metadata or nil without consuming any body."
  [bucket key]
  (object-map (await (n/invoke bucket "head" [(key! key)]))))

(defn ^:async get!
  "Returns {:state :missing/:condition-unmet/:found :object ...}. Only :found has a body."
  ([bucket key]
   (get! bucket key nil))
  ([bucket key options]
   (let [result (await (n/invoke bucket
                                 "get"
                                 (cond-> [(key! key)]
                                   options (conj (options-native options
                                                                 #{:range :onlyIf}
                                                                 :r2-get)))))]
     (cond (nil? result) {:state :missing}
           (nil? (aget result "body")) {:state :condition-unmet
                                        :object (object-map result)}
           :else {:state :found :object (object-map result)}))))

(defn ^:async put!
  "Returns the stored native metadata, or nil when a native write precondition refuses it."
  ([bucket key value]
   (put! bucket key value nil))
  ([bucket key value options]
   (object-map (await (n/invoke bucket
                                "put"
                                (cond-> [(key! key) (body! value)]
                                  options (conj (options-native options
                                                                write-options
                                                                :r2-put))))))))

(defn delete!
  "Deletes one key or a checked vector of keys; returns the native Promise."
  [bucket keys]
  (n/invoke bucket
            "delete"
            [(if (vector? keys)
               (to-array (mapv key!
                           (v/check! [:vector {:max 1000} [:or :keyword :string]]
                                     keys
                                     :r2-delete)))
               (key! keys))]))

(defn ^:async list!
  "Returns streamed-object metadata pages with :truncated? and an opaque cursor when supplied."
  ([bucket]
   (list! bucket nil))
  ([bucket options]
   (let [result (await (n/invoke bucket
                                 "list"
                                 (if options
                                   [(options-native options
                                                    #{:prefix :delimiter :cursor
                                                      :startAfter :limit :include}
                                                    :r2-list)]
                                   [])))]
     (cond-> {:objects (mapv object-map (array-seq (aget result "objects")))
              :truncated? (aget result "truncated")
              :delimited-prefixes (vec (array-seq (aget result "delimitedPrefixes")))}
       (some? (aget result "cursor")) (assoc :cursor (aget result "cursor"))))))

(defn create-multipart!
  "Returns a Promise of a native multipart handle. Checksums/conditions are unsupported here."
  ([bucket key]
   (n/invoke bucket "createMultipartUpload" [(key! key)]))
  ([bucket key options]
   (n/invoke bucket
             "createMultipartUpload"
             [(key! key)
              (options-native options
                              #{:httpMetadata :customMetadata :storageClass}
                              :r2-multipart)])))

(defn resume-multipart
  "Returns a native handle immediately; missing/lost uploads reject on first use."
  [bucket key upload-id]
  (n/invoke bucket
            "resumeMultipartUpload"
            [(key! key) (v/check! :string upload-id :r2-upload-id)]))

(defn ^:async upload-part!
  "Uploads one checked part number and native body; returns native part metadata."
  [upload number value]
  (n/data-map (await (n/invoke upload
                               "uploadPart"
                               [(v/check! [:int {:min 1 :max 10000}] number :r2-part)
                                (body! value)]))))

(defn ^:async complete!
  "Completes in native part order using checked {partNumber,etag} records."
  [upload parts]
  (v/check! [:vector {:min 1 :max 10000}
             [:map {:closed true} [:partNumber [:int {:min 1 :max 10000}]]
              [:etag :string]]]
            parts
            :r2-parts)
  (object-map (await (n/invoke upload
                               "complete"
                               [(to-array
                                  (mapv #(n/options % #{:partNumber :etag} :r2-part)
                                    parts))]))))

(defn abort!
  "Aborts the native upload and preserves rejection."
  [upload]
  (n/invoke upload "abort" []))

(defn body
  "Returns the original native readable body; consuming it is caller-owned."
  [object]
  (n/property object "body"))

(defn write-http-metadata!
  "Writes object-owned HTTP metadata into the supplied native Headers."
  [object headers]
  (n/invoke object "writeHttpMetadata" [headers]))

(defn array-buffer!
  "Explicit native whole-body consumption; native ownership/rejection applies."
  [object]
  (n/invoke object "arrayBuffer" []))

(defn bytes!
  "Explicit native bytes consumption; prefer bounded shared readers for untrusted bodies."
  [object]
  (n/invoke object "bytes" []))

(defn text!
  "Explicit native UTF-8 text consumption."
  [object]
  (n/invoke object "text" []))

(defn ^:async json!
  "Consumes native JSON and projects received object fields to keyword data."
  [object]
  (json/decode-native (await (n/invoke object "json" []))))

(defn blob!
  "Explicit native Blob consumption."
  [object]
  (n/invoke object "blob" []))

(defn body-used?
  "Reads the live native bodyUsed getter; does not consume or clone the body."
  [object]
  (n/property object "bodyUsed"))

(defn upload-map
  "Projects multipart key/uploadId and retains its live native handle."
  [upload]
  {:key (n/property upload "key")
   :uploadId (n/property upload "uploadId")
   :fast-twitch.r2/upload upload})
