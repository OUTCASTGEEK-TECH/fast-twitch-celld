(ns fast-twitch.celld.services.r2
  "Native R2 objects remain live handles with streamed bodies. Missing, unmet
  read conditions and refused writes are represented separately. Multipart
  handles belong to the node/load that opened them and cannot survive a restart."
  (:require [cljs.core :refer [await]]
            [malli.experimental :as mx]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.contracts :as contracts]
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

(def ^:private Key
  [:or :keyword :string])

(def ^:private Body
  [:fn
   #(or (nil? %)
        (string? %)
        (instance? ArrayBuffer %)
        (ArrayBuffer.isView %)
        (instance? Blob %)
        (instance? ReadableStream %))])

(def ^:private DateValue
  [:or number? [:fn #(instance? Date %)]])

(def ^:private HeadersValue
  [:fn #(instance? Headers %)])

(def ^:private HttpMetadata
  [:or HeadersValue
   (contracts/closed {:contentType :string
                      :contentLanguage :string
                      :contentDisposition :string
                      :contentEncoding :string
                      :cacheControl :string
                      :cacheExpiry DateValue})])

(def ^:private OnlyIf
  [:or HeadersValue
   (contracts/closed {:etagMatches :string
                      :etagDoesNotMatch :string
                      :uploadedAfter DateValue
                      :uploadedBefore DateValue})])

(def ^:private ReadRange
  [:or HeadersValue
   (contracts/closed
     {:offset [:int {:min 0}] :length [:int {:min 0}] :suffix [:int {:min 0}]})])

(def ^:private Checksum
  [:fn
   #(or (and (string? %) (re-matches #"[0-9a-fA-F]+" %))
        (instance? ArrayBuffer %)
        (ArrayBuffer.isView %))])

(def ^:private MultipartOptions
  {:httpMetadata HttpMetadata
   :customMetadata [:map-of :keyword :string]
   :storageClass [:enum :Standard :InfrequentAccess]})

(def ^:private WriteOptions
  (contracts/closed (merge MultipartOptions
                           {:onlyIf OnlyIf
                            :md5 Checksum
                            :sha1 Checksum
                            :sha256 Checksum
                            :sha384 Checksum
                            :sha512 Checksum})))

(def ^:private GetOptions
  (contracts/closed {:range ReadRange :onlyIf OnlyIf}))

(def ^:private ListOptions
  (contracts/closed {:prefix Key
                     :delimiter Key
                     :startAfter Key
                     :cursor :string
                     :limit [:int {:min 1 :max 1000}]
                     :include [:vector [:enum :httpMetadata :customMetadata]]}))

(def ^:private Parts
  [:vector {:min 1 :max 10000}
   (contracts/closed {:partNumber [:int {:min 1 :max 10000}] :etag :string}
                     #{:partNumber :etag})])

(mx/defn ^:dynamic ^:private key!
  :-
  [:string {:min 1}]
  [key :- Key]
  (let [key (names/text key)]
    (when (> (.-byteLength (.encode (TextEncoder.) key)) 1024)
      (v/fail! :r2-key :bytes "Use a key of at most 1024 UTF-8 bytes."))
    key))

(defn- options-native
  [options]
  (n/fields
    (reduce-kv
      (fn [out key value]
        (assoc out
          key
            (cond
              (#{:storageClass :prefix :delimiter :startAfter} key) (names/text value)
              (= :include key) (to-array (mapv names/text value))
              (and (#{:httpMetadata :onlyIf :range :customMetadata} key) (map? value))
                (n/fields value)
              :else value)))
      {}
      options)))

(defn ^:async head!
  "Returns metadata or nil without consuming any body."
  [bucket key]
  (object-map (await (n/invoke bucket "head" [(key! key)]))))

(mx/defn ^{:dynamic true :async true} get!
  "Returns {:state :missing/:condition-unmet/:found :object ...}. Only :found has a body."
  [bucket key & [options] :- [:? [:maybe GetOptions]]]
  (let [result (await (n/invoke bucket
                                "get"
                                (cond-> [(key! key)]
                                  options (conj (options-native options)))))]
    (cond (nil? result) {:state :missing}
          (nil? (aget result "body")) {:state :condition-unmet
                                       :object (object-map result)}
          :else {:state :found :object (object-map result)})))

(mx/defn ^{:dynamic true :async true} put!
  "Returns the stored native metadata, or nil when a native write precondition refuses it."
  [bucket key value :- Body & [options] :- [:? [:maybe WriteOptions]]]
  (object-map (await (n/invoke bucket
                               "put"
                               (cond-> [(key! key) value]
                                 options (conj (options-native options)))))))

(mx/defn ^:dynamic delete!
  "Deletes one key or a checked vector of keys; returns the native Promise."
  [bucket keys :- [:or Key [:vector {:max 1000} Key]]]
  (n/invoke bucket
            "delete"
            [(if (vector? keys)
               (to-array (mapv key! keys))
               (key! keys))]))

(mx/defn ^{:dynamic true :async true} list!
  "Returns streamed-object metadata pages with :truncated? and an opaque cursor when supplied."
  [bucket & [options] :- [:? [:maybe ListOptions]]]
  (let [result (await
                 (n/invoke bucket "list" (if options [(options-native options)] [])))]
    (cond-> {:objects (mapv object-map (array-seq (aget result "objects")))
             :truncated? (aget result "truncated")
             :delimited-prefixes (vec (array-seq (aget result "delimitedPrefixes")))}
      (some? (aget result "cursor")) (assoc :cursor (aget result "cursor")))))

(mx/defn ^:dynamic create-multipart!
  "Returns a Promise of a native multipart handle. Checksums/conditions are unsupported here."
  [bucket key & [options :as supplied] :- [:? (contracts/closed MultipartOptions)]]
  (n/invoke bucket
            "createMultipartUpload"
            (cond-> [(key! key)] supplied (conj (options-native options)))))

(mx/defn ^:dynamic resume-multipart
  "Returns a native handle immediately; missing/lost uploads reject on first use."
  [bucket key upload-id :- :string]
  (n/invoke bucket
            "resumeMultipartUpload"
            [(key! key) upload-id]))

(mx/defn ^{:dynamic true :async true} upload-part!
  "Uploads one checked part number and native body; returns native part metadata."
  [upload number :- [:int {:min 1 :max 10000}] value :- Body]
  (n/data-map (await (n/invoke upload
                               "uploadPart"
                               [number value]))))

(mx/defn ^{:dynamic true :async true} complete!
  "Completes in native part order using checked {partNumber,etag} records."
  [upload parts :- Parts]
  (object-map (await (n/invoke upload
                               "complete"
                               [(to-array
                                  (mapv n/fields parts))]))))

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

(v/instrument! key!
               get!
               put!
               delete!
               list!
               create-multipart!
               resume-multipart
               upload-part!
               complete!)
