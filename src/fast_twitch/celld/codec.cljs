(ns fast-twitch.celld.codec
  "Explicit versioned payload policy over Fast-Twitch JSON; native resources stay native."
  (:require [fast-twitch.codecs.json :as json]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names])
  (:require-macros [fast-twitch.celld.contracts :refer [payload-limit]])
  (:refer-global :only
                 [ArrayBuffer Date Map Set RegExp Object Uint8Array WeakSet TextEncoder]))

(defn- native-data!
  [value depth seen]
  (when (> depth 64) (v/fail! :native-value :limit "Reduce payload depth."))
  (cond
    (or (nil? value) (string? value) (boolean? value) (= "bigint" (goog/typeOf value)))
      value
    (number? value) (v/safe-number! value :native-value)
    (or (instance? ArrayBuffer value)
        (ArrayBuffer.isView value)
        (instance? Date value)
        (instance? RegExp value))
      value
    (or (coll? value) (keyword? value) (symbol? value) (fn? value))
      (v/fail! :native-value
               :codec
               "Choose :json for supported CLJS data, or supply native structured data.")
    (or (instance? Map value)
        (instance? Set value)
        (array? value)
        (let [prototype (Object.getPrototypeOf value)]
          (or (nil? prototype) (= prototype (.-prototype Object)))))
      (do (when-not (.has seen value)
            (.add seen value)
            (cond
              (instance? Map value) (.forEach value
                                              (fn [item key]
                                                (native-data! key (inc depth) seen)
                                                (native-data! item (inc depth) seen)))
              (instance? Set value) (.forEach value
                                              (fn [item]
                                                (native-data! item (inc depth) seen)))
              (array? value) (doseq [item (array-seq value)]
                               (native-data! item (inc depth) seen))
              :else (doseq [key (array-seq (Object.keys value))]
                      (native-data! (aget value key) (inc depth) seen))))
          value)
    :else
      (v/fail! :native-value :resource "This resource is not a serializable payload.")))

(defn native-value!
  "Checks native structured data, including BigInt and cycles, without cloning resources or losing identity.
  CLJS collections/functions and unsupported capabilities require an explicit operation policy."
  ([value]
   (native-value! value 0))
  ([value depth]
   (native-data! value depth (WeakSet.))))

(defn rpc-value!
  "Explicit native RPC domain: keeps live native capabilities, while rejecting
  CLJS collections/functions and imprecise numbers. Native RPC enforces transfer
  eligibility; this policy is never accepted by storage or Workflow persistence."
  [value]
  (cond (number? value) (v/safe-number! value :rpc-value)
        (or (coll? value) (keyword? value) (symbol? value) (fn? value))
          (v/fail! :rpc-value
                   :codec
                   "Use native RPC values/capabilities or the explicit JSON codec.")
        :else value))

(def ^:private max-payload-bytes
  (payload-limit))

(defn- json-size!
  "Counts exact UTF-8 bytes of the encoded JSON payload, including version/tag data."
  [text]
  (v/check! :string text :json-payload)
  (when (> (.-byteLength (.encode (TextEncoder.) text)) max-payload-bytes)
    (v/fail! :json-payload :limit
             "Reduce the encoded UTF-8 JSON payload to the target budget."
               {:expected :payload-bytes :path [:payload]}))
  text)

(defn- json-data
  "Prefixes every keyword key; the reserved keyword tag cannot collide with user maps."
  [value depth]
  (when (> depth 62)
    (v/fail! :json-value :limit "Reduce encoded JSON depth to 64 or less."))
  (cond
    (keyword? value) {(keyword "~keyword") (subs (str value) 1)}
    (map? value)
      (reduce-kv (fn [out key item]
                   (when-not (keyword? key)
                     (v/fail! :json-value :key "Use keyword map keys."))
                   (let [key (keyword (str "k" (names/text key)))]
                     (when (contains? out key)
                       (v/fail! :json-value :collision "Use distinct encoded map keys."))
                     (assoc out key (json-data item (inc depth)))))
                 {}
                 value)
    (vector? value) (mapv #(json-data % (inc depth)) value)
    (number? value) (v/safe-number! value :json-value)
    (or (nil? value) (string? value) (boolean? value)) value
    :else (v/fail! :json-value
                   :domain
                   "Use CLJS maps/vectors/keywords and JSON scalar values.")))

(defn- cljs-data
  "Rejects malformed tags/key projections before reconstructing CLJS payloads."
  [value depth]
  (when (> depth 62)
    (v/fail! :json-value :limit "Reduce encoded JSON depth to 64 or less."))
  (cond
    (and (map? value) (= (hash-set (keyword "~keyword")) (set (keys value))))
      (keyword (v/check! :string (req! value (keyword "~keyword")) :json-keyword))
    (map? value)
      (reduce-kv
        (fn [out key item]
          (let [key (names/text key)
                _ (v/check! [:string {:min 1}] key :json-map-key)
                type (subs key 0 1)
                text (subs key 1)
                key (case type
                      "k" (keyword text)
                      (v/fail! :json-value :codec "Use recognized map key projections."))]
            (when (contains? out key)
              (v/fail! :json-value :collision "Use distinct map keys."))
            (assoc out key (cljs-data item (inc depth)))))
        {}
        value)
    (vector? value) (mapv #(cljs-data % (inc depth)) value)
    :else value))

(defn- read-wire
  "Retains parser/contract causes while reporting a bounded payload location."
  [text]
  (try (json/decode (json-size! text))
       (catch :default error
         (if (:code (ex-data error))
           (throw error)
           (v/fail! :json-payload :codec
                    "Supply valid bounded JSON data."
                      {:path [:payload] :expected :json :cause error})))))

(defn write-json
  "Serializes CLJS JSON data losslessly using version 2 and a UTF-8 byte budget."
  [value]
  (json-size! (json/encode ["fast-twitch/cljs-json" 2 (json-data value 0)])))

(defn read-json
  "Reads version-2 CLJS JSON text; legacy JSON projects received object keys to keywords."
  [text]
  (let [value (read-wire text)]
    (if (and (vector? value) (= "fast-twitch/cljs-json" (first value)))
      (do (v/check! [:tuple [:= "fast-twitch/cljs-json"] [:= 2] :any] value :json-version)
          (cljs-data (nth value 2) 0))
      value)))

(defn encode
  "Encodes an explicitly selected native/JSON/RPC boundary value; storage callers require a data-only policy."
  [policy value]
  (case (or policy :native)
    :native (native-value! value)
    :rpc (rpc-value! value)
    :json #js {:fastTwitchCodec "json" :version 2 :payload (write-json value)}
    (v/fail! :encode :codec "Choose an operation-supported explicit codec.")))

(defn decode
  "Decodes a checked native value or versioned JSON envelope, retaining legacy v1 reads."
  [policy value]
  (case (or policy :native)
    :native (native-value! value)
    :rpc (rpc-value! value)
    :json (do (when-not (and (some? value)
                             (= "json" (aget value "fastTwitchCodec"))
                             (#{1 2} (aget value "version")))
                (v/fail! :decode :codec "Supply a supported versioned JSON envelope."))
              (if (= 1 (aget value "version"))
                (read-wire (aget value "payload"))
                (let [payload (read-wire (aget value "payload"))]
                  (v/check! [:tuple [:= "fast-twitch/cljs-json"] [:= 2] :any]
                            payload
                            :json-version)
                  (cljs-data (nth payload 2) 0))))
    (v/fail! :decode :codec "Choose an operation-supported explicit codec.")))

(defn persistence-policy!
  "Checks a data-only policy before persistent reads or effects; RPC capabilities cannot be persisted."
  [policy]
  (v/check! [:maybe [:enum :native :json]] policy :persistence-codec))
