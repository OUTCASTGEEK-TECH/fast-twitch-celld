(ns fast-twitch.celld.native
  "Receiver-preserving interop. Optional arguments are omitted by wrapper arity."
  (:require [malli.experimental :as mx]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names])
  (:refer-global :only [Object Reflect]))

(defn invoke
  "Invokes once with the original receiver and exact argument count; preserves native sync/Promise result and rejection."
  [receiver member args]
  (Reflect.apply (v/method! receiver member) receiver (to-array args)))

(defn property
  "Reads a live native property synchronously without cloning; getter failures propagate."
  [receiver member]
  (when (nil? receiver) (v/fail! member :capability "Supply a native handle."))
  (aget receiver (names/text member)))

(mx/defn ^{:dynamic true :no-doc true} key-name
  "Internal projection for keyword KV keys; full namespace spelling is native text."
  [key :- :keyword _operation]
  (names/text key))

(mx/defn ^:dynamic fields
  "Projects keyword field names losslessly; rejects native-key collisions and preserves values/handles."
  [value :- [:map-of :keyword :any]]
  (let [out (Object.create nil)]
    (doseq [[key item] value]
      (let [key (names/text key)]
        (when (Object.hasOwn out key)
          (v/fail! :native-field :collision
                   "Use distinct native field projections." {:path [key]}))
        (aset out key item)))
    out))

(defn ^:no-doc option-fields
  "Projects validated option fields/selectors while preserving native resources."
  [value]
  (fields (reduce-kv (fn [out key item]
                       (assoc out
                         key (cond
                               (and (#{:env :labels :retention} key) (map? item))
                                 (fields
                                   item)
                               (and (= :entrypoint key) (vector? item)) (to-array item)
                               (keyword? item) (names/text item)
                               :else item)))
                     {}
                     value)))

(defn data-map
  "Shallow keyword projection preserving native handles; accepts a shared keyword selector for encoded aliases."
  ([value]
   (data-map value keyword))
  ([value key-fn]
   (when (some? value)
     (reduce (fn [out key]
               (let [projected (key-fn key)]
                 (when (contains? out projected)
                   (v/fail! :native-data
                            :collision
                            "Use distinct projected field identities."))
                 (assoc out projected (aget value key))))
       {}
       (array-seq (Object.keys value))))))

(v/instrument! key-name fields)
