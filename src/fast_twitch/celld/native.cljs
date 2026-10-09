(ns fast-twitch.celld.native
  "Receiver-preserving interop. Optional arguments are omitted by wrapper arity."
  (:require [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.contracts :as contracts]
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

(defn ^:no-doc key-name
  "Internal projection for keyword KV keys; full namespace spelling is native text."
  [key operation]
  (names/text (v/check! :keyword key operation)))

(defn fields
  "Projects keyword field names losslessly; rejects native-key collisions and preserves values/handles."
  [value]
  (v/check! [:map-of :keyword :any] value :native-field)
  (let [out (Object.create nil)]
    (doseq [[key item] value]
      (let [key (names/text key)]
        (when (Object.hasOwn out key)
          (v/fail! :native-field :collision
                   "Use distinct native field projections." {:path [key]}))
        (aset out key item)))
    out))

(defn options
  "Checks the closed operation options before projecting selectors/fields; resources remain native."
  [value allowed operation]
  (v/check! (or (get contracts/schemas operation)
                (contracts/closed (zipmap allowed (repeat :any))))
            (if (map? value) (contracts/option-values operation value) value)
            operation)
  (fields (reduce-kv (fn [out key item]
                       (assoc out
                         key (cond
                               (and (#{:env :labels :retention} key) (map? item)) (fields
                                                                                    item)
                               (and (= :entrypoint key) (vector? item)) (to-array item)
                               (keyword? item) (names/text item)
                               :else item)))
                     {}
                     (contracts/option-values operation value))))

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
