(ns fast-twitch.celld.validation
  "Release-enabled data and native capability guards. Checks precede effects."
  (:require-macros [fast-twitch.celld.validation])
  (:require [malli.core :as m]
            [fast-twitch.celld.contracts :as contracts]
            [fast-twitch.celld.names :as names])
  (:refer-global :only [Number Object]))

(defn- diagnostic-details
  [details]
  (reduce (fn [out key]
            (if (contains? out key)
              (update out
                      key
                      #(if (and (keyword? %) (<= (count (str %)) 128))
                         %
                         {:type (keyword (goog/typeOf %))}))
              out))
    (cond-> (select-keys details
                         [:path :schema-path :expected :received :method :binding])
      (contains? details :path) (update :path contracts/safe-path)
      (contains? details :schema-path) (update :schema-path contracts/safe-path))
    [:method :binding]))

(defn fail!
  "Throws a bounded structured diagnostic with known identity and optional native cause."
  ([operation boundary repair]
   (fail! operation boundary repair {}))
  ([operation boundary repair details]
   (throw (ex-info repair
                   (merge {:code :fast-twitch.celld/invalid-boundary
                           :operation operation
                           :boundary boundary
                           :path []
                           :expected :valid-boundary
                           :repair repair}
                          (diagnostic-details details))
                   (:cause details)))))

(defn at!
  "Adds a known boundary identity to synchronous contract failures, retaining their cause.
  Foreign/native exceptions propagate unchanged."
  [details f]
  (try (f)
       (catch :default error
         (let [data (ex-data error)
               code (:code data)]
           (if (and (keyword? code)
                    (some-> (namespace code)
                            (.startsWith "fast-twitch")))
             (throw (ex-info (.-message error)
                             (merge data
                                    (select-keys (diagnostic-details details)
                                                 [:method :binding]))
                             error))
             (throw error))))))

(defn ^:no-doc instrument
  "Installs a registered mx/defn contract in release output with bounded diagnostics."
  [operation f & [details]]
  (m/-instrument
    (assoc (get-in (m/function-schemas :cljs)
                   [(symbol (namespace operation)) (symbol (name operation))])
      :report (fn [_ {:keys [input output args value]}]
                (fail! operation
                       :data
                       "Supply a value matching the function contract."
                       (merge (when details (details args))
                              (when-let [schema (or input output)]
                                (dissoc (contracts/issue schema (if input args value))
                                  :type))))))
    ;; Invoke captured variadic dispatch instead of the instrumented public var.
    (if (.-cljs$lang$applyTo f)
      (fn [& args]
        (.call (.-cljs$lang$applyTo f) f args))
      f)))

(defn check!
  "Checks release-time data before effects and reports a bounded schema location/identity."
  ([schema value operation]
   (check! schema value operation {}))
  ([schema value operation details]
   (when-not ((try
                (contracts/validator schema)
                (catch :default _
                  (fail! operation :schema "Supply a valid portable contract." details)))
               value)
     (fail! operation
            :data
            "Supply a value matching the declared contract."
            (merge details
                   (dissoc (contracts/issue schema value) :type)
                   {:received {:type (cond (map? value) :map
                                           (vector? value) :vector
                                           (keyword? value) :keyword
                                           :else (keyword (goog/typeOf value)))
                               :count (when (coll? value) (count value))}})))
   value))

(defn method!
  "Checks a live callable member without invoking it; callers must retain its receiver."
  [receiver member]
  (let [identity member
        member (names/identifier member)
        f (when (some? receiver) (aget receiver member))]
    (when-not (fn? f)
      (fail! :native-method :capability
             "Supply the native receiver supporting this method."
               {:method (if (string? identity) (keyword identity) identity)}))
    f))

(defn thenable?
  "Detects Promise-compatible returns at synchronous callback boundaries."
  [value]
  (and (some? value) (fn? (aget value "then"))))

(defn synchronous!
  "Rejects thenables inside the native synchronous callback before successful commit."
  [value operation]
  (when (thenable? value)
    (fail! operation :synchronous "Use a synchronous callback; thenables cannot commit."))
  value)

(defn safe-number!
  "Rejects nonfinite numbers and integers outside the exact native number range."
  [value operation]
  (when (and (number? value)
             (or (not (Number.isFinite value))
                 (and (integer? value) (not (Number.isSafeInteger value)))))
    (fail! operation :number "Use finite safe numbers or an explicit string identifier."))
  value)
