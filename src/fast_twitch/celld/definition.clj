(ns fast-twitch.celld.definition
  "JVM declaration rules shared by macros, application builds and tooling."
  (:require [malli.core :as m]
            [fast-twitch.celld.contracts :as contracts]
            [fast-twitch.celld.names :as names]))

(def ^:dynamic *source*
  "Known build input location when no declaration source is available."
  nil)

(def reserved-members
  #{"constructor" "then" "__proto__" "prototype" "toString" "valueOf" "hasOwnProperty"})

(def cell-events
  #{"fetch" "alarm" "webSocketMessage" "webSocketClose" "webSocketError"})

(defn fail!
  "Throws a bounded, source-aware diagnostic. Payloads are never included."
  ([code source declaration path expected received repair]
   (fail! code source declaration path expected received repair {}))
  ([code source declaration path expected received repair details]
   (let [path (contracts/safe-path path)]
     (throw (ex-info (str (name code) " at " path ". " repair)
                     (merge {:code (keyword "fast-twitch.celld.definition" (name code))
                             :source (or source *source*)
                             :declaration declaration
                             :path path
                             :expected expected
                             :received {:type (str (type received))
                                        :count (when (coll? received) (count received))}
                             :repair repair}
                            (select-keys details [:sources :native-event])))))))

(defn check!
  "Checks immutable static schemas during macro expansion/build without running application code."
  [schema value source declaration path]
  (when-not ((contracts/validator schema) value)
    (let [issue (contracts/issue schema value)]
      (fail! (cond
               (= :malli.core/extra-key (:type issue)) :unknown-key
               (some #{:codec} (:path issue)) :codec
               (= :binding (first (:path issue))) :binding
               (= :export (first (:path issue))) :native-name
               (#{:version :mode :id} (first (:path issue))) :websocket-dispatch
               :else :option-value)
             source
             declaration
             (into path (:path issue))
             (:expected issue)
             value
             "Use literal values matching the documented static option contract.")))
  value)

(defn closed!
  "Validates closed literal declaration shapes/types during expansion."
  [kind options source declaration]
  (when-not (map? options)
    (fail! :literal-map source declaration [] :map options "Use a literal options map."))
  (check! (get contracts/declarations kind) options source declaration []))

(defn native-name!
  "Checks a stable non-reserved native JavaScript identifier at its declaration path."
  [value source declaration path]
  (let [value (names/identifier value)]
    (when-not (and (string? value)
                   (re-matches #"[A-Za-z_$][A-Za-z0-9_$]*" value)
                   (not (contains? reserved-members value)))
      (fail! :native-name
             source
             declaration
             path
             :native-identifier
             value
             "Use a stable, non-reserved JavaScript identifier."))
    value))

(defn schema!
  "Compiles a portable literal schema now, rather than postponing errors to calls."
  [schema source declaration path]
  (try (m/schema schema)
       (catch Exception
         _
         (fail! :schema
                source
                declaration
                path
                :portable-malli-schema
                schema
                "Use a valid portable Malli schema.")))
  schema)
