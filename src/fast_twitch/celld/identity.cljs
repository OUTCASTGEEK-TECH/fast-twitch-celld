(ns fast-twitch.celld.identity
  "Native Cell namespace/ID operations; lookup remains lazy and namespace scoped."
  (:require [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names]))

(defn id-from-name
  "Derives a native namespace-scoped ID from the full keyword/string name, synchronously."
  [namespace value]
  (n/invoke namespace "idFromName" [(v/check! :string (names/text value) :id-from-name)]))

(defn id-from-string
  "Validates an opaque native ID string in this namespace, synchronously."
  [namespace value]
  (n/invoke namespace "idFromString" [(v/check! :string value :id-from-string)]))

(defn new-unique-id
  "Creates a random native ID synchronously; jurisdiction restrictions are unsupported by Celld."
  [namespace]
  (n/invoke namespace "newUniqueId" []))

(defn get-stub
  "Returns a lazy native stub for an ID. This call does not activate the Cell."
  ([namespace id]
   (n/invoke namespace "get" [id]))
  ([namespace id options]
   (n/invoke namespace "get" [id (n/options options #{:locationHint} :get-stub)])))

(defn get-by-name
  "Returns a lazy stub for a native name (keyword namespaces are retained); native ID derivation remains the default."
  ([namespace value]
   (n/invoke namespace "getByName" [(v/check! :string (names/text value) :get-by-name)]))
  ([namespace value options]
   (n/invoke namespace
             "getByName"
             [(v/check! :string (names/text value) :get-by-name)
              (n/options options #{:locationHint} :get-by-name)])))

(defn id-string
  "Returns the complete opaque native ID string synchronously."
  [id]
  (n/invoke id "toString" []))

(defn id-name
  "Returns the optional native name; long names may be absent even when routing succeeds."
  [id]
  (n/property id "name"))

(defn equals?
  "Compares native IDs through their original receiver."
  [id other]
  (n/invoke id "equals" [other]))

(defn stub-id
  "Returns the live native ID associated with a namespace stub."
  [stub]
  (n/property stub "id"))
