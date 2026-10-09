(ns fast-twitch.celld.identity
  "Native Cell namespace/ID operations; lookup remains lazy and namespace scoped."
  (:require [fast-twitch.celld.contracts :as contracts]
            [malli.experimental :as mx]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names]))

(mx/defn ^:dynamic id-from-name
  "Derives a native namespace-scoped ID from the full keyword/string name, synchronously."
  [namespace value :- [:or :keyword :string]]
  (n/invoke namespace "idFromName" [(names/text value)]))

(mx/defn ^:dynamic id-from-string
  "Validates an opaque native ID string in this namespace, synchronously."
  [namespace value :- :string]
  (n/invoke namespace "idFromString" [value]))

(defn new-unique-id
  "Creates a random native ID synchronously; jurisdiction restrictions are unsupported by Celld."
  [namespace]
  (n/invoke namespace "newUniqueId" []))

(mx/defn ^{:dynamic true} get-stub
  "Returns a lazy native stub for an ID. This call does not activate the Cell."
  ([namespace id]
   (n/invoke namespace "get" [id]))
  ([namespace id options :- (:get-stub contracts/schemas)]
   (n/invoke namespace "get" [id (n/option-fields options)])))

(mx/defn ^{:dynamic true} get-by-name
  "Returns a lazy stub for a native name (keyword namespaces are retained); native ID derivation remains the default."
  ([namespace value :- [:or :keyword :string]]
   (n/invoke namespace "getByName" [(names/text value)]))
  ([namespace value :- [:or :keyword :string] options :- (:get-by-name contracts/schemas)]
   (n/invoke namespace "getByName" [(names/text value) (n/option-fields options)])))

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

(v/instrument! id-from-name id-from-string get-stub get-by-name)
