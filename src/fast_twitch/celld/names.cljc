(ns fast-twitch.celld.names
  "Shared keyword projection at native name/identifier boundaries. Namespace is retained."
  (:require [clojure.string :as str])
  #?(:cljs (:refer-global :only [String parseInt])))

(defn text
  "Projects a keyword's full spelling; opaque native strings remain unchanged."
  [value]
  (if (keyword? value) (subs (str value) 1) value))

(defn identifier
  "Projects keyword identities to legal JS identifiers. Namespaced/reserved-prefix
  keywords use injective UTF-16 hex; plain keywords keep their familiar spelling.
  Explicit native strings retain their spelling and collisions are checked by owners."
  [value]
  (if (and (keyword? value)
           (or (namespace value) (str/starts-with? (name value) "__ft_")))
    (let [s (text value)]
      (str "__ft_"
           (apply str
             (for [i (range (count s))]
               #?(:clj (format "%04x" (int (.charAt ^String s i)))
                  :cljs (.padStart (.toString (.charCodeAt s i) 16) 4 "0"))))))
    (text value)))

(defn selector
  "Restores an owned native selector; opaque native IDs never use this conversion."
  [value]
  (keyword (if (and (string? value) (re-matches #"__ft_(?:[0-9a-f]{4})+" value))
             (apply str
               (map #?(:clj (fn [digits]
                              (char (Integer/parseInt digits 16)))
                       :cljs (fn [digits]
                               (.fromCharCode String (parseInt digits 16))))
                 (re-seq #"...." (subs value 5))))
             value)))
