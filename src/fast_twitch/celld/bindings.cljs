(ns fast-twitch.celld.bindings
  "Bindings remain native capabilities scoped to the supplied context."
  (:require [fast-twitch.celld.context :as context]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names]
            [fast-twitch.celld.contracts :as contracts]))

(defn get-binding
  "Returns the named native env capability or fails before effects when missing. Keyword identities retain namespaces through native identifier projection."
  [context binding]
  (v/check! contracts/identity-value binding :binding {:binding binding})
  (let [value (aget (context/env context) (names/identifier binding))]
    (when (nil? value)
      (v/fail! :binding :env
               "Declare and supply the required binding." {:binding binding}))
    value))

(defn native
  "Returns the same native binding capability without conversion."
  [context binding]
  (get-binding context binding))
