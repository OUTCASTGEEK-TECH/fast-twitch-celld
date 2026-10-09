(ns fast-twitch.celld.bindings
  "Bindings remain native capabilities scoped to the supplied context."
  (:require [malli.experimental :as mx]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names]
            [fast-twitch.celld.contracts :as contracts]))

(mx/defn ^:dynamic get-binding
  "Returns the named native env capability or fails before effects when missing. Keyword identities retain namespaces through native identifier projection."
  [context binding :- contracts/identity-value]
  (let [value (aget (context/env context) (names/identifier binding))]
    (when (nil? value)
      (v/fail! :binding :env
               "Declare and supply the required binding." {:binding binding}))
    value))

(defn native
  "Returns the same native binding capability without conversion."
  [context binding]
  (get-binding context binding))

(set! get-binding
      (v/instrument 'fast-twitch.celld.bindings/get-binding
                    get-binding
                    #(hash-map :binding (nth % 1 nil))))
