(ns fast-twitch.celld.core
  "Small explicit-context entry surface. Import service namespaces only when used."
  (:refer-clojure :exclude [binding])
  (:require [fast-twitch.celld.context :as context]
            [fast-twitch.celld.bindings :as bindings]))

(defn native-context
  "Returns the actual native state/execution context synchronously."
  [ctx]
  (context/native ctx))

(defn binding
  "Checks and returns this explicit context's native binding without caching it."
  [ctx name]
  (bindings/get-binding ctx name))
