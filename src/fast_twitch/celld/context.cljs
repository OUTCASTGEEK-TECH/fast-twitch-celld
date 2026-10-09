(ns fast-twitch.celld.context
  "Explicit activation and invocation context; native handles never share env caches."
  (:require [fast-twitch.celld.native :as n] [fast-twitch.celld.validation :as v])
  (:refer-global :only [WeakMap]))

(defonce ^:private activations
  (WeakMap.))

(defn activate!
  "Associates one actual instance with ctx/env and establishes initialization gating
  synchronously. The constructor itself returns no Promise."
  ([instance ctx env initializer]
   (activate! instance ctx env initializer :cell))
  ([instance ctx env initializer kind]
   (let [context {:kind kind :native ctx :env env :state (atom {})}]
     (.set activations instance context)
     (when initializer
       (n/invoke ctx
                 "blockConcurrencyWhile"
                 [(fn []
                    (initializer context))]))
     nil)))

(defn of
  "Returns the private activation context for the actual Cell receiver; rejects an unrelated instance synchronously."
  [instance]
  (or (.get activations instance)
      (v/fail! :context :receiver "Invoke the method on its original Cell instance.")))

(defn invocation
  "Adds the current native event to an explicit context value; does not install dynamic/global context."
  [context event]
  (assoc context :event event))

(defn worker
  "Creates invocation context from native Worker env and execution context; no binding is cached."
  [env ctx]
  {:kind :worker :env env :native ctx})

(defn native
  "Returns the original native state/execution context from a checked context map."
  [context]
  (req! context :native))

(defn env
  "Returns this invocation/activation env without serialization."
  [context]
  (req! context :env))

(defn storage
  "Returns the live native Cell storage handle; unavailable in ordinary Workers."
  [context]
  (n/property (native context) "storage"))

(defn id
  "Returns the native Cell ID, retaining optional native name semantics."
  [context]
  (n/property (native context) "id"))

(defn props
  "Returns the native startup properties for this activation."
  [context]
  (n/property (native context) "props"))

(defn exports
  "Returns native loopback export capabilities for this context."
  [context]
  (n/property (native context) "exports"))

(defn facets
  "Returns the native Cell facet registry, scoped to its root object."
  [context]
  (n/property (native context) "facets"))

(defn container
  "Returns the experimental native container handle when configured."
  [context]
  (n/property (native context) "container"))

(defn wait-until!
  "Registers a Promise with the native invocation context, synchronously. Native rejection and lifetime semantics remain intact."
  [context promise]
  (n/invoke (native context) "waitUntil" [promise]))

(defn block-concurrency!
  "Establishes a native input gate immediately and invokes the callback in that gate. It does not make arbitrary awaits atomic."
  [context callback]
  (n/invoke (native context) "blockConcurrencyWhile" [callback]))

(defn abort!
  "Aborts this native Cell activation with the supplied reason; future handle use follows native terminal semantics."
  [context reason]
  (n/invoke (native context) "abort" [reason]))
