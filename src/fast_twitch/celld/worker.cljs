(ns fast-twitch.celld.worker
  "Native class relationships for named Workers and Workflows only."
  (:require [fast-twitch.celld.context :as context])
  (:require-global ["cloudflare:workers" :as workers])
  (:refer-global :only [Object Reflect]))

(defn- constructor
  [base kind]
  (let [ctor (fn [ctx env]
               (this-as this
                        (let [native (Reflect.construct base
                                                        #js [ctx env]
                                                        (aget this "constructor"))]
                          (context/activate! native ctx env nil kind)
                          native)))]
    (set! (.-prototype ctor) (Object.create (.-prototype base)))
    (aset (.-prototype ctor) "constructor" ctor)
    (Object.setPrototypeOf ctor base)
    ctor))

(defn named-constructor
  "Returns the actual native WorkerEntrypoint-derived CLJS constructor; no separate Cell instance is created."
  []
  (constructor workers/WorkerEntrypoint :named-worker))

(defn workflow-constructor
  "Returns the actual WorkflowEntrypoint-derived CLJS constructor with receiver-associated context."
  []
  (constructor workers/WorkflowEntrypoint :workflow))
