(ns hooks.celld.declarations
  "Models declaration bindings without evaluating or expanding application code."
  (:require [clj-kondo.hooks-api :as api]))

(defn workflow
  "Analyzes the always-async run callback while keeping the declaration a class value."
  [{:keys [node]}]
  (let [[_ name options argv & body] (:children node)
        function (assoc (api/token-node 'fn) :meta [(api/keyword-node :async)])]
    ;; The macro always creates an async run callback, while its public value is a
    ;; class.
    {:node (api/list-node
             [(api/token-node 'def) name
              (api/list-node [(api/token-node 'do) options
                              (api/list-node (concat [function argv] body))])])}))

(defn transaction
  "Checks a synchronous callback without allowing outer async metadata to leak in."
  [{:keys [node]}]
  (let [[_ storage & body] (:children node)]
    {:node (api/list-node
             [(api/token-node 'do) storage
              (api/list-node (concat [(api/token-node 'fn) (api/vector-node [])]
                                     body))])}))
