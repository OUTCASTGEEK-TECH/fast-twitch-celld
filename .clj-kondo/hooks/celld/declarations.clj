(ns hooks.celld.declarations
  "Models declaration bindings without evaluating or expanding application code."
  (:require [clj-kondo.hooks-api :as api]))

(defn- arity!
  "Reports DSL signature errors at the original macro call location."
  [node valid? message]
  (when-not valid?
    (api/reg-finding! (merge (meta node) {:type :invalid-arity :message message}))))

(defn value
  "Defines the public value and analyzes literal declaration references."
  [{:keys [node]}]
  (arity! node
          (= 3 (count (:children node)))
          "Declaration expects a name and value/options.")
  {:node (api/list-node (cons (api/token-node 'def) (rest (:children node))))})

(defn handler
  "Preserves function metadata, argument bindings and calls through focused declarations."
  [{:keys [node]}]
  (let [[_ name & forms] (:children node)
        [doc forms] (if (api/string-node? (first forms))
                      [(first forms) (rest forms)]
                      [nil forms])
        [options forms] (if (api/map-node? (first forms))
                          [(first forms) (rest forms)]
                          [nil forms])
        definition (api/list-node
                     (concat [(api/token-node 'defn) name] (when doc [doc]) forms))]
    (let [kind (symbol (clojure.core/name (api/sexpr (first (:children node)))))
          expected ({'defcell-init 1
                     'deffetch 2
                     'defalarm 2
                     'defqueue-handler 2
                     'defscheduled-handler 2}
                    kind)
          argv (first forms)]
      (arity! node
              (and (api/vector-node? argv)
                   (or (nil? expected) (= expected (count (:children argv)))))
              "Use the documented native handler argument vector, including context."))
    ;; Options remain ordinary expressions, so contract/handler references are
    ;; checked.
    {:node (api/list-node (concat [(api/token-node 'do)]
                                  (when options [options])
                                  [definition]))}))

(defn workflow
  "Analyzes the always-async run callback while keeping the declaration a class value."
  [{:keys [node]}]
  (let [[_ name options argv & body] (:children node)
        function (assoc (api/token-node 'fn) :meta [(api/keyword-node :async)])]
    (arity! node
            (and (api/vector-node? argv) (= 3 (count (:children argv))))
            "Workflow run expects context, event and step.")
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
    (arity! node (some? storage) "Transaction requires storage.")
    {:node (api/list-node
             [(api/token-node 'do) storage
              (api/list-node (concat [(api/token-node 'fn) (api/vector-node [])]
                                     body))])}))
