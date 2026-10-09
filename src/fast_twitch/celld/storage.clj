(ns fast-twitch.celld.storage
  "Paired storage macro; synchronous transaction bodies stay in their native callback."
  (:require [cljs.analyzer :as analyzer]
            [fast-twitch.celld.definition :as d]))

(defmacro with-transaction-sync
  "Evaluates storage once and rejects thenables inside its native sync callback."
  [storage & body]
  (when (some #{'await 'cljs.core/await :async} (tree-seq coll? seq body))
    (d/fail! :async-sync
             (assoc (select-keys (meta &form) [:line :column])
               :file (or (:file &env) analyzer/*cljs-file*))
             'fast-twitch.celld.storage/with-transaction-sync
             []
             :synchronous-body
             body
             "Use transaction! for asynchronous work."))
  `(let [storage# ~storage]
     (fast-twitch.celld.storage/transaction-sync! storage#
                                                  (fn []
                                                    ~@body))))
