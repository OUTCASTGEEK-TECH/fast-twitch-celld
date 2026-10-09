(ns fast-twitch.celld.lint-fixtures
  "Checks that declaration hooks retain meaningful binding and arity diagnostics."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]))

(defn verify!
  "Lints isolated positive and negative fixtures without adding them to maintained source."
  []
  (let
    [prefix " (:require-macros [fast-twitch.celld.macros :as macros]))\n"
     cases
       [["modern-handler"
         "(defrpc ^:async sample [_ctx input] (let [{:keys! [value]} input] (await (js/Promise.resolve (req! value :nested))))) (sample nil {})"
         #{}]
        ["handler-call-arity" "(defrpc sample [_ctx value] value) (sample nil)"
         #{:invalid-arity}]
        ["handler-vector-arity" "(deffetch sample [_ctx] nil)" #{:invalid-arity}]
        ["declaration-arity" "(defcell Sample {} {})" #{:invalid-arity}]
        ["unresolved-body" "(defrpc sample [_ctx] missing-value)" #{:unresolved-symbol}]
        ["unresolved-options" "(defcell Sample {:include [missing-handler]})"
         #{:unresolved-symbol}]
        ["await-requires-async" "(defrpc sample [_ctx] (await (js/Promise.resolve nil)))"
         #{:await-without-async-fn}]
        ["checked-key-binding"
         "(defrpc sample [_ctx input] (let [{:keys! [value]} input] missing-value))"
         #{:unresolved-symbol :unused-binding}]
        ["workflow-async"
         "(defworkflow Sample {} [_ctx event _step] (await (js/Promise.resolve event)))"
         #{}]
        ["sync-transaction-await"
         "(with-transaction-sync nil (await (js/Promise.resolve nil)))"
         #{:await-without-async-fn}]]]
    (doseq [[name source expected] cases]
      (let [file (io/file "target/lint-fixtures/fixture"
                          (str (str/replace name "-" "_") ".cljs"))]
        (io/make-parents file)
        (spit file
              (str "(ns fixture."
                   name
                   prefix
                   (str/replace
                     source
                     #"\b(defrpc|deffetch|defcell|defworkflow|with-transaction-sync)\b"
                     "macros/$1")
                   "\n"))
        (let [{:keys [out err exit]} (shell/sh "clj-kondo" "--repro"
                                               "--cache-dir" "target/clj-kondo-cache"
                                               "--lint" (str file)
                                               "--config" "{:output {:format :edn}}")
              actual (set (map :type (:findings (edn/read-string out))))]
          (when-not (and (= expected actual)
                         (empty? err)
                         (= (empty? expected) (zero? exit)))
            (throw (ex-info
                     "Lint fixture failed"
                     {:fixture name :expected expected :actual actual :stderr err}))))))
    (println (str "Declaration lint fixtures passed: " (count cases)))))
