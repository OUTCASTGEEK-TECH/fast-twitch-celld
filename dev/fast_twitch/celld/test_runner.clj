(ns fast-twitch.celld.test-runner
  "Runs portable contract tests, release CLJS tests and the public build-failure suite."
  (:require [cljs.build.api :as cljs]
            [clojure.test :as t]
            [clojure.java.shell :as shell]
            [fast-twitch.celld.definition :as definition]
            [fast-twitch.celld.contracts :as contracts]))

(t/deftest portable-contracts
  (t/is (contracts/options-valid? :queue-send {:delaySeconds 86400}))
  (t/is (not (contracts/options-valid? :queue-send {:delaySeconds 86401})))
  (t/is (not (contracts/options-valid? :queue-send {:typo true})))
  (t/is (contracts/options-valid? :storage-put {:codec :json}))
  (t/is (not (contracts/options-valid? :storage-put {:codec :edn})))
  (t/is (= [:cat :nil :boolean :int]
           (definition/schema! [:cat :nil :boolean :int] nil 'sample []))))

(t/deftest bounded-static-option-path
  (let [key (keyword (apply str (repeat 500 "REVIEW_SENTINEL_")))]
    (try (definition/closed! :fetch {key true} {:file "fixture.cljs"} 'fetch)
         (t/is false)
         (catch clojure.lang.ExceptionInfo error
           (t/is (= [:redacted-segment] (:path (ex-data error))))
           (t/is (< (count (pr-str (ex-data error))) 1024))
           (t/is (< (count (.getMessage error)) 256))
           (t/is (not (.contains (pr-str (ex-data error)) "REVIEW_SENTINEL_")))))))

(defn -main
  [& _args]
  (let [result (t/run-tests 'fast-twitch.celld.test-runner)]
    (when (pos? (+ (:fail result) (:error result))) (System/exit 1)))
  (cljs/build "test"
              {:main 'fast-twitch.celld.test-main
               :target :nodejs
               :optimizations :simple
               :output-to "target/tests.cjs"
               :output-dir "target/test-cljs"
               :infer-externs true})
  (let [result (shell/sh "node" "target/tests.cjs")]
    (print (:out result))
    (flush)
    (binding [*out* *err*]
      (print (:err result))
      (flush))
    (when-not (zero? (:exit result)) (System/exit (:exit result))))
  (shutdown-agents))
