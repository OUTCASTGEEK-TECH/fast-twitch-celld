(ns fast-twitch.celld.test-runner
  "Runs portable contract tests, release CLJS tests and the public build-failure suite."
  (:require [malli.core :as m]
            [cljs.build.api :as cljs]
            [clojure.test :as t]
            [clojure.java.shell :as shell]
            [fast-twitch.celld.config :as config]
            [fast-twitch.celld.definition :as definition]
            [fast-twitch.celld.contracts :as contracts]))

(t/deftest portable-contracts
  (t/is (m/validate (:queue-send contracts/schemas) {:delaySeconds 86400}))
  (t/is (not (m/validate (:queue-send contracts/schemas) {:delaySeconds 86401})))
  (t/is (not (m/validate (:queue-send contracts/schemas) {:typo true})))
  (t/is (m/validate (:storage-put contracts/schemas) {:codec :json}))
  (t/is (not (m/validate (:storage-put contracts/schemas) {:codec :edn})))
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

(t/deftest configuration-boundaries
  (let [valid {:queues {:producers
                          [{:binding "JOBS" :queue "jobs" :delivery_delay 86400}]}
               :rules [{:type "Text" :globs ["**/*.txt"]}]}]
    (t/is (= valid (config/validate! valid [])))
    (doseq [invalid [{:queues {:producers [{:queue "jobs"}]}}
                     {:queues {:producers
                                 [{:binding "JOBS" :queue "jobs" :delivery_delay 86401}]}}
                     {:queues {:consumers [{:queue "jobs"}]}} {:rules [{:type "Text"}]}
                     {:rules [{:globs ["*.txt"]}]} {:rules [{:type "Text" :globs []}]}
                     {:rules [{:type "Text" :globs ["*.wasm"]}]}
                     {:services [{:binding "SHARED" :service "worker"}]
                      :vars {:SHARED "collision"}}]]
      (t/is (thrown? clojure.lang.ExceptionInfo (config/validate! invalid []))))))

(def native-test-modules
  "Node stubs installed before compiled native module aliases initialize."
  "globalThis['cloudflare:workers']={WorkerEntrypoint:class{},WorkflowEntrypoint:class{}};
   globalThis['cloudflare:sockets']={};
   globalThis['cloudflare:workflows']={NonRetryableError:class extends Error{}};
   require(require('node:path').resolve(process.argv[1]));")

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
  (let [result (shell/sh "node" "-e" native-test-modules "target/tests.cjs")]
    (print (:out result))
    (flush)
    (binding [*out* *err*]
      (print (:err result))
      (flush))
    (when-not (zero? (:exit result)) (System/exit (:exit result))))
  (shutdown-agents))
