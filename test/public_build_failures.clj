(ns public-build-failures
  "Exercises actual public build failures, recovery and publication atomicity."
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [fast-twitch.celld.tooling :as t]))

(def base
  "target/public-build-fixtures")

(def app
  (str base "/app.edn"))

(def output
  (t/path base "out"))

(def good
  "(defcell Good {:binding :GOOD})\n")

(def prelude
  "(ns fixture.invalid (:require-macros [fast-twitch.celld.macros :refer [defcell defrpc deffetch defalarm defcell-init defqueue-handler defworkflow defcontract defwebsocket-handlers]]))\n")

(defn websocket-case
  [kind event variadic?]
  (let [handler (if variadic?
                  "(defn bad [ctx socket event extra & events] nil) "
                  "(defn bad [ctx] nil) ")
        spec (str "{:id :fixture/socket :version 1 :mode :resident :" event " bad}")]
    {:fixture
       (str "websocket-" kind "-" (if variadic? "variadic-required" event) "-arity")
     :source (str handler
                  (if (= kind "focused")
                    (str "(defwebsocket-handlers sockets "
                         spec
                         ") (defcell Good {:binding :GOOD :include [sockets]})")
                    (str "(defcell Good {:binding :GOOD :websocket " spec "})")))
     :extra ""
     :code "arity"}))

(defn cases
  []
  (vec (concat
         (t/read-edn (t/path "test/public_build_cases.edn"))
         (for [kind ["focused" "ordinary"]
               event ["message" "close" "error"]]
           (websocket-case kind event false))
         [{:fixture "oversize-option-redaction"
           :source (str "(deffetch f {:" (str/join (repeat 500 "REVIEW_SENTINEL_"))
                        " true} [ctx request] nil) " good)
           :extra ""
           :code "unknown-key"}]
         (for [kind ["focused" "ordinary"]] (websocket-case kind "message" true)))))

(defn run-build!
  [local source extra]
  (t/write! (t/path base "src/fixture/invalid.cljs")
            (str (when-not (str/starts-with? source "(ns ") prelude) source "\n"))
  (t/write-edn! (t/path app)
                (merge {:name :compile-fixtures
                        :entry 'fixture.invalid
                        :paths [(str base "/src")]
                        :output (str base "/out")
                        :history (str base "/history.edn")}
                       (edn/read-string (str "{" extra "}"))))
  (let [pointer (t/path base "out/wrangler.json")
        before (when (fs/exists? pointer) (t/sha256 pointer))
        result (t/run! (t/clojure-command local "build" "build" app))]
    [result before (when (fs/exists? pointer) (t/sha256 pointer))]))

(defn positive!
  [local source extra name]
  (t/success! (first (run-build! local source extra)))
  (println name "PASS"))

(defn qualify!
  [{:keys [local]}]
  (let [names (some-> (not-empty (System/getenv "CELLD_FIXTURES"))
                      (str/split #",")
                      set)
        cases (cond->> (cases) names (filterv #(names (:fixture %))))
        results (atom [])]
    (t/write-edn! (t/path base "history.edn") [{:tag "v1" :new_sqlite_classes [:Good]}])
    (t/success! (first (run-build! local good "")))
    (doseq [{:keys [fixture source extra code]} cases]
      (let [start (t/now)
            [result before after] (run-build! local source extra)
            diagnostic (:err result)
            passed (and (not (zero? (:exit result)))
                        (str/includes? diagnostic code)
                        (= before after)
                        (not (str/includes? diagnostic "SECRET_SENTINEL_66af"))
                        (not (str/includes? diagnostic "REVIEW_SENTINEL_"))
                        (every? #(str/includes? diagnostic %)
                                [":repair" ":source" ":file" ":expected" ":path"])
                        (str/includes? (or (t/text (t/path base "out/status.edn")) "")
                                       ":stale"))]
        (swap! results conj
          {:fixture fixture
           :passed passed
           :exit (:exit result)
           :diagnostic (t/tail diagnostic 3500)
           :seconds (t/rounded (t/elapsed start))})
        (println fixture (if passed "PASS" "FAIL"))
        (t/write-json! (t/path base "evidence.json") @results)
        (t/ensure! passed (t/tail diagnostic 4000))))
    (t/write-json! (t/path base "evidence.json") @results)
    (positive! local good "" "positive recovery")
    (positive!
      local
      "(defcontract Empty [:cat]) (defcontract Integer :int) (defrpc value {:method :value :args Empty :returns Integer} [ctx] 1) (defcell Good {:binding :GOOD :include [value]})"
      ":config {:queues {:producers [{:binding :JOBS :queue :fixture :delivery_delay 86400}]}}"
      "referenced contract and valid queue")
    (positive!
      local
      "(defn exact [ctx socket event] nil) (defn variadic [ctx socket event & events] nil) (defn fetch [ctx request] nil) (defn adapter [ctx socket event] (.log js/console event)) (defwebsocket-handlers sockets {:id :fixture/socket :version 1 :mode :resident :message exact :close variadic :error adapter}) (defcell Good {:binding :GOOD :include [sockets] :fetch {:handler fetch :native? false}})"
      ""
      "websocket exact/variadic/adapter and boolean positive")
    (positive!
      local
      "(defn exact [ctx socket event] nil) (defn variadic [ctx socket event & events] nil) (defn adapter [ctx socket event] (.log js/console event)) (defcell Good {:binding :GOOD :websocket {:id :fixture/socket :version 1 :mode :resident :message exact :close variadic :error adapter}})"
      ""
      "ordinary websocket exact/variadic/adapter positive")
    (let
      [pointer (t/sha256 (t/path base "out/wrangler.json"))
       clients (into {}
                     (for [p (fs/glob output "generations/*/generated-src/**/*.cljs")]
                       [(str p) (t/sha256 p)]))
       expression
         "(require (quote fast-twitch.celld.build)) (with-redefs [fast-twitch.celld.build/generate-clients! (fn [& _] (throw (ex-info \"client generation failed\" {:code :fast-twitch.celld.build/client-generation :repair \"Correct the client generator.\"})))] (fast-twitch.celld.build/-main \"build\" \"target/public-build-fixtures/app.edn\"))"
       result (t/run! (t/expression-command local expression))]
      (t/ensure! (and (not (zero? (:exit result)))
                      (str/includes? (:err result) "client-generation")
                      (= pointer (t/sha256 (t/path base "out/wrangler.json")))
                      (every? (fn [[p digest]]
                                (= digest (t/sha256 p)))
                              clients))
                 (str "Client-generation failure atomicity: " (:err result)))
      (println "client-generation failure atomicity PASS"))))

(t/main!
  *file*
  {"--local" :flag}
  "bb test/public_build_failures.clj [--local]; CELLD_FIXTURES accepts comma-separated failure case names."
  qualify!)
