(ns dev-qualification
  "Qualifies macro reload, failed-build retention, dotenv removal and safe clean."
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
            [fast-twitch.celld.tooling :as t]))

(def base
  "target/dev-qualification")

(def app
  (str base "/app.edn"))

(def output
  (t/path base ".celld-build"))

(def macro-path
  (t/path base "src/fixture/reload.clj"))

(def macro-source
  "(ns fixture.reload)\n(defmacro message [] \"Hello from CLJS\")\n")

(def app-data
  {:name :dev-qualification
   :entry 'example.app
   :paths [(str base "/src")]
   :output (str base "/.celld-build")
   :history (str base "/history.edn")})

(def source
  "(ns example.app (:require [fast-twitch.celld.bindings :as bindings]) (:require-macros [fixture.reload :refer [message]] [fast-twitch.celld.macros :refer [deffetch defworker]]))\n(deffetch hello [ctx request] {:status 200 :body (if (= \"/vars\" (:uri request)) (bindings/get-binding ctx :FIXTURE_MODE) (message))})\n(defworker App {:include [hello]})\n")

(defn qualify!
  [{:keys [local]}]
  (t/write! macro-path macro-source)
  (t/write-edn! (t/path app) app-data)
  (t/write! (t/path base "history.edn") "[]\n")
  (t/write! (t/path base "src/example/app.cljs") source)
  (t/write! (t/path base ".dev.vars") "FIXTURE_MODE=override\n")
  (t/write! (t/path base ".env") "FIXTURE_MODE=fallback\n")
  (let [log (t/path "target/dev-qualification.log")
        process (t/logged! (t/clojure-command local
                                              "dev"
                                              "dev" app
                                              "--port" "19009"
                                              "--host" "127.0.0.1")
                           log
                           {})
        results (atom [])
        response #(t/response "http://127.0.0.1:19009" %)
        status #(or (t/text (str (fs/path output "status.edn"))) "")
        pointer (str (fs/path output "wrangler.json"))
        state (str (fs/path output ".celld/dev/qualification-preserve"))
        wait! (fn [predicate name]
                (t/wait-for! predicate (str name "; inspect " log) 150 process)
                (t/passed! results name))]
    (try
      (wait! #(and (= "Hello from CLJS" (response "/")) (= "override" (response "/vars")))
             "initial-and-dotenv")
      (let [before (t/sha256 pointer)]
        (t/write! state "owned-fixture-state")
        (t/write!
          macro-path
          "(ns fixture.reload)\n(defmacro message [] (throw (ex-info \"Fixture macro changed\" {:code :fast-twitch.celld.fixture/macro-edit-fixture :repair \"Restore the fixture macro\"})))\n")
        (wait! #(str/includes? (status) "macro-edit-fixture") "macro-edit-reloaded")
        (t/ensure! (and (= before (t/sha256 pointer))
                        (= "Hello from CLJS" (response "/")))
                   "Macro failure changed the running generation")
        (t/passed! results "macro-failure-retains-running-generation"))
      (t/write! macro-path macro-source)
      (wait! #(and (str/includes? (status) ":current")
                   (= "Hello from CLJS" (response "/")))
             "macro-recovery")
      (let [before (t/sha256 pointer)]
        (t/write! (t/path app) "{:name")
        (wait! #(str/includes? (status) ":stale") "malformed-app-stale")
        (t/ensure! (and (= before (t/sha256 pointer))
                        (= "Hello from CLJS" (response "/")))
                   "Malformed app changed the running generation"))
      (t/write-edn! (t/path app) app-data)
      (wait! #(and (str/includes? (status) ":current")
                   (= "Hello from CLJS" (response "/")))
             "app-recovery")
      (fs/delete (t/path base ".dev.vars"))
      (wait! #(and (= "fallback" (response "/vars"))
                   (not (fs/exists? (str (fs/path output ".dev.vars")))))
             "dotenv-removal-fallback")
      (t/ensure! (= "owned-fixture-state" (slurp state)) "Dev changed owned state")
      (finally
        (try (t/stop! process 20000)
             (finally (t/write! macro-path macro-source)
                      (t/write-edn! (t/path app) app-data)))))
    (t/success! (t/run! (t/clojure-command local "dev" "clean" app)))
    (t/ensure! (and (= "owned-fixture-state" (slurp state))
                    (= "[]\n" (slurp (t/path base "history.edn")))
                    (fs/regular-file? (str (fs/path output ".env")))
                    (not (fs/exists? (str (fs/path output "generations"))))
                    (not (fs/exists? pointer)))
               "Clean did not preserve only native state, history and dotenv")
    (t/passed! results "clean-preserves-native-state-history-dotenv")
    (t/write-json! (t/path base "evidence.json") @results)))

(t/main! *file* {"--local" :flag} "bb test/dev_qualification.clj [--local]" qualify!)
