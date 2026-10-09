(ns dependency-qualification
  "Verifies dev re-resolves fixture-owned dependencies without changing production pins."
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
            [fast-twitch.celld.tooling :as t]))

(defn dependency-data
  [version extra-alias]
  (let [dependency {'fixture/dependency {:local/root version}}
        main {:paths ["src"]
              :deps {'fast-twitch/celld {:local/root (str t/root)}}
              :aliases {:local {:override-deps {'fast-twitch/fast-twitch
                                                  {:local/root (t/path
                                                                 "../fast-twitch")}}}
                        :dev {:extra-paths [(t/path "dev")]
                              :main-opts ["-m" "fast-twitch.celld.dev"]}
                        :build {:extra-paths [(t/path "dev")]
                                :main-opts ["-m" "fast-twitch.celld.build"]}}}]
    (if extra-alias
      (assoc-in main [:aliases :extra :extra-deps] dependency)
      (update main :deps merge dependency))))

(defn qualify!
  [{:keys [extra-alias]}]
  (let [base (t/path (if extra-alias
                       "target/dependency-extra-qualification"
                       "target/dependency-qualification"))
        output (str (fs/path base ".celld-build"))
        file (str (fs/path base "deps.edn"))
        port (if extra-alias "19010" "19008")]
    (doseq [[version value] [["version-a" "A"] ["version-b" "B"]]]
      (t/write-edn! (str (fs/path base version "deps.edn")) {:paths ["src"]})
      (t/write! (str (fs/path base version "src/fixture/dep.cljs"))
                (str "(ns fixture.dep)\n(def value " (pr-str value) ")\n")))
    (t/write!
      (str (fs/path base "src/fixture/app.cljs"))
      "(ns fixture.app (:require [fixture.dep :as dep]) (:require-macros [fast-twitch.celld.macros :refer [deffetch defworker]]))\n(deffetch fetch [ctx request] {:status 200 :body dep/value})\n(defworker App {:include [fetch]})\n")
    (t/write-edn! (str (fs/path base "app.edn"))
                  {:name :dependency-qualification
                   :entry 'fixture.app
                   :paths ["src"]
                   :output ".celld-build"
                   :history "history.edn"})
    (t/write! (str (fs/path base "history.edn")) "[]")
    (when-not (fs/exists? (fs/path base "node_modules"))
      (fs/create-sym-link (fs/path base "node_modules") (t/path "node_modules")))
    (t/write-edn! file (dependency-data "version-a" extra-alias))
    (let [log (str (fs/path base "dev.log"))
          command [(t/path "bin/celld-clojure")
                   (if extra-alias "-M:local:extra:dev" "-M:local:dev") "dev" "app.edn"
                   "--host" "127.0.0.1" "--port" port]
          process (t/logged! command
                             log
                             {:dir base :extra-env {"CELLD_BIN" (t/path ".tools/celld")}})
          results (atom [])
          response #(t/response (str "http://127.0.0.1:" port) "/")
          status #(or (t/text (str (fs/path output "status.edn"))) "")
          pointer (str (fs/path output "wrangler.json"))
          wait! (fn [predicate name]
                  (t/wait-for! predicate (str name "; inspect " log) 180 process)
                  (t/passed! results name))]
      (try
        (wait! #(= "A" (response)) "initial-local-dependency")
        (t/write-edn! file (dependency-data "version-b" extra-alias))
        (wait! #(= "B" (response)) "dependency-root-change-resolved")
        (t/write! (str (fs/path base "version-b/src/fixture/dep.cljs"))
                  "(ns fixture.dep)\n(def value \"B2\")\n")
        (wait! #(= "B2" (response)) "new-local-source-watched")
        (let [before (t/sha256 pointer)]
          (t/write-edn! file (dependency-data "missing-version" extra-alias))
          (wait! #(str/includes? (status) ":stale") "dependency-failure-stale")
          (let [body (response)
                after (t/sha256 pointer)]
            (t/ensure! (and (= "B2" body) (= before after))
                       (str
                         "Dependency failure changed the running generation: "
                         {:response body :pointer-before before :pointer-after after}))))
        (t/write-edn! file (dependency-data "version-b" extra-alias))
        (wait! #(and (= "B2" (response)) (str/includes? (status) ":current"))
               "dependency-recovery")
        (finally
          (try (t/stop! process 20000)
               (finally (t/write-edn! file (dependency-data "version-b" extra-alias))))))
      (t/write-json! (str (fs/path base "evidence.json")) @results))))

(t/main! *file*
         {"--extra-alias" :flag}
         "bb test/dependency_qualification.clj [--extra-alias]"
         qualify!)
