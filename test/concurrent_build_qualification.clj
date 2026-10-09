(ns concurrent-build-qualification
  "Checks lock-wait and staged-resource failures retain one complete published generation."
  (:require [babashka.fs :as fs]
            [cheshire.core :as json]
            [clojure.string :as str]
            [fast-twitch.celld.tooling :as t])
  (:import [java.lang ProcessHandle]
           [java.nio.channels FileChannel]
           [java.nio.file OpenOption StandardOpenOption]))

(def base
  "target/concurrent-build")

(def app
  (str base "/app.edn"))

(def output
  (t/path base ".celld-build"))

(def resource
  (t/path base "assets/input.txt"))

(defn app-data
  [name]
  {:name name
   :entry 'fixture.app
   :paths [(str base "/src")]
   :output (str base "/.celld-build")
   :history (str base "/history.edn")
   :profile :development
   :profiles {:development {:assets {:directory (str base "/assets")}}}})

(def staged-edit
  "(require (quote fast-twitch.celld.build)) (let [original fast-twitch.celld.build/stage-resources!] (with-redefs [fast-twitch.celld.build/stage-resources! (fn [& args] (let [result (apply original args)] (spit \"target/concurrent-build/assets/input.txt\" \"changed\") result))] (fast-twitch.celld.build/-main \"build\" \"target/concurrent-build/app.edn\")))")

(defn retained!
  [result pointer]
  (t/ensure! (and (not (zero? (:exit result)))
                  (str/includes? (:err result) "inputs-changed")
                  (= pointer (t/sha256 (str (fs/path output "wrangler.json")))))
             (str (:out result) (:err result))))

(defn qualify!
  [{:keys [local]}]
  (t/write!
    (t/path base "src/fixture/app.cljs")
    "(ns fixture.app (:require-macros [fast-twitch.celld.macros :refer [defcell]]))\n(defcell Good {:binding :GOOD})\n")
  (t/write-edn! (t/path base "history.edn") [{:tag "v1" :new_sqlite_classes [:Good]}])
  (t/write! resource "before")
  (t/write-edn! (t/path app) (app-data :concurrent-fixture))
  (let [command (t/clojure-command local "build" "build" app)
        expression (t/expression-command local staged-edit)]
    (t/success! (t/run! command))
    (let [pointer (t/sha256 (str (fs/path output "wrangler.json")))
          lock-path (str (fs/path output ".build.lock"))
          process (atom nil)]
      (try
        (with-open [channel (FileChannel/open (fs/path lock-path)
                                              (into-array OpenOption
                                                          [StandardOpenOption/WRITE]))
                    _lock (.lock channel)]
          (reset! process (t/start! command))
          (t/wait-for! #(let [users (str/split (:out (t/run! ["lsof" "-t" lock-path]))
                                               #"\s+")]
                          (some (fn [pid]
                                  (when-let [pid (parse-long pid)]
                                    (not= pid (.pid (ProcessHandle/current)))))
                                users))
                       "Build never reached its output lock"
                       60
                       @process)
          (t/write-edn! (t/path app) (app-data :changed-while-waiting)))
        (retained! (t/finish! @process 120000) pointer)
        (println "lock-wait-input-change PASS")
        (finally (t/stop! @process)))
      (t/write-edn! (t/path app) (app-data :concurrent-fixture))
      (retained! (t/run! expression {:timeout 120000}) pointer)
      (println "selected-profile-resource-edit PASS"))
    (t/write! resource "before")
    (let [native (t/path base "native.jsonc")
          data {:name :concurrent-fixture
                :main "placeholder.mjs"
                :no_bundle true
                :compatibility_date "2026-10-07"
                :durable_objects {:bindings [{:name :GOOD :class_name :Good}]}
                :migrations [{:tag "v1" :new_sqlite_classes [:Good]}]
                :assets {:directory "assets"}
                :vars {:URL "https://fixture/*kept*/"}}
          wire (json/generate-string data)]
      (t/write! native (str "{// fixture\n" (subs wire 1 (dec (count wire))) ",}"))
      (t/write-edn! (t/path app)
                    {:name :concurrent-fixture
                     :entry 'fixture.app
                     :paths [(str base "/src")]
                     :output (str base "/.celld-build")
                     :native-config (str base "/native.jsonc")})
      (t/success! (t/run! command))
      (let [pointer (t/sha256 (str (fs/path output "wrangler.json")))
            config (t/read-json (str (fs/path output "wrangler.json")))]
        (t/ensure! (= "https://fixture/*kept*/" (get-in config [:vars :URL]))
                   "JSONC changed a string literal")
        (retained! (t/run! expression {:timeout 120000}) pointer)
        (println "native-relative-resource-edit PASS")))
    (t/write-json! (t/path base "evidence.json")
                   (mapv #(hash-map :fixture % :passed true)
                     ["lock-wait-input-change" "selected-profile-resource-edit"
                      "native-jsonc-string-preservation"
                      "native-relative-resource-edit"]))))

(t/main! *file*
         {"--local" :flag}
         "bb test/concurrent_build_qualification.clj [--local]"
         qualify!)
