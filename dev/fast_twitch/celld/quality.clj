(ns fast-twitch.celld.quality
  "Shares explicit maintained-file selection between formatting and lint checks."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [fast-twitch.celld.lint-fixtures :as fixtures]))

(defn maintained-files
  "Selects owned sources and EDN, pruning generated and imported dependency trees."
  []
  (let [excluded #{"target" "build" "node_modules" "vendor" "imports"}
        roots ["src" "test" "dev" "resources" "examples" "docs" ".clj-kondo/hooks"]]
    (sort
      (concat ["deps.edn" "bb.edn" ".zprintrc" ".clj-kondo/config.edn"]
              (for [root roots
                    file (tree-seq
                           #(and (.isDirectory %)
                                 (or (= (str %) root)
                                     (and (not (.startsWith (.getName %) "."))
                                          (not (excluded (.getName %))))))
                           #(or (seq (.listFiles %)) [])
                           (io/file root))
                    :when (and (.isFile file)
                               (re-find #"\.(clj|cljs|cljc|edn)$" (.getName file)))]
                (str file))))))

(defn- execute!
  [& command]
  (let [{:keys [exit out err]} (apply shell/sh command)]
    (print out)
    (flush)
    (binding [*out* *err*] (print err))
    (when-not (zero? exit) (throw (ex-info "Quality check failed" {:exit exit})))))

(defn -main
  "Runs reproducible format/check or lint against the same maintained file set."
  [action & args]
  (try
    (case action
      "format" (apply execute!
                 "bin/celld-clojure"
                 "-M:zprint"
                 (if (= ["--check"] args) "-sc" "-sw")
                 (maintained-files))
      "lint" (do
               (execute! "clj-kondo"
                           "--repro"
                         "--cache-dir"
                           "target/clj-kondo-cache"
                         "--lint"
                           (str/join java.io.File/pathSeparator
                                     (remove #(str/starts-with?
                                                (.getCanonicalPath (io/file %))
                                                (str (.getCanonicalPath (io/file "."))
                                                     java.io.File/separator))
                                       (str/split (System/getProperty "java.class.path")
                                                  (re-pattern
                                                    java.io.File/pathSeparator))))
                         "--dependencies"
                           "--parallel")
               (apply execute!
                 "clj-kondo" "--repro"
                 "--cache-dir"
                   "target/clj-kondo-cache"
                 "--lint" (concat (maintained-files) args)))
      "lint-fixtures" (fixtures/verify!)
      (throw (ex-info "Use format [--check], lint or lint-fixtures" {:action action})))
    (catch clojure.lang.ExceptionInfo error
      (binding [*out* *err*]
        (println (.getMessage error))
        (when-not (:exit (ex-data error)) (prn (ex-data error))))
      (System/exit (:exit (ex-data error) 1)))
    (finally (shutdown-agents))))
