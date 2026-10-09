(ns fast-twitch.celld.dev
  "Local tasks supervise final-artifact builds; they never deploy or provision services."
  (:require [clojure.java.io :as io]
            [fast-twitch.celld.build :as build]
            [fast-twitch.celld.diagnostics :as diagnostics]))

(defn delete-tree!
  "Deletes only the explicitly selected generated path; no native state lookup occurs."
  [file]
  (doseq [item (reverse (file-seq (io/file file)))
          :when (.exists item)]
    (when-not (.delete item)
      (throw (ex-info "Build cleanup failed" {:path (str item)})))))

(defn clean!
  "Removes generated build artifacts under the build lock; preserves adopted history, dotenv input and local Cell state."
  [app-file]
  (let [app (build/read-edn app-file)
        output (:output app ".celld-build")
        lock-file (io/file output ".build.lock")]
    (io/make-parents lock-file)
    (with-open [raf (java.io.RandomAccessFile. lock-file "rw")
                channel (.getChannel raf)
                _lock (.lock channel)]
      (doseq [path ["generations" "staging" "manifest.edn" "status.edn" "wrangler.json"]]
        (delete-tree! (io/file output path))))
    (prn {:state :cleaned :preserved [".celld/dev" ".dev.vars" :adopted-history]})))

(defn starter!
  "Creates a new minimal direct-CLJS Worker project without adopting Cell history."
  [directory app-name]
  (let [root (io/file directory)]
    (when (.exists root)
      (build/fail! :starter [:directory] directory "Choose a new project directory."))
    (.mkdirs (io/file root "src/example"))
    (spit
      (io/file root "src/example/app.cljs")
      "(ns example.app\n  (:require-macros [fast-twitch.celld.macros :refer [deffetch defworker]]))\n(deffetch hello [context request] {:status 200 :body \"Hello from CLJS\"})\n(defworker App {:include [hello]})\n")
    (spit (io/file root "app.edn")
          (str (pr-str {:name app-name
                        :entry 'example.app
                        :paths [(str directory "/src")]
                        :output (str directory "/.celld-build")
                        :history (str directory "/class-history.edn")})
               "\n"))
    (spit (io/file root "class-history.edn") "[]\n")
    (prn
      {:starter directory
       :next
         "build app.edn; add Cell declarations and review prepare-config history candidates"})))

(def excluded
  #{".git" ".celld" ".celld-build" "target" "node_modules" ".cpcache" ".tools" "scratch"})

(defn source-files
  "Returns source files excluding generated output and local durable state."
  [root]
  (filter #(.isFile %)
    (tree-seq #(and (.isDirectory %) (not (excluded (.getName %))))
              #(or (seq (.listFiles %)) [])
              (io/file root))))

(defn signature
  "Detects application, macro, dependency and resource edits for supervised development."
  [app-file]
  ;; Include local dependency edits and macro/config/resource changes. Compiler
  ;; environments are fresh each time, so deleted declarations disappear too.
  (let [app (try (build/read-edn app-file) (catch Exception _ {}))
        roots (distinct (concat (build/local-input-roots)
                                ["src" "resources" "dev" "deps.edn" "package-lock.json"
                                 "../fast-twitch/src" "../fast-twitch/resources"]
                                (:paths app)
                                [(or (.getParent (io/file app-file)) ".")]))]
    (build/sha (pr-str (for [file (sort-by str (mapcat source-files roots))]
                         [(str file) (.lastModified file) (.length file)
                          (when (= (.getCanonicalPath file)
                                   (.getCanonicalPath (io/file app-file)))
                            (build/sha (slurp file)))])))))

(defn sync-vars!
  "Synchronizes and removes only owned staged dotenv copies; never changes source files or Cell state."
  [app-file output]
  (let [parent (or (.getParentFile (io/file app-file)) (io/file "."))
        marker (io/file output ".vars-owned.edn")
        old (if (.isFile marker) (set (build/read-edn marker)) #{})
        current (set (for [name [".dev.vars" ".env" ".env.local"]
                           :when (.isFile (io/file parent name))]
                       name))]
    (doseq [name current] (io/copy (io/file parent name) (io/file output name)))
    (doseq [name old
            :when (not (current name))]
      (let [file (io/file output name)] (when (.isFile file) (.delete file))))
    (build/atomic! marker (pr-str current))))

(defn fresh-build!
  "Resolves current dependencies through the CLI and builds in a fresh JVM with selected aliases preserved."
  [app-file]
  (let [basis (when-let [file (System/getProperty "clojure.basis")] (build/read-edn file))
        aliases (conj (vec (remove #{:dev :build :test}
                             (get-in basis [:basis-config :aliases])))
                      :build)
        extra (get-in basis [:basis-config :extra])
        command (into [(or (System/getenv "CELLD_CLOJURE_BIN") "clojure")]
                      (concat (when extra
                                ["-Sdeps" (if (string? extra) extra (pr-str extra))])
                              [(str "-M" (apply str aliases)) "build" app-file]))
        process (.start (.inheritIO (ProcessBuilder. ^java.util.List command)))]
    (zero? (.waitFor process))))

(defn rebuild!
  "Publishes a complete fresh-JVM build or retains the previous native generation and marks stale."
  [app-file output]
  (if (fresh-build! app-file)
    (do (sync-vars! app-file output) (prn {:state :built :reload :pending}) true)
    (do
      (when-not (try (= :stale (:state (build/read-edn (str output "/status.edn"))))
                     (catch Exception _ false))
        (build/atomic!
          (str output "/status.edn")
          (pr-str
            {:state :stale
             :diagnostic
               {:code :fast-twitch.celld.build/process
                :repair
                  "Correct the failed build input; inspect the preceding structured diagnostic."}})))
      (prn {:state :stale :message "Previous generation retained"})
      false)))

(defn stop-process!
  "Stops only the tracked local Celld process and its descendants, preserving local durable state."
  [process]
  (when process
    (doseq [child (reverse (vec (iterator-seq (.iterator (.descendants (.toHandle
                                                                         process))))))]
      (.destroy child))
    (.destroy process)
    (when-not (.waitFor process 10 java.util.concurrent.TimeUnit/SECONDS)
      (.destroyForcibly process))))

(defn dev!
  "Supervises native --no-watch and restarts only after a successful complete build. Failed builds retain the previous running generation and mark stale."
  [app-file args]
  (when-not (fresh-build! app-file)
    (build/fail! :initial-build []
                 :failed "Correct the failed initial build before starting dev."))
  (let [app (build/read-edn app-file)
        output (:output app ".celld-build")
        executable (or (System/getenv "CELLD_BIN")
                       (when (.isFile (io/file ".tools/celld")) ".tools/celld")
                       "celld")
        command (into [executable "dev" (str output "/wrangler.json") "--no-watch"] args)
        start (fn []
                (sync-vars! app-file output)
                (.start (.inheritIO (ProcessBuilder. ^java.util.List command))))
        process (atom (start))
        stopped (atom false)
        hook (Thread. #(do (reset! stopped true) (stop-process! @process)))]
    (.addShutdownHook (Runtime/getRuntime) hook)
    (try
      (loop [previous (signature app-file)]
        (when (and (.isAlive @process) (not @stopped))
          (Thread/sleep 300)
          (let [current (try (signature app-file) (catch Exception _ previous))]
            (when (and (not= current previous) (rebuild! app-file output))
              (stop-process! @process)
              (reset! process (start))
              (prn {:state :restarting :generation :published}))
            (recur current))))
      (.waitFor @process)
      (finally (stop-process! @process)
               (.removeShutdownHook (Runtime/getRuntime) hook)))))

(defn -main
  "Runs the supported local starter, clean or supervised dev task."
  [action & args]
  (try (case action
         "clean" (clean! (or (first args) "app.edn"))
         "starter" (starter! (or (first args) "example-app")
                             (or (second args) "example-app"))
         "dev" (dev! (or (first args) "app.edn") (rest args))
         (build/fail! :task [:action] action "Use starter, dev, or clean."))
       (shutdown-agents)
       (catch Throwable error
         (binding [*out* *err*] (prn (diagnostics/diagnostic error)))
         (System/exit 1))))
