(ns fast-twitch.celld.tooling
  "Small shared IO and owned-process helpers for the Babashka qualification scripts."
  (:refer-clojure :exclude [bytes run!])
  (:require [babashka.fs :as fs]
            [babashka.http-client :as http]
            [babashka.process :as process]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.nio.file Files]
           [java.security MessageDigest]
           [java.util.zip Deflater]))

(def root
  (fs/canonicalize (fs/path (fs/parent *file*) "../../..")))

(defn path
  [& parts]
  (str (reduce #(.resolve %1 (str %2)) root parts)))

(defn relative
  [p]
  (str (fs/relativize root (fs/canonicalize p))))

(defn env
  [key fallback]
  (or (not-empty (System/getenv key)) fallback))

(defn text
  [p]
  (when (fs/exists? p) (slurp p)))

(defn read-edn
  [p]
  (edn/read-string (slurp p)))

(defn read-json
  [p]
  (json/parse-string (slurp p) true))

(defn bytes
  [p]
  (Files/readAllBytes (fs/path p)))

(defn write!
  [p value]
  (fs/create-dirs (fs/parent p))
  (spit p value))

(defn write-edn!
  [p value]
  (write! p (str (pr-str value) "\n")))

(defn write-json!
  [p value]
  (write! p (str (json/generate-string value {:pretty true}) "\n")))

(defn write-bytes!
  [p value]
  (fs/create-dirs (fs/parent p))
  (with-open [out (io/output-stream p)] (.write out value)))

(defn sha256
  [p]
  (format
    "%064x"
    (java.math.BigInteger. 1 (.digest (MessageDigest/getInstance "SHA-256") (bytes p)))))

(defn gzip-bytes
  "Counts gzip at level 9, including its fixed header/trailer, without retaining a copy."
  [p]
  (let [deflater (Deflater. Deflater/BEST_COMPRESSION true)
        buffer (byte-array 8192)]
    (try
      (.setInput deflater (bytes p))
      (.finish deflater)
      (loop [size 18]
        (if (.finished deflater)
          size
          (recur (+ size (.deflate deflater buffer)))))
      (finally (.end deflater)))))

(defn now
  []
  (System/nanoTime))

(defn elapsed
  [start]
  (/ (- (now) start) 1e9))

(defn rounded
  [number]
  (/ (Math/round (* 1000.0 number)) 1000.0))

(defn tail
  [value limit]
  (subs (or value "") (max 0 (- (count (or value "")) limit))))

(defn ensure!
  [condition message]
  (when-not condition (throw (ex-info (str message) {}))))

(defn alive?
  [p]
  (process/alive? p))

(defn stop!
  "Interrupts only this process and its descendants; escalates after a bounded wait."
  ([p]
   (stop! p 15000))
  ([p timeout]
   (when p
     (let [proc (:proc p)
           handles (with-open [stream (.descendants (.toHandle proc))]
                     (vec (.toArray stream)))
           owned (conj handles (.toHandle proc))]
       (doseq [handle owned
               :when (.isAlive handle)]
         @(process/process ["kill" "-INT" (str (.pid handle))]
                           {:out :discard :err :discard}))
       (let [deadline (+ (now) (* timeout 1000000))]
         (loop []
           (when (and (some #(.isAlive %) owned) (< (now) deadline))
             (Thread/sleep 50)
             (recur))))
       (doseq [handle owned :when (.isAlive handle)] (.destroyForcibly handle))
       @p))))

(defn start!
  ([command]
   (start! command {}))
  ([command options]
   (process/process
     (mapv str command)
     (merge {:dir (str root) :out :string :err :string :shutdown process/destroy-tree}
            options))))

(defn finish!
  ([p]
   @p)
  ([p timeout]
   (let [result (deref p timeout ::timeout)]
     (when (= ::timeout result)
       (stop! p)
       (throw (ex-info "Owned command timed out" {:command (:cmd p)})))
     result)))

(defn run!
  ([command]
   (run! command {}))
  ([command options]
   (let [p (start! command (dissoc options :timeout))]
     (if-let [timeout (:timeout options)]
       (finish! p timeout)
       (finish! p)))))

(defn success!
  [result]
  (ensure! (zero? (:exit result)) (tail (str (:out result) (:err result)) 3000))
  result)

(defn clojure-command
  [local alias & args]
  (into [(path "bin/celld-clojure") (str "-M:" (when local "local:") alias)] args))

(defn expression-command
  [local expression]
  [(path "bin/celld-clojure") "-Sdeps" "{:paths [\"src\" \"resources\" \"dev\"]}"
   (if local "-M:local" "-M") "-e" expression])

(defn logged!
  [command log options]
  (fs/create-dirs (fs/parent log))
  (start! command (merge options {:out (io/file log) :err :out})))

(defn wait-for!
  [predicate description seconds p]
  (let [start (now)]
    (loop []
      (cond
        (predicate) true
        (and p (not (alive? p)))
          (throw (ex-info (str description ": owned process exited") {}))
        (> (elapsed start) seconds) (throw (ex-info (str description " timed out") {}))
        :else (do (Thread/sleep 100) (recur))))))

(defn get!
  [origin route timeout]
  (let [start (now)
        {:keys [status body]} (http/get (str origin route)
                                        {:timeout timeout :throw false})]
    (ensure! (< status 400) (str "HTTP " status " " (tail body 3000)))
    [body (rounded (* 1000 (elapsed start)))]))

(defn response
  [origin route]
  (try (first (get! origin route 2000)) (catch Exception _ nil)))

(defn passed!
  [results fixture]
  (swap! results conj {:fixture fixture :passed true})
  (println fixture "PASS"))

(defn generation
  "Resolves the immutable manifest from the published native configuration."
  [config-path]
  (let [config (read-json config-path)
        bundle (str (.resolve (fs/parent config-path) (:main config)))
        manifest (str (fs/path (fs/parent bundle) "manifest.edn"))]
    {:config config
     :bundle bundle
     :manifest manifest
     :fingerprint (str (fs/file-name (fs/parent bundle)))
     :input-fingerprint (:input-sha (read-edn manifest))}))

(defn indexed
  [records key-fn]
  (into {} (map (juxt key-fn identity) records)))

(defn latest
  "Keeps first-seen order and the last record for each identity, as existing reports do."
  [records key-fn]
  (let [index (indexed records key-fn)]
    (mapv index (distinct (map key-fn records)))))

(defn shell-quote
  [value]
  (str "'" (str/replace (str value) "'" "'\"'\"'") "'"))

(defn options
  "Parses the small script flag sets before any fixture or report writes."
  [args spec]
  (loop [args args
         opts {}]
    (if-let [flag (first args)]
      (if (= "--help" flag)
        {:help true}
        (let [kind (get spec flag)
              key (keyword (subs flag 2))]
          (ensure! kind (str "Unknown argument: " flag))
          (case kind
            :flag (recur (next args) (assoc opts key true))
            :value (do (ensure! (and (second args)
                                     (not (str/starts-with? (second args) "--")))
                                (str "Missing value for " flag))
                       (recur (nnext args) (assoc opts key (second args))))
            :many (let [[values rest] (split-with #(not (str/starts-with? % "--"))
                                                  (next args))]
                    (ensure! (seq values) (str "Missing values for " flag))
                    (recur rest (assoc opts key (vec values)))))))
      opts)))

(defn main!
  "Allows requiring scripts for inspection without running their entry points."
  [script spec usage f]
  (when (= (str (fs/canonicalize script))
           (some-> (System/getProperty "babashka.file")
                   fs/canonicalize
                   str))
    (try
      (let [opts (options *command-line-args* spec)]
        (if (:help opts) (println usage) (f opts)))
      (catch Exception error
        (binding [*out* *err*] (println (.getMessage error)))
        (System/exit 1)))))
