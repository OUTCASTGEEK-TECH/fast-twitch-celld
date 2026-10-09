(ns build-reproducibility
  "Checks public repeat-build determinism, minimal-bundle budget and immutable input edits."
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
            [fast-twitch.celld.tooling :as t]))

(def app
  "test/fixtures/macro-only/app.edn")

(def config-path
  (t/path "test/fixtures/macro-only/.celld-build/wrangler.json"))

(def report-path
  (t/path "target/build-reproducibility.json"))

(def budget
  {:bundle-bytes 2500000 :gzip-bytes 300000})

(defn current
  []
  (let [generation (t/generation config-path)]
    (assoc generation :manifest-sha256 (t/sha256 (:manifest generation)))))

(defn run-build!
  [local]
  (t/success! (t/run! (t/clojure-command local "build" "build" app)))
  (current))

(defn run-record
  [local]
  (let [{:keys [bundle manifest] :as generation} (run-build! local)]
    (merge (select-keys generation [:fingerprint :input-fingerprint :manifest-sha256])
           {:bundle-sha256 (t/sha256 bundle)
            :config-sha256 (t/sha256 config-path)
            :bundle-bytes (fs/size bundle)
            :gzip-bytes (t/gzip-bytes bundle)
            :runtime-namespaces (:runtime-namespaces (t/read-edn manifest))
            :manifest (t/relative manifest)})))

(defn unchanged!
  [generation]
  (t/ensure! (= (:manifest-sha256 generation) (t/sha256 (:manifest generation)))
             "Prior immutable manifest changed"))

(defn qualify!
  [{:keys [local]}]
  (let
    [runs (mapv (fn [_]
                  (run-record local))
            (range 2))
     last-run (peek runs)
     evidence
       {:fixture "public-repeat-build-and-minimal-bundle"
        :passed true
        :runs runs
        :declared-budget budget
        :scope
          "Pinned simple compiler, minimal checked native RPC Cell; not every application bundle."}]
    (doseq [field [:fingerprint :bundle-sha256 :manifest-sha256 :config-sha256
                   :input-fingerprint]]
      (t/ensure! (= (get (first runs) field) (get last-run field))
                 (str "Non-deterministic repeat build: " field)))
    (doseq [namespace (:runtime-namespaces last-run)]
      (t/ensure! (not-any? #(str/starts-with? namespace %)
                           ["fast-twitch.celld.sql" "fast-twitch.celld.worker"
                            "fast-twitch.celld.services."
                            "fast-twitch.celld.communication"
                            "fast-twitch.celld.websocket"])
                 namespace))
    (doseq [[field limit] budget]
      (t/ensure! (< (get last-run field) limit) (str field " exceeds bundle budget")))
    (t/write-json! report-path evidence)
    (println (:fixture evidence) "PASS")
    (let [source (t/path "test/fixtures/macro-only/src/fixture/app.cljs")
          source-before (t/bytes source)
          app-before (slurp (t/path app))
          prior (current)
          edits (atom nil)]
      (try
        (t/write! source (str (slurp source) "\n;; Owned no-output input edit.\n"))
        (let [comment (run-build! local)]
          (t/ensure! (not= (:fingerprint prior) (:fingerprint comment))
                     "Source edit reused prior generation")
          (unchanged! prior)
          (t/ensure! (= (:bundle-sha256 last-run) (t/sha256 (:bundle comment)))
                     "No-output source edit changed the bundle")
          (t/write-bytes! source source-before)
          (let [native-file (t/path "target/build-reproducibility/native.json")
                data (assoc (t/read-json config-path)
                       :main "unused.mjs"
                       :vars {:FIXTURE "before"})]
            (t/write-json! native-file data)
            (t/write-edn! (t/path app)
                          (assoc (t/read-edn (t/path app)) :native-config native-file))
            (let [native (run-build! local)]
              (t/write-json! native-file (assoc data :vars {:FIXTURE "after"}))
              (let [edited (run-build! local)]
                (t/ensure! (not= (:fingerprint native) (:fingerprint edited))
                           "Native config edit reused prior generation")
                (unchanged! native)
                (t/ensure! (= (t/sha256 (:bundle native)) (t/sha256 (:bundle edited)))
                           "Native-only config edit changed the bundle")
                (unchanged! prior)
                (unchanged! comment)
                (reset! edits {:passed true
                               :no-output-source-edit {:before (:fingerprint prior)
                                                       :after (:fingerprint comment)}
                               :native-config-only-edit {:before (:fingerprint native)
                                                         :after (:fingerprint edited)}
                               :prior-manifests-unchanged true})))))
        (finally (t/write-bytes! source source-before)
                 (t/write! (t/path app) app-before)))
      (run-build! local)
      (t/write-json! report-path (assoc evidence :immutable-input-edits @edits))
      (println "immutable-no-output-and-native-config-edits PASS"))))

(t/main! *file* {"--local" :flag} "bb test/build_reproducibility.clj [--local]" qualify!)
