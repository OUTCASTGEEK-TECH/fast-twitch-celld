(ns freeze-release-evidence
  "Retains exact observed evidence outside application compilation inputs."
  (:require [babashka.fs :as fs]
            [cheshire.core :as json]
            [fast-twitch.celld.tooling :as t]))

(def public-checks
  {:public-command "target/public-build-fixtures/evidence.json"
   :development "target/dev-qualification/evidence.json"
   :local-dependency "target/dependency-qualification/evidence.json"
   :alias-extra-dependency "target/dependency-extra-qualification/evidence.json"
   :concurrent-public-build "target/concurrent-build/evidence.json"
   :repeat-build-and-immutability "target/build-reproducibility.json"})

(def retained-logs
  ["target/correction-public-builds-retained.log"
   "target/correction-public-builds-remaining.log"
   "target/correction-public-builds-websocket-final.log"])

(def source-pins
  {:celld "90b43017241f81189453d326d05948f388b34652"
   :fast-twitch "a9f96bf90d0ded9282ac80a0f7fb808f1e5c719a"
   :fast-twitch-publication
     "published Git dependency; optional sibling development override"
   :cljs "56a94ac13bfac22108e4d868dfd4763edaceb864"
   :malli "0.20.2"
   :honeysql "2.7.1479"
   :esbuild "0.25.12"
   :jdk "OpenJDK27"
   :node "26.10.0"})

(defn integrity
  [manifest]
  {:manifest manifest
   :manifest-sha256 (t/sha256 (t/path manifest))
   :bundle-sha256 (t/sha256 (str (fs/path (fs/parent (t/path manifest)) "bundle.mjs")))})

(defn freeze!
  [{:keys [output-dir]}]
  (let
    [native-path (t/env "CELLD_EVIDENCE_FILE" "target/native-qualification/evidence.json")
     native (t/read-json (t/path native-path))
     latest
       (mapv
         #(if (= "Date.now" (get-in % [:adapter-validation-measurement :clock]))
            (assoc-in %
              [:adapter-validation-measurement :timing-status]
              "Unusable for overhead measurement: pinned native clock is frozen within the turn. Retained as diagnostic only.")
            %)
         (t/latest (:fixtures native) :fixture))
     artifacts (t/latest (:artifacts native) :fingerprint)
     artifact-index (t/indexed artifacts :fingerprint)
     referenced (set (map :artifact-fingerprint latest))
     artifacts (mapv #(assoc %
                        :current-case-linkage
                          (if (referenced (:fingerprint %))
                            "referenced-by-retained-native-case"
                            "build-or-historical-artifact-no-current-case-link"))
                 artifacts)
     container (t/read-json (t/path "target/native-container/evidence.json"))
     container-manifest (str "examples/06-containers/.celld-build/generations/"
                             (:artifact-fingerprint container)
                             "/manifest.edn")
     container (assoc container
                 :qualification-scope (:scope container
                                              "Historical experimental VM cases only.")
                 :artifact-integrity-observed-at-freeze (integrity container-manifest))
     checks (into {}
                  (for [[name path] public-checks]
                    [name
                     {:record path
                      :sha256 (t/sha256 (t/path path))
                      :evidence (t/read-json (t/path path))}]))
     checks
       (into
         checks
         (for [path retained-logs]
           [(keyword path)
            {:record path
             :sha256 (t/sha256 (t/path path))
             :scope
               "Retained actual public-command output, including fixed variadic rerun."}]))
     coverage (t/read-json (t/path "docs/coverage.json"))
     report
       {:target "celld-0.6.2"
        :status (if (seq (:remaining-scoped-paths coverage))
                  "in-progress"
                  "scoped-follow-up-complete")
        :release-ready (:release-ready coverage)
        :scope
          "Observed exact artifact cases and the seven authorized follow-up items; inventory counts do not create additional tasks."
        :source-pins source-pins
        :fixtures latest
        :artifacts artifacts
        :container container
        :public-checks checks
        :coverage coverage
        :scoped-complete (:scoped-complete coverage)
        :native-limits (:native-limits coverage)
        :remaining (:remaining-scoped-paths coverage)
        :native-evidence-source native-path
        :historical-evidence
          "target/native-qualification/evidence.json retains earlier runs; fixtures here select latest observed cases from the named native-evidence-source."}]
    (doseq [record latest]
      (let [artifact (artifact-index (:artifact-fingerprint record))]
        (t/ensure! artifact (str "Missing artifact for " (:fixture record)))
        (t/ensure! (fs/exists? (t/path (:manifest artifact)))
                   (str "Missing manifest for " (:fixture record)))
        (let [actual (integrity (:manifest artifact))]
          (doseq [key [:manifest-sha256 :bundle-sha256]
                  :when (get artifact key)]
            (t/ensure! (= (get artifact key) (get actual key))
                       (str "Artifact digest mismatch: " (:fixture record) " " key))))))
    (t/write-json! (t/path (or output-dir "") "docs/qualification-evidence.json") report)
    (println (json/generate-string {:native-cases (count latest)
                                    :artifacts (count artifacts)
                                    :container-qualified (:qualified container)
                                    :release-ready (:release-ready coverage)}))))

(t/main!
  *file*
  {"--output-dir" :value}
  "bb test/freeze_release_evidence.clj [--output-dir DIR]; CELLD_EVIDENCE_FILE selects the retained native input."
  freeze!)
