(ns native-container-qualification
  "Runs the owned fixture in an existing rootful Podman VM with an explicit Linux binary."
  (:require [babashka.fs :as fs]
            [cheshire.core :as json]
            [clojure.string :as str]
            [fast-twitch.celld.tooling :as t]))

(def owned
  "/tmp/celld-phase-one-fixture")

(def output
  (t/path "target/native-container"))

(def native-command
  "cd /tmp/celld-phase-one-fixture && exec env RUST_LOG=warn DOCKER_HOST=unix:///run/podman/podman.sock CELLD_DOCKER=/usr/bin/podman ./celld dev wrangler.json --host 127.0.0.1 --port 18998 --no-watch --logs")

(defn qualify!
  [{:keys [ssh-port identity destination linux-binary local]}]
  (t/ensure! ssh-port "Missing required --ssh-port")
  (t/ensure! identity "Missing required --identity")
  (let [destination (or destination "root@127.0.0.1")
        linux-binary (or linux-binary "target/celld-linux")
        ssh ["ssh" "-i" identity "-p" ssh-port "-o" "StrictHostKeyChecking=no" "-o"
             "UserKnownHostsFile=/dev/null" destination]
        scp ["scp" "-i" identity "-P" ssh-port "-o" "StrictHostKeyChecking=no" "-o"
             "UserKnownHostsFile=/dev/null"]
        remote (fn [command & [timeout]]
                 (t/run! (conj ssh command) (if timeout {:timeout timeout} {})))
        evidence-path (str (fs/path output "evidence.json"))
        previous (when (fs/exists? evidence-path) (t/read-json evidence-path))
        build
          (t/run!
            (t/clojure-command local "build" "build" "examples/06-containers/app.edn"))
        config-path (t/path "examples/06-containers/.celld-build/wrangler.json")]
    (t/write! (str (fs/path output "build.log")) (str (:out build) (:err build)))
    (t/success! build)
    (let [{:keys [bundle manifest fingerprint]} (t/generation config-path)
          generation (str (fs/parent bundle))]
      (t/success! (remote (str "mkdir -p " (t/shell-quote (str owned "/generations")))))
      (doseq [[source target recursive] [[(t/path linux-binary) (str owned "/celld")
                                          false]
                                         [config-path (str owned "/wrangler.json") false]
                                         [generation (str owned "/generations/") true]]]
        (t/success! (t/run! (into scp
                                  (concat (when recursive ["-r"])
                                          [source (str destination ":" target)])))))
      (let [version (t/success!
                      (remote
                        (str "chmod +x " owned "/celld; " owned "/celld --version")))]
        (t/ensure! (re-find #"(?m)^celld 0\.6\.2$" (str/trim (:out version)))
                   (:out version)))
      (let [log (str (fs/path output "native.log"))
            process (t/logged! (conj ssh native-command) log {})
            evidence (atom
                       {:target "celld-0.6.2"
                        :platform
                          "official-linux-arm-binary inside existing rootful Podman VM"
                        :artifact-fingerprint fingerprint
                        :release-ready false
                        :previous-run previous
                        :artifact-integrity {:manifest-sha256 (t/sha256 manifest)
                                             :bundle-sha256 (t/sha256 bundle)}})]
        (try
          (t/wait-for! #(str/includes? (or (t/text log) "") "ready")
                       (str "Native VM readiness; inspect " log)
                       30
                       process)
          (let
            [result
               (t/success!
                 (remote
                   "curl --fail --silent --show-error --max-time 90 http://127.0.0.1:18998/"
                   100000))
             checks (json/parse-string (:out result) true)
             debug
               (remote
                 "curl --fail --silent --show-error --max-time 5 http://127.0.0.1:18998/debug"
                 10000)]
            (swap! evidence assoc
              :checks checks
              :diagnostics (json/parse-string (:out debug) true)
              :qualified (every? true? (vals checks))
              :adapter-qualified (every? true? (vals (dissoc checks :kill)))
              :native-limits
                (if (:kill checks)
                  []
                  ["process.kill(15) returned; native process exited normally after the bounded 3-second sleep."])
              :scope
                "Current declared container adapters; signal is native fire-and-forget, inactivity Promise is awaited; no general engine qualification.")
            (println (json/generate-string @evidence {:pretty true})))
          (finally
            (try
              (remote "curl --silent --max-time 10 http://127.0.0.1:18998/cleanup" 15000)
              (finally
                (try
                  (remote
                    "pkill -INT -f '^./celld dev wrangler.json --host 127.0.0.1 --port 18998 --no-watch --logs$'"
                    10000)
                  (finally
                    (try (t/stop! process 10000)
                         (finally (t/write-json! evidence-path @evidence)))))))))
        (t/ensure! (:adapter-qualified @evidence)
                   "Container adapter qualification failed")))))

(t/main!
  *file*
  {"--ssh-port" :value
   "--identity" :value
   "--destination" :value
   "--linux-binary" :value
   "--local" :flag}
  "bb test/native_container_qualification.clj --ssh-port PORT --identity FILE [--destination root@127.0.0.1] [--linux-binary target/celld-linux] [--local]"
  qualify!)
