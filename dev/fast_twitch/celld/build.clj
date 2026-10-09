(ns fast-twitch.celld.build
  "Mandatory application build: analyze, compile, validate, stage, atomically publish."
  (:require [cljs.build.api :as cljs]
            [cljs.compiler :as compiler]
            [cljs.analyzer :as analyzer]
            [fast-twitch.celld.config :as config]
            [cljs.env :as env]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.java.shell :as shell]
            [fast-twitch.celld.definition :as d]
            [fast-twitch.celld.contracts :as contracts]
            [fast-twitch.celld.names :as names]
            [fast-twitch.celld.diagnostics :as diagnostics])
  (:import [java.nio.file Files StandardCopyOption]
           [java.security MessageDigest]))

(def target
  (edn/read-string (slurp (io/resource "fast_twitch/celld/runtime-profiles.edn"))))

(def supported-native-modules
  #{"cloudflare:workers" "cloudflare:sockets" "cloudflare:workflows" "node:assert"
    "node:assert/strict" "node:timers/promises" "node:test" "node:test/reporters"
    "node:util" "node:util/types" "node:events" "node:os" "node:path" "node:path/posix"
    "node:path/win32" "node:buffer" "node:crypto" "node:async_hooks"
    "node:diagnostics_channel" "node:stream" "node:stream/promises" "node:stream/web"
    "node:stream/consumers" "node:fs" "node:fs/promises" "node:zlib"})

(defn resolve-path
  "Resolves an explicit absolute or base-relative native input path without changing its authority."
  [base path]
  (let [file (io/file path)] (if (.isAbsolute file) file (io/file base path))))

(defn read-edn
  "Reads portable EDN build input without executing tagged readers."
  [file]
  (edn/read-string {:readers {}
                    :default (fn [tag _]
                               (throw (ex-info "Tagged values unavailable" {:tag tag})))}
                   (slurp file)))

(defn read-native
  "Reads JSON/JSONC, preserving complete string tokens while removing comments and trailing commas."
  [file]
  (let [without-comments (str/replace (slurp file)
                                      #"\"(?:\\.|[^\"\\])*\"|//[^\r\n]*|/\*[\s\S]*?\*/"
                                      #(if (= \" (first %)) % " "))
        without-trailing-commas (str/replace without-comments
                                             #"\"(?:\\.|[^\"\\])*\"|,\s*[\]}]"
                                             #(if (= \" (first %)) % (subs % 1)))]
    (json/read-str without-trailing-commas :key-fn keyword)))

(defn- sha-bytes
  [bytes]
  (format "%064x"
          (BigInteger. 1
                       (.digest (MessageDigest/getInstance "SHA-256") bytes))))

(defn sha
  "Returns a deterministic SHA-256 digest of UTF-8 input."
  [text]
  (sha-bytes (.getBytes (str text) "UTF-8")))

(defn fail!
  "Throws a bounded structured build diagnostic."
  [code path received repair]
  (d/fail! code nil :application path :valid-configuration received repair))

(def ^:private App
  (contracts/closed
    {:name :keyword
     :entry [:fn symbol?]
     :paths [:vector :string]
     :output :string
     :history :string
     :optimizations [:= :simple]
     :native-config :string
     :config :map
     :profile :keyword
     :profiles [:map-of :keyword :map]
     :extensions contracts/extensions}
    #{:name :entry}))

(defn config!
  "Checks whole-application native configuration against selected declarations."
  [config descriptors]
  (config/validate! config descriptors)
  (when-not (and (string? (:name config))
                 (re-matches #"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?" (:name config)))
    (fail! :app-name
           [:name]
           (:name config)
           "Use 1–63 lowercase letters/digits/internal hyphens."))
  (when (and (:no_bundle config) (or (:define config) (:rules config)))
    (fail! :bundling [:no_bundle] true "Omit define/rules with no_bundle."))
  (doseq [flag (:compatibility_flags config)]
    (when-not ((:flags target) flag)
      (fail! :unsupported-flag
             [:compatibility_flags]
             flag
             "Select an honored target-profile flag.")))
  (let [rpc? (some #(= :rpc (:kind %)) (mapcat :handlers descriptors))]
    (when (and rpc? (not (some #{"js_rpc"} (:compatibility_flags config))))
      (fail! :required-flag [:compatibility_flags]
             "js_rpc" "Keep js_rpc for plain Cell RPC.")))
  config)

(defn reachable-namespaces
  "Returns the actual analyzed entry graph; unrelated classpath declarations are excluded."
  [compiler entry]
  (let [namespaces (:cljs.analyzer/namespaces @compiler)]
    (loop [pending [entry]
           seen #{}]
      (if-let [ns (first pending)]
        (if (seen ns)
          (recur (next pending) seen)
          (recur (concat (next pending) (vals (:requires (get namespaces ns))))
                 (conj seen ns)))
        seen))))

(defn descriptors
  "Collects compiler metadata only from the selected application graph."
  [compiler reachable]
  (->> (:cljs.analyzer/namespaces @compiler)
       (filter (fn [[ns _]]
                 (reachable ns)))
       (map second)
       (mapcat #(vals (:defs %)))
       (keep :fast-twitch.celld/descriptor)
       (filter #(#{:cell :worker :workflow} (:kind %)))
       (sort-by (comp str :name))
       vec))

(defn duplicates!
  "Rejects duplicate declaration identities with both source locations."
  [items key]
  (doseq [[value ds] (group-by key (filter #(some? (key %)) items))
          :when (> (count ds) 1)]
    (d/fail! :duplicate-identity
             (:source (first ds))
             (:name (first ds))
             [key]
             :unique-native-identity value
             "Use distinct stable export/binding identities."
               {:sources (mapv :source ds)})))

(defn- cells
  [descriptors]
  (filter #(and (= :cell (:kind %)) (not (:facet? %))) descriptors))

(defn history!
  "Checks compiled classes against explicitly adopted SQLite history."
  [history descriptors]
  (d/check! [:vector
             (contracts/closed
               {:tag :string :new_sqlite_classes [:vector :string]}
               #{:tag})]
            history
            nil
            :application
            [:history])
  (let [adopted (set (mapcat :new_sqlite_classes history))
        current (set (map :export
                       (cells descriptors)))]
    (when (seq (remove adopted current))
      (fail! :unadopted-class
             [:history]
             (vec (remove adopted current))
             "Run prepare-config, review and adopt its history candidate."))
    (when (seq (remove current adopted))
      (fail! :class-history-drift
             [:history]
             (vec (remove current adopted))
             "Retain adopted class exports; rename/delete migrations are unsupported.")))
  history)

(defn generated-config
  "Derives minimal native configuration from declarations and adopted history."
  [app ds history main]
  (let [cells (cells ds)
        workflows (filter #(= :workflow (:kind %)) ds)
        handlers (mapcat :handlers ds)
        required (when (some #(= :rpc (:kind %)) handlers)
                   ["js_rpc"])
        base {:name (names/identifier (:name app))
              :main main
              :no_bundle true
              :compatibility_date (:compatibility-date target)
              :compatibility_flags (vec required)
              :durable_objects {:bindings (mapv (fn [x]
                                                  {:name (:binding x)
                                                   :class_name (:export x)})
                                            cells)}
              :migrations history}
        crons (vec (mapcat #(get-in % [:options :crons] []) handlers))
        base (cond-> base (seq crons) (assoc :triggers {:crons crons}))
        base (cond-> base
               (seq workflows) (assoc :workflows
                                 (mapv (fn [x]
                                         {:binding (:binding x)
                                          :name (:workflow-name x)
                                          :class_name (:export x)})
                                   workflows)))
        explicit (let [selected (:profile app :release)
                       profiles (:profiles app {:release {} :development {}})]
                   (when-not (contains? profiles selected)
                     (fail! :profile [:profile] selected "Select a declared profile."))
                   (config/native-values (merge (:config app {})
                                                (get profiles selected))))]
    (when (seq (select-keys explicit
                            [:name :main :durable_objects :migrations :workflows]))
      (fail! :authoritative-config
             [:config]
             (keys explicit)
             "Use existing-native-file mode or let declarations derive identity."))
    (merge base
           explicit
           {:compatibility_flags
              (vec (distinct (concat required (:compatibility_flags explicit))))})))

(defn atomic!
  "Atomically replaces one generation pointer on the same filesystem."
  [file text]
  (let [f (io/file file)
        temp (io/file (str file ".pending"))]
    (io/make-parents f)
    (spit temp text)
    (Files/move (.toPath temp)
                (.toPath f)
                (into-array StandardCopyOption
                            [StandardCopyOption/ATOMIC_MOVE
                             StandardCopyOption/REPLACE_EXISTING]))))

(defn strict-warning!
  "Escalates enabled application arity/resolution warnings while respecting suppressed library analysis."
  [kind environment extra]
  (when (and (#{:fn-arity :undeclared-var :undeclared-ns} kind)
             (let [mode (get analyzer/*cljs-warnings* kind)]
               (and mode (not (#{false :off} mode)))))
    (throw (ex-info "Compiler boundary failed"
                    {:code :fast-twitch.celld.build/compiler-boundary
                     :source (assoc (select-keys environment [:line :column])
                               :file (or (:file environment) analyzer/*cljs-file*))
                     :declaration (or (:name environment)
                                      (get-in environment [:ns :name])
                                      :application)
                     :path [kind]
                     :expected :resolved-compiler-boundary
                     :received {:type (str (type extra))}
                     :repair "Resolve the compiler warning before publication."})))
  (analyzer/default-warning-handler kind environment extra))

(defn compile!
  "Analyzes the selected graph, then compiles only its runtime dependencies; restores the caller classloader."
  [app staging]
  (let [compiler (env/default-compiler-env)
        js (str staging "/compiled.js")
        thread (Thread/currentThread)
        previous-loader (.getContextClassLoader thread)
        loader (clojure.lang.DynamicClassLoader. (clojure.lang.RT/baseLoader))]
    (doseq [path (:paths app ["src"])] (.addURL loader (.toURL (.toURI (io/file path)))))
    (.setContextClassLoader thread loader)
    (try
      (with-bindings {clojure.lang.Compiler/LOADER loader}
        (binding [env/*compiler* compiler]
          (binding [analyzer/*cljs-warnings* (zipmap (keys analyzer/*cljs-warnings*)
                                                     (repeat false))]
            (compiler/with-core-cljs))
          (binding [analyzer/*cljs-warning-handlers* [strict-warning!]]
            (analyzer/analyze-file (cljs/ns->source (:entry app)))))
        (let [reachable (reachable-namespaces compiler (:entry app))]
          (cljs/build (set (filter cljs/ns->source reachable))
                      {:optimizations :simple
                       :output-to js
                       :output-dir (str staging "/cljs")
                       :target :bundle
                       :warning-handlers [strict-warning!]
                       :infer-externs true
                       :source-map false
                       :pretty-print false
                       :parallel-build false}
                      compiler)
          {:descriptors (descriptors compiler reachable)
           :js js
           :runtime-namespaces (vec (sort (map str
                                            reachable)))
           :external-modules
             (vec (sort (for
                          [namespace reachable
                           :let [specifier (str namespace)]
                           :when (and (or (str/starts-with? specifier "node:")
                                          (str/starts-with? specifier "cloudflare:"))
                                      (get-in @compiler
                                              [:js-dependency-index specifier
                                               :external?]))]
                          specifier)))}))
      (finally (.setContextClassLoader thread previous-loader)))))

(defn package!
  "Packages the final native ESM artifact and automatic named exports."
  [js ds staging generation external-modules]
  (doseq [specifier external-modules]
    (when-not (supported-native-modules specifier)
      (fail! :runtime-import
             [:imports specifier]
             specifier
             "Import a module implemented by the pinned Celld target.")))
  (let
    [raw (str staging "/entry.js")
     output (str generation "/bundle.mjs")
     imports
       (apply str
         (map-indexed
           (fn [index specifier]
             (format
               "import * as ft_native_module_%d from %s;\nglobalThis[%s]=ft_native_module_%d;\n"
               index
               (json/write-str specifier)
               (json/write-str specifier)
               index))
           external-modules))
     exports (str/join
               "\n"
               (map-indexed
                 (fn [i x]
                   (format
                     "const ft_export_%d=globalThis[%s];\nexport {ft_export_%d as %s};"
                     i
                     (json/write-str (:export-root x))
                     i
                     (:export x)))
                 ds))]
    (spit raw (str imports (slurp js) "\n" exports "\n"))
    (let [result (shell/sh "node_modules/.bin/esbuild"
                           raw
                           "--bundle"
                           "--minify-whitespace" "--format=esm"
                           "--platform=neutral" "--external:cloudflare:*"
                           "--external:node:*" (str "--outfile=" output))]
      (when-not (zero? (:exit result))
        (throw (ex-info "Bundle failed"
                        {:code :fast-twitch.celld.build/bundler :detail (:err result)}))))
    output))

(defn- client-namespace
  [app]
  (symbol (str "fast-twitch.celld.generated." (names/identifier (:name app)))))

(defn generate-clients!
  "Writes deterministic case-preserving RPC clients into staging."
  [output app descriptors]
  (let
    [ns (client-namespace app)
     path (str output
               "/generated-src/"
               (-> (str ns)
                   (str/replace "." "/")
                   (str/replace "-" "_"))
               ".cljs")
     methods
       (for [cell descriptors
             handler (:handlers cell)
             :when (= :rpc (:kind handler))]
         (let [function (symbol (str (:export cell)
                                     "-"
                                     (:member handler)
                                     "!"))
               options (:options handler)]
           `(~'defn
             ~(with-meta function {:async true})
             ~(format
                "Native %s.%s client. Takes its stub first; schemas/codecs match the server."
                (:export cell)
                (:member handler))
             [~'stub & ~'args]
             (cljs.core/await
               (~'rpc/call!
                ~'stub
                ~(:method options)
                (~'vec ~'args)
                ~{:args-schema (:args options)
                  :returns (:returns options)
                  :codec (:codec options)})))))]
    (let [symbols (map second methods)]
      (when-not (= (count symbols) (count (distinct symbols)))
        (fail! :client-symbol-collision
               [:clients]
               symbols
               "Use distinct native export/method identities.")))
    (io/make-parents path)
    (binding [*print-meta* true]
      (spit path
            (str (pr-str (list 'ns
                               ns
                               "Generated native clients; do not edit."
                               '(:require [fast-twitch.celld.rpc :as rpc])))
                 "\n"
                 (str/join "\n" (map pr-str methods))
                 "\n")))
    path))

(defn copy-tree!
  "Stages declared resource trees without generated state or links."
  [source destination]
  (let [root (io/file source)]
    (when-not (.exists root)
      (fail! :resource-path [:resources] source "Create the declared resource path."))
    (doseq [file (tree-seq (fn [file]
                             (and (.isDirectory file)
                                  (not (#{".git" ".celld" ".celld-build" "node_modules"}
                                        (.getName file)))
                                  (not (Files/isSymbolicLink (.toPath file)))))
                           #(or (seq (.listFiles %)) [])
                           root)
            :when (.isFile file)]
      (when (Files/isSymbolicLink (.toPath file))
        (fail! :resource-link [:resources] source "Use regular project resource files."))
      (let [relative (.relativize (.toPath root) (.toPath file))
            target (io/file destination (str relative))]
        (io/make-parents target)
        (io/copy file target)))))

(defn stage-resources!
  "Stages declared resources directly in the publishable generation and hashes them once."
  [config generation base]
  (let [resources (vec
                    (concat
                      (when-let [directory (get-in config [:assets :directory])]
                        (let [source (resolve-path base directory)]
                          (when-not (.isDirectory source)
                            (fail! :resource-path
                                   [:assets :directory]
                                   directory
                                   "Supply an existing asset directory."))
                          [{:config-path [:assets :directory]
                            :source source
                            :path "resources/assets"}]))
                      (for [[index container] (map-indexed vector (:containers config))
                            :let [source (resolve-path base (:image container))]
                            :when (.isFile source)]
                        {:config-path [:containers index :image]
                         :source source
                         :path (str "resources/container-" index
                                    "/" (.getName source))})))]
    (doseq [{:keys [source path]} resources]
      (let [directory? (.isDirectory source)]
        (copy-tree! (if directory? source (.getParentFile source))
                    (io/file generation
                             (if directory? path (.getParent (io/file path)))))))
    {:resources resources
     :sha (sha (pr-str (for [file (sort-by str
                                           (file-seq (io/file generation "resources")))
                             :when (.isFile file)]
                         [(str (.relativize (.toPath generation) (.toPath file)))
                          (sha-bytes (Files/readAllBytes (.toPath file)))])))}))

(defn validate-clients!
  "Compiles generated RPC clients before the native generation can publish."
  [generation app staging]
  (compile! (assoc app
              :entry (client-namespace app)
              :paths (conj (vec (:paths app ["src"])) (str generation "/generated-src")))
            (str staging "/client-validation")))

(defn local-input-roots
  "Resolves current local dependency source roots for watch and coherent-generation checks."
  []
  (let [basis (when-let [file (System/getProperty "clojure.basis")] (read-edn file))
        selected (get-in basis [:basis-config :aliases])]
    (loop [pending [(io/file ".")]
           seen #{}
           roots []]
      (if-let [root (first pending)]
        (let [canonical (.getCanonicalPath root)
              file (io/file root "deps.edn")]
          (if (seen canonical)
            (recur (next pending) seen roots)
            (let [deps (if (.isFile file) (read-edn file) {})
                  overrides (apply merge
                              (map #(get-in deps [:aliases % :override-deps]) selected))
                  dependencies (merge (:deps deps)
                                      (apply merge
                                        (map #(get-in deps [:aliases % :extra-deps])
                                          selected))
                                      overrides)
                  children (for [[_ value] dependencies
                                 :when (:local/root value)]
                             (resolve-path root (:local/root value)))
                  paths (concat (:paths deps ["src"])
                                (mapcat #(get-in deps [:aliases % :extra-paths])
                                  selected))]
              (recur (concat (next pending) children)
                     (conj seen canonical)
                     (into roots
                           (cons (str file) (map #(str (resolve-path root %)) paths)))))))
        roots))))

(defn input-roots
  "Shares current application, dependency and authority-relative resource roots with dev."
  [app app-file]
  (let [native-file (:native-config app)
        base (if native-file (or (.getParent (io/file native-file)) ".") ".")
        config (if native-file
                 (try (read-native native-file) (catch Exception _ {}))
                 (merge (:config app) (get-in app [:profiles (:profile app :release)])))]
    (concat (local-input-roots)
            ["src" "resources" "dev" "deps.edn" "package-lock.json" app-file]
            (:paths app)
            [(:history app "class-history.edn") native-file]
            (when-let [directory (get-in config [:assets :directory])]
              [(str (resolve-path base directory))])
            (for [entry (:containers config)
                  :let [image (resolve-path base (:image entry))]
                  :when (.isFile image)]
              (.getParent image)))))

(defn input-files
  "Enumerates stable, distinct regular inputs while excluding generated and native state."
  [roots output]
  (let [output (.getCanonicalFile (io/file output))]
    (sort-by str
             (distinct
               (for [root (remove nil? roots)
                     file (tree-seq
                            #(and (.isDirectory %)
                                  (not= output (.getCanonicalFile %))
                                  (not (#{".git" ".celld" ".celld-build" "target"
                                          "node_modules" ".cpcache" ".tools" "scratch"}
                                        (.getName %))))
                            #(or (seq (.listFiles %)) [])
                            (io/file root))
                     :when (.isFile file)]
                 (.getCanonicalFile file))))))

(defn input-fingerprint
  "Hashes the shared input set so concurrent edits cannot publish a mixed generation."
  [app app-file]
  (sha (pr-str (for [file (input-files (input-roots app app-file)
                                       (:output app ".celld-build"))]
                 [(str file) (sha-bytes (Files/readAllBytes (.toPath file)))]))))

(defn- build-app!
  "Builds or inspects one minimal app.edn. Every invocation uses a fresh compiler
  environment, so deleted declarations cannot survive watch metadata."
  [app-file action]
  (when-not (#{:build :validate :inspect :prepare-config} action)
    (fail! :action [:action] action "Use build, validate, inspect or prepare-config."))
  (let [app (d/check! App (read-edn app-file) {:file app-file} :application [])
        _ (when (and (:native-config app) (or (:config app) (:profiles app)))
            (fail! :authoritative-config []
                   :both
                     "Choose generated or existing-native-file mode."))
        output (:output app ".celld-build")
        lock-file (io/file output ".build.lock")]
    (io/make-parents lock-file)
    (with-open [raf (java.io.RandomAccessFile. lock-file "rw")
                channel (.getChannel raf)
                _lock (.lock channel)]
      (let
        [_ (when-not (= app (read-edn app-file))
             (fail!
               :inputs-changed [:app]
               :lock-wait
                 "Rebuild the changed application input after acquiring its output lock."))
         input-sha (input-fingerprint app app-file)
         staging (str output "/staging/" (java.util.UUID/randomUUID))
         generation (io/file staging "generation")
         _ (.mkdirs generation)
         {:keys [descriptors js external-modules runtime-namespaces]} (compile! app
                                                                                staging)
         native-config (when-let [file (:native-config app)] (read-native file))
         history-file (:history app "class-history.edn")
         history (config/native-values
                   (if native-config
                     (:migrations native-config [])
                     (if (.exists (io/file history-file)) (read-edn history-file) [])))]
        (duplicates! descriptors :export)
        (duplicates! descriptors :binding)
        (when-let [profiles (:profiles app)]
          (doseq [profile (keys profiles)]
            (when (or (#{:inspect :prepare-config} action)
                      (not= profile (:profile app :release)))
              (config! (generated-config (assoc app :profile profile)
                                         descriptors
                                         history
                                         "bundle.mjs")
                       descriptors))))
        (case action
          :inspect (do (prn descriptors) {:descriptors descriptors})
          :prepare-config
            (let [adopted (set (mapcat :new_sqlite_classes history))
                  new-classes (vec (sort (remove adopted
                                           (map :export
                                             (cells descriptors)))))
                  candidate (cond-> history
                              (seq new-classes) (conj {:tag (str "v"
                                                                 (inc (count history)))
                                                       :new_sqlite_classes new-classes}))]
              (spit (str history-file ".candidate") (str (pr-str candidate) "\n"))
              (prn {:candidate (str history-file ".candidate") :new-classes new-classes})
              {:history candidate})
          (let
            [_ (history! history descriptors)
             preliminary-config
               (if native-config
                 (do
                   (when-not (= (set (map (juxt :binding :export) (cells descriptors)))
                                (set (map (juxt :name :class_name)
                                       (get-in native-config
                                               [:durable_objects :bindings]))))
                     (fail!
                       :export-config []
                       :mismatch
                         "Match native binding names and classes to compiled declarations."))
                   native-config)
                 (generated-config app descriptors history "bundle.mjs"))
             _ (binding [d/*source* (if-let [file (:native-config app)]
                                      {:file (.getAbsolutePath (io/file file))}
                                      d/*source*)]
                 (config! preliminary-config descriptors))
             client-source (generate-clients! generation app descriptors)
             _ (validate-clients! generation app staging)
             resource-inputs (stage-resources! preliminary-config
                                               generation
                                               (if-let [file (:native-config app)]
                                                 (or (.getParent (io/file file)) ".")
                                                 "."))
             bundle (package! js descriptors staging generation external-modules)
             artifact {:bundle-sha (sha (slurp bundle))
                       :client-source-sha (sha (slurp client-source))
                       :resources-sha (:sha resource-inputs)
                       :descriptors descriptors
                       :history history
                       :app app
                       :input-sha input-sha
                       :native-config preliminary-config
                       :target (:target target)
                       :runtime-namespaces runtime-namespaces
                       :external-modules external-modules}
             fingerprint (sha (pr-str artifact))
             generation-path #(str "generations/" fingerprint "/" %)
             relative-main (generation-path "bundle.mjs")
             config (reduce
                      (fn [config {:keys [config-path path]}]
                        (assoc-in config config-path (generation-path path)))
                      (assoc preliminary-config :main relative-main)
                      (:resources resource-inputs))
             manifest (assoc (select-keys artifact
                                          [:target :descriptors :runtime-namespaces
                                           :external-modules :client-source-sha :input-sha
                                           :resources-sha])
                        :fingerprint fingerprint
                        :main relative-main
                        :config-sha (sha (json/write-str config :key-fn names/text))
                        :clients (generation-path "generated-src")
                        :qualification :unrun)]
            (when-not (= input-sha (input-fingerprint app app-file))
              (fail! :inputs-changed []
                     :concurrent-edit "Wait for source edits to finish, then rebuild."))
            (when-not (= action :validate)
              (let [dest (io/file output "generations" fingerprint)
                    manifest-file (io/file dest "manifest.edn")]
                (if (.exists dest)
                  (when-not (and (.isFile manifest-file)
                                 (= manifest (read-edn manifest-file)))
                    (fail!
                      :generation-conflict [:generation fingerprint]
                      :immutable-generation
                        "Preserve this generation and correct the fingerprint inputs before rebuilding."))
                  (do
                    (io/make-parents dest)
                    (spit (io/file generation "manifest.edn") (pr-str manifest))
                    (Files/move (.toPath generation)
                                (.toPath dest)
                                (into-array StandardCopyOption
                                            [StandardCopyOption/ATOMIC_MOVE]))))
                ;; Config is the sole atomic generation pointer used by celld dev.
                (atomic! (str output "/wrangler.json")
                         (json/write-str config :key-fn names/text :escape-slash false))
                (atomic! (str output "/status.edn")
                         (pr-str {:state :current :fingerprint fingerprint}))))
            (prn (select-keys manifest [:fingerprint :main :qualification]))
            manifest))))))

(defn build!
  "Builds with the known input location attached to all source-less diagnostics."
  [app-file action]
  (binding [d/*source* {:file (.getAbsolutePath (io/file app-file))}]
    (build-app! app-file action)))

(defn -main
  "Runs a mandatory supported build action and emits bounded failure diagnostics."
  [& [action app-file]]
  (try (build! (or app-file "app.edn") (keyword (or action "build")))
       (shutdown-agents)
       (catch Throwable error
         (let [diagnostic (diagnostics/diagnostic error)]
           (binding [*out* *err*] (prn {:status :failed :diagnostic diagnostic}))
           (try (let [app (read-edn (or app-file "app.edn"))
                      _ (when (and (:native-config app)
                                   (or (:config app) (:profiles app)))
                          (fail! :authoritative-config []
                                 :both
                                   "Choose generated or existing-native-file mode."))
                      output (:output app ".celld-build")]
                  (atomic! (str output "/status.edn")
                           (pr-str {:state :stale :diagnostic diagnostic})))
                (catch Throwable _ nil)))
         (System/exit 1))))
