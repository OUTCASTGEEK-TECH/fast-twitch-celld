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
  "Reads JSON/JSONC using a string-aware comment/trailing-comma scanner."
  [file]
  (let [source (slurp file)
        out (StringBuilder.)
        length (count source)
        clean (loop [i 0
                     state :normal
                     escape? false]
                (if (>= i length)
                  (str out)
                  (let [c (.charAt source i)
                        next (when (< (inc i) length) (.charAt source (inc i)))]
                    (case state
                      :line (if (= c \newline)
                              (do (.append out c) (recur (inc i) :normal false))
                              (recur (inc i) :line false))
                      :block (if (and (= c \*) (= next \/))
                               (recur (+ i 2) :normal false)
                               (recur (inc i) :block false))
                      :string (do (.append out c)
                                  (recur (inc i)
                                         (if (and (= c \") (not escape?)) :normal :string)
                                         (and (= c \\) (not escape?))))
                      (cond (and (= c \/) (= next \/)) (recur (+ i 2) :line false)
                            (and (= c \/) (= next \*)) (do (.append out \space)
                                                           (recur (+ i 2) :block false))
                            (= c \") (do (.append out c) (recur (inc i) :string false))
                            :else (do (.append out c) (recur (inc i) :normal false)))))))
        result (StringBuilder.)]
    (loop [i 0
           string? false
           escape? false]
      (when (< i (count clean))
        (let [c (.charAt clean i)
              after (when (and (= c \,) (not string?))
                      (first (drop-while #(Character/isWhitespace ^char %)
                                         (subs clean (inc i)))))
              skip? (and (= c \,) (not string?) (#{\] \}} after))]
          (when-not skip? (.append result c))
          (recur (inc i)
                 (if (and (= c \") (not escape?)) (not string?) string?)
                 (and (= c \\) (not escape?))))))
    (json/read-str (str result) :key-fn keyword)))

(defn sha
  "Returns a deterministic SHA-256 digest of UTF-8 input."
  [text]
  (format "%064x"
          (BigInteger. 1
                       (.digest (MessageDigest/getInstance "SHA-256")
                                (.getBytes (str text) "UTF-8")))))

(defn fail!
  "Throws a bounded structured build diagnostic."
  [code path received repair]
  (d/fail! code nil :application path :valid-configuration received repair))

(def app-keys
  #{:name :entry :paths :output :history :optimizations :native-config :config :profile
    :profiles :extensions})

(defn closed-map!
  "Rejects undeclared configuration keys before effects."
  [value allowed path]
  (when-not (map? value) (fail! :configuration path (type value) "Supply a map."))
  (doseq [key (keys value)]
    (when-not (allowed key)
      (fail! :unknown-key (conj path key) key "Remove unsupported configuration key.")))
  value)

(defn config!
  "Checks whole-application native configuration against selected declarations."
  [config descriptors]
  (d/check! contracts/configuration config nil :application [:config])
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
  (let [rpc? (some #(some (fn [h]
                            (= :rpc (:kind h)))
                          (:handlers %))
                   descriptors)]
    (when (and rpc? (not (some #{"js_rpc"} (:compatibility_flags config))))
      (fail! :required-flag [:compatibility_flags]
             "js_rpc" "Keep js_rpc for plain Cell RPC.")))
  (config/validate! config descriptors))

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
  [compiler entry]
  (->> (:cljs.analyzer/namespaces @compiler)
       (filter (fn [[ns _]]
                 ((reachable-namespaces compiler entry) ns)))
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

(defn history!
  "Checks compiled classes against explicitly adopted SQLite history."
  [history descriptors]
  (doseq [record history] (closed-map! record #{:tag :new_sqlite_classes} [:history]))
  (let [adopted (set (mapcat :new_sqlite_classes history))
        current (set (map :export
                       (filter #(and (= :cell (:kind %)) (not (:facet? %)))
                         descriptors)))]
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
  (let [cells (filter #(and (= :cell (:kind %)) (not (:facet? %))) ds)
        workflows (filter #(= :workflow (:kind %)) ds)
        required (when (some #(some (fn [h]
                                      (= :rpc (:kind h)))
                                    (:handlers %))
                             ds)
                   ["js_rpc"])
        base {:name (:name app)
              :main main
              :no_bundle true
              :compatibility_date (:compatibility-date target)
              :compatibility_flags (vec required)
              :durable_objects {:bindings (mapv (fn [x]
                                                  {:name (:binding x)
                                                   :class_name (:export x)})
                                            cells)}
              :migrations history}
        crons (vec (mapcat #(mapcat (fn [h]
                                      (get-in h [:options :crons] []))
                              (:handlers %))
                     ds))
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
        loader (clojure.lang.DynamicClassLoader. (clojure.lang.RT/baseLoader))
        entry (io/file staging "entry-src/fast_twitch/celld/build_entry.cljs")
        write-entry (fn [namespaces]
                      (spit entry
                            (pr-str (list 'ns
                                          'fast-twitch.celld.build-entry
                                          (list* :require (map vector namespaces))))))]
    (doseq [path (:paths app ["src"])] (.addURL loader (.toURL (.toURI (io/file path)))))
    (io/make-parents entry)
    (write-entry [(:entry app)])
    (.addURL loader (.toURL (.toURI (io/file staging "entry-src"))))
    (.setContextClassLoader thread loader)
    (try
      (with-bindings {clojure.lang.Compiler/LOADER loader}
        (binding [env/*compiler* compiler]
          (binding [analyzer/*cljs-warnings* (zipmap (keys analyzer/*cljs-warnings*)
                                                     (repeat false))]
            (compiler/with-core-cljs))
          (binding [analyzer/*cljs-warning-handlers* [strict-warning!]]
            (analyzer/analyze-file entry)))
        (let [runtime (sort (filter #(str/starts-with? (str %) "fast-twitch.celld.")
                              (reachable-namespaces compiler (:entry app))))]
          (write-entry (distinct (concat runtime [(:entry app)]))))
        (cljs/build entry
                    {:main 'fast-twitch.celld.build-entry
                     :optimizations :simple
                     :output-to js
                     :output-dir (str staging "/cljs")
                     :target :bundle
                     :warning-handlers [strict-warning!]
                     :infer-externs true
                     :source-map false
                     :pretty-print false
                     :parallel-build false}
                    compiler))
      (finally (.setContextClassLoader thread previous-loader)))
    {:descriptors (descriptors compiler (:entry app))
     :js js
     :runtime-namespaces (vec (sort (map str
                                      (reachable-namespaces compiler (:entry app)))))
     :external-modules
       (vec (sort (for [namespace (reachable-namespaces compiler (:entry app))
                        :let [specifier (str namespace)]
                        :when (and (or (str/starts-with? specifier "node:")
                                       (str/starts-with? specifier "cloudflare:"))
                                   (get-in @compiler
                                           [:js-dependency-index specifier :external?]))]
                    specifier)))
     :native-imports (cond-> #{}
                       ((reachable-namespaces compiler (:entry app))
                         'fast-twitch.celld.communication)
                         (conj :tcp)
                       ((reachable-namespaces compiler (:entry app))
                         'fast-twitch.celld.services.workflows)
                         (conj :workflow-errors))}))

(defn package!
  "Packages the final native ESM artifact and automatic named exports."
  [js ds staging native-imports external-modules]
  (doseq [specifier external-modules]
    (when-not (supported-native-modules specifier)
      (fail! :runtime-import
             [:imports specifier]
             specifier
             "Import a module implemented by the pinned Celld target.")))
  (let
    [raw (str staging "/entry.js")
     output (str staging "/bundle.mjs")
     bases? (some #(or (= :workflow (:kind %))
                       (and (= :worker (:kind %)) (not= "default" (:export %))))
                  ds)
     imports
       (str
         (apply str
           (map-indexed (fn [index specifier]
                          (str "import * as ft_native_module_"
                               index
                               " from "
                               (json/write-str specifier)
                               ";\nglobalThis["
                               (json/write-str specifier)
                               "]=ft_native_module_"
                               index
                               ";\n"))
                        external-modules))
         (when (native-imports :tcp)
           "import {connect as ft_native_connect} from 'cloudflare:sockets';\nglobalThis.__ft_connect=ft_native_connect;\n")
         (when (native-imports :workflow-errors)
           "import {NonRetryableError} from 'cloudflare:workflows';\nglobalThis.__ft_NonRetryableError=NonRetryableError;\n")
         (when bases?
           "import {WorkerEntrypoint, WorkflowEntrypoint} from 'cloudflare:workers';\nglobalThis.__ft_WorkerEntrypoint=WorkerEntrypoint; globalThis.__ft_WorkflowEntrypoint=WorkflowEntrypoint;\n"))
     exports (str/join "\n"
                       (map-indexed (fn [i x]
                                      (str "const ft_export_"
                                           i
                                           "=globalThis["
                                           (json/write-str (:export-root x))
                                           "];\nexport {ft_export_"
                                           i
                                           " as "
                                           (:export x)
                                           "};"))
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

(defn generate-clients!
  "Writes deterministic case-preserving RPC clients into staging."
  [output app descriptors]
  (let
    [ns (symbol (str "fast-twitch.celld.generated." (:name app)))
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
                                     (names/identifier (get-in handler
                                                               [:options :method]))
                                     "!"))
               options (:options handler)]
           (list
             'defn
             (with-meta function {:async true})
             (str
               "Native "
               (:export cell)
               "."
               (:method options)
               " client. Receives its stub first; schemas/codecs match the server. No implementation namespace is loaded.")
             '[stub & args]
             (list 'cljs.core/await
                   (list 'rpc/call!
                         'stub
                         (:method options)
                         '(vec args)
                         {:args-schema (:args options)
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
  "Copies and hashes declared assets and local container contexts into staging."
  [config staging base]
  (let [resources (atom {})]
    (when-let [directory (get-in config [:assets :directory])]
      (let [source (resolve-path base directory)]
        (when-not (.isDirectory source)
          (fail! :resource-path
                 [:assets :directory]
                 directory
                 "Supply an existing asset directory."))
        (copy-tree! source (str staging "/resources/assets"))
        (swap! resources assoc :assets "resources/assets")))
    (doseq [[index container] (map-indexed vector (:containers config))]
      (let [source (resolve-path base (:image container))]
        (when (.isFile source)
          (let [destination (str "resources/container-" index)]
            (copy-tree! (.getParentFile source) (str staging "/" destination))
            (swap! resources assoc index (str destination "/" (.getName source)))))))
    {:paths @resources
     :sha (sha (pr-str (for [file (sort-by str (file-seq (io/file staging "resources")))
                             :when (.isFile file)]
                         [(str (.relativize (.toPath (io/file staging)) (.toPath file)))
                          (format "%064x"
                                  (BigInteger.
                                    1
                                    (.digest (MessageDigest/getInstance "SHA-256")
                                             (Files/readAllBytes (.toPath file)))))])))}))

(defn validate-clients!
  "Compiles generated RPC clients before the native generation can publish."
  [_source staging app]
  (compile! (assoc app
              :entry (symbol (str "fast-twitch.celld.generated." (:name app)))
              :paths (conj (vec (:paths app ["src"])) (str staging "/generated-src")))
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

(defn input-fingerprint
  "Hashes compile/config/resource inputs; a concurrent source edit cannot publish a mixed generation."
  [app app-file]
  (let [base (if-let [file (:native-config app)]
               (or (.getParent (io/file file)) ".")
               ".")
        config (if-let [file (:native-config app)]
                 (read-native file)
                 (merge (:config app) (get-in app [:profiles (:profile app :release)])))
        roots (concat (local-input-roots)
                      ["src" "resources" "dev" "../fast-twitch/src"
                       "../fast-twitch/resources" "deps.edn" "package-lock.json" app-file]
                      (:paths app)
                      [(:history app "class-history.edn") (:native-config app)]
                      [(when-let [directory (get-in config [:assets :directory])]
                         (str (resolve-path base directory)))]
                      (for [entry (:containers config)
                            :let [image (resolve-path base (:image entry))]
                            :when (.isFile image)]
                        (.getParent image)))
        files (distinct (for [root (remove nil? roots)
                              file (tree-seq #(and (.isDirectory %)
                                                   (not (#{".git" ".celld" ".celld-build"
                                                           "target" "node_modules"}
                                                         (.getName %))))
                                             #(or (seq (.listFiles %)) [])
                                             (io/file root))
                              :when (.isFile file)]
                          file))]
    (sha (pr-str (for [file (sort-by str files)]
                   [(str file)
                    (format "%064x"
                            (BigInteger. 1
                                         (.digest (MessageDigest/getInstance "SHA-256")
                                                  (Files/readAllBytes (.toPath
                                                                        file)))))])))))

(defn- build-app!
  "Builds or inspects one minimal app.edn. Every invocation uses a fresh compiler
  environment, so deleted declarations cannot survive watch metadata."
  [app-file action]
  (when-not (#{:build :validate :inspect :prepare-config} action)
    (fail! :action [:action] action "Use build, validate, inspect or prepare-config."))
  (let [app (closed-map! (read-edn app-file) app-keys [])
        _ (when-not (symbol? (:entry app))
            (fail! :entry [:entry] (:entry app) "Supply the entry namespace symbol."))
        _ (when-not (= :simple (:optimizations app :simple))
            (fail! :optimization
                   [:optimizations]
                   (:optimizations app)
                   "Only qualified :simple is currently enabled."))
        output (:output app ".celld-build")
        lock-file (io/file output ".build.lock")]
    (io/make-parents lock-file)
    (with-open [raf (java.io.RandomAccessFile. lock-file "rw")
                channel (.getChannel raf)
                _lock (.lock channel)]
      (let
        [_ (when-not (= app (closed-map! (read-edn app-file) app-keys []))
             (fail!
               :inputs-changed [:app]
               :lock-wait
                 "Rebuild the changed application input after acquiring its output lock."))
         input-sha (input-fingerprint app app-file)
         staging (str output "/staging/" (java.util.UUID/randomUUID))
         _ (.mkdirs (io/file staging))
         {:keys [descriptors js native-imports external-modules runtime-namespaces]}
           (compile! app staging)
         history-file (:history app "class-history.edn")
         history (config/native-values
                   (if-let [file (:native-config app)]
                     (:migrations (read-native file) [])
                     (if (.exists (io/file history-file)) (read-edn history-file) [])))]
        (duplicates! descriptors :export)
        (duplicates! descriptors :binding)
        (when-let [profiles (:profiles app)]
          (when (:native-config app)
            (fail! :authoritative-config
                   [:profiles]
                   profiles
                   "Existing-native-file mode has one configuration authority."))
          (when-not (and (map? profiles) (every? keyword? (keys profiles)))
            (fail! :profile [:profiles] profiles "Use a map of named profile maps."))
          (doseq [[profile value] profiles]
            (when-not (map? value)
              (fail! :profile
                     [:profiles profile]
                     value
                     "Supply a profile configuration map."))
            (config! (generated-config (assoc app :profile profile)
                                       descriptors
                                       history
                                       "bundle.mjs")
                     descriptors)))
        (case action
          :inspect (do (prn descriptors) {:descriptors descriptors})
          :prepare-config
            (let [adopted (set (mapcat :new_sqlite_classes history))
                  new-classes (vec (sort (remove adopted
                                           (map :export
                                             (filter #(and (= :cell (:kind %))
                                                           (not (:facet? %)))
                                               descriptors)))))
                  candidate (cond-> history
                              (seq new-classes) (conj {:tag (str "v"
                                                                 (inc (count history)))
                                                       :new_sqlite_classes new-classes}))]
              (spit (str history-file ".candidate") (str (pr-str candidate) "\n"))
              (prn {:candidate (str history-file ".candidate") :new-classes new-classes})
              {:history candidate})
          (let
            [_ (history! history descriptors)
             client-source (generate-clients! staging app descriptors)
             _ (validate-clients! client-source staging app)
             client-fingerprint (sha (slurp client-source))
             preliminary-config (if-let [file (:native-config app)]
                                  (read-native file)
                                  (generated-config app descriptors history "bundle.mjs"))
             _ (binding [d/*source* (if-let [file (:native-config app)]
                                      {:file (.getAbsolutePath (io/file file))}
                                      d/*source*)]
                 (config! preliminary-config descriptors))
             resource-inputs (stage-resources! preliminary-config
                                               staging
                                               (if-let [file (:native-config app)]
                                                 (or (.getParent (io/file file)) ".")
                                                 "."))
             bundle (package! js descriptors staging native-imports external-modules)
             fingerprint (sha (pr-str {:bundle-sha (sha (slurp bundle))
                                       :client-sha client-fingerprint
                                       :resources-sha (:sha resource-inputs)
                                       :descriptors descriptors
                                       :history history
                                       :app app
                                       :input-sha input-sha
                                       :native-config preliminary-config
                                       :target (:target target)
                                       :runtime-namespaces runtime-namespaces
                                       :external-modules external-modules}))
             relative-main (str "generations/" fingerprint "/bundle.mjs")
             config
               (if-let [file (:native-config app)]
                 (let [native (read-native file)]
                   (when (or (:config app) (:profiles app))
                     (fail! :authoritative-config []
                            :both "Choose generated or existing-native-file mode."))
                   (when-not (= (set (map (fn [d] [(:binding d) (:export d)])
                                       (filter #(and (= :cell (:kind %))
                                                     (not (:facet? %)))
                                         descriptors)))
                                (set (map (fn [d] [(:name d) (:class_name d)])
                                       (get-in native [:durable_objects :bindings]))))
                     (fail!
                       :export-config []
                       :mismatch
                         "Match native binding names and classes to compiled declarations."))
                   (assoc native :main relative-main))
                 (generated-config app descriptors history relative-main))
             config (cond-> config
                      (get-in resource-inputs [:paths :assets])
                        (assoc-in [:assets :directory]
                          (str "generations/" fingerprint
                               "/" (get-in resource-inputs [:paths :assets])))
                      (seq (:containers config))
                        (update
                          :containers
                          (fn [containers]
                            (mapv (fn [index entry]
                                    (if-let [path (get-in resource-inputs [:paths index])]
                                      (assoc entry
                                        :image (str "generations/" fingerprint "/" path))
                                      entry))
                              (range)
                              containers))))
             _ (config! config descriptors)
             manifest {:target (:target target)
                       :fingerprint fingerprint
                       :descriptors descriptors
                       :main relative-main
                       :runtime-namespaces runtime-namespaces
                       :external-modules external-modules
                       :config-sha (sha (json/write-str config :key-fn names/text))
                       :client-source-sha client-fingerprint
                       :input-sha input-sha
                       :resources-sha (:sha resource-inputs)
                       :clients (str "generations/" fingerprint "/generated-src")
                       :qualification :unrun}]
            (when-not (= input-sha (input-fingerprint app app-file))
              (fail! :inputs-changed []
                     :concurrent-edit "Wait for source edits to finish, then rebuild."))
            (when-not (= action :validate)
              (let [dest (io/file output relative-main)
                    manifest-file (io/file (.getParentFile dest) "manifest.edn")]
                (if (.exists manifest-file)
                  (when-not (= manifest (read-edn manifest-file))
                    (fail!
                      :generation-conflict [:generation fingerprint]
                      :immutable-generation
                        "Preserve this generation and correct the fingerprint inputs before rebuilding."))
                  (do
                    (io/make-parents dest)
                    (io/copy (io/file bundle) dest)
                    (let [clients-dest (io/file (.getParentFile dest) "generated-src")]
                      (doseq [file (file-seq (io/file staging "generated-src"))
                              :when (.isFile file)]
                        (let [relative (.relativize (.toPath (io/file staging
                                                                      "generated-src"))
                                                    (.toPath file))
                              target-file (io/file clients-dest (str relative))]
                          (io/make-parents target-file)
                          (io/copy file target-file))))
                    (when (.isDirectory (io/file staging "resources"))
                      (copy-tree! (io/file staging "resources")
                                  (io/file (.getParentFile dest) "resources")))
                    (spit manifest-file (pr-str manifest))))
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
                      output (:output app ".celld-build")]
                  (atomic! (str output "/status.edn")
                           (pr-str {:state :stale :diagnostic diagnostic})))
                (catch Throwable _ nil)))
         (System/exit 1))))
