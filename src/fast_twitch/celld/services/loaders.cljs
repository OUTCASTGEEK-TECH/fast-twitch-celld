(ns fast-twitch.celld.services.loaders
  "Native Worker Loader code and handles. Loading is explicit; importing this
  namespace does not compile code or allocate native bindings. Disposal ends new
  calls on that load while native in-flight calls finish."
  (:require [malli.experimental :as mx]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.names :as names]
            [fast-twitch.celld.contracts :as contracts]))

(defn native
  "Returns the original native loader/stub/class/entrypoint capability."
  [handle]
  handle)

(def ^:private Module
  [:or [:fn #(not (map? %))]
   [:and
    (contracts/closed
      {:js :string :cjs :string :wasm :any :json :string :text :string :data :any})
    [:fn #(= 1 (count %))]]])

(def ^:private WorkerCode
  (contracts/closed
    {:mainModule :keyword
     :modules [:map-of :keyword Module]
     :compatibilityDate :string
     :compatibilityFlags [:vector :keyword]
     :env :any
     :globalOutbound :any
     :limits (:loader-limits contracts/schemas)
     :tails :any}
    #{:mainModule :modules :compatibilityDate}))

(mx/defn ^:dynamic code
  "Checks WorkerCode and projects option keys. Module names are keyword keys; modules
  may be native {wasm/data/...} records. Capabilities in env remain native."
  [options :- WorkerCode]
  (let [{:keys! [mainModule modules]} options]
    (when-not (contains? modules mainModule)
      (v/fail! :loader-code :module "Include mainModule in the module map."))
    (n/fields
      (cond-> options
        true (assoc :mainModule
               (names/text mainModule) :modules
               (n/fields (update-vals modules
                                      #(if (map? %) (n/fields %) %))))
        (map? (:env options)) (update :env n/fields)
        (map? (:limits options)) (update :limits n/fields)
        (:compatibilityFlags options) (update :compatibilityFlags
                                              #(to-array (mapv names/text %)))
        (:tails options) (update :tails to-array)))))

(mx/defn ^:dynamic get-worker
  "Synchronously returns a lazy native named load. get-code executes only on native
  first compilation, so callback failures appear at first use, not necessarily here."
  [loader id :- :string get-code :- [:fn fn?]]
  (n/invoke loader "get" [id get-code]))

(defn load
  "Synchronously returns an anonymous native load, preserving native lazy Promise errors."
  [loader native-code]
  (n/invoke loader "load" [native-code]))

(def ^:private EntrypointOptions
  (contracts/closed {:props :any :limits (:loader-limits contracts/schemas)}))

(mx/defn ^{:dynamic true} get-entrypoint
  "Returns a native Fetch/RPC entrypoint with checked props/limits; omission is preserved."
  ([worker]
   (n/invoke worker "getEntrypoint" []))
  ([worker name]
   (n/invoke worker "getEntrypoint" [(names/identifier name)]))
  ([worker name options :- EntrypointOptions]
   (when (contains? options :props) (codec/native-value! (:props options)))
   (n/invoke worker
             "getEntrypoint"
             [(names/identifier name)
              (n/option-fields (cond-> options
                                 (map? (:limits options)) (update :limits
                                                                  n/option-fields)))])))

(mx/defn ^{:dynamic true} get-cell-class
  "Returns the native plain-Cell class handle used by facets. Only props is supported."
  ([worker]
   (n/invoke worker "getDurableObjectClass" []))
  ([worker name]
   (n/invoke worker "getDurableObjectClass" [(names/identifier name)]))
  ([worker name options :- (contracts/closed {:props :any})]
   (when (contains? options :props) (codec/native-value! (:props options)))
   (n/invoke worker
             "getDurableObjectClass"
             [(names/identifier name) (n/option-fields options)])))

(defn dispose!
  "Explicitly releases the native load synchronously; do not persist derived handles."
  [worker]
  (n/invoke worker "dispose" []))

(v/instrument! code get-worker get-entrypoint get-cell-class)
