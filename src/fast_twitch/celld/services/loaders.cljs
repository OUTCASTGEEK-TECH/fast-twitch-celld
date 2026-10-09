(ns fast-twitch.celld.services.loaders
  "Native Worker Loader code and handles. Loading is explicit; importing this
  namespace does not compile code or allocate native bindings. Disposal ends new
  calls on that load while native in-flight calls finish."
  (:require [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.names :as names]))

(defn native
  "Returns the original native loader/stub/class/entrypoint capability."
  [handle]
  handle)

(defn- module-value!
  [module]
  (when (map? module)
    (when-not (= 1 (count module))
      (v/fail! :loader-module :shape "Supply exactly one native module kind."))
    (let [[kind value] (first module)]
      (when (#{:js :cjs :text :json} kind) (v/check! :string value :loader-module))))
  module)

(defn code
  "Checks WorkerCode and projects option keys. Module names are keyword keys; modules
  may be native {wasm/data/...} records. Capabilities in env remain native."
  [options]
  (let [{:keys! [mainModule modules compatibilityDate]} options]
    (v/check! :string mainModule :loader-main)
    (v/check! :string compatibilityDate :loader-date)
    (v/check! [:map-of :keyword :any] modules :loader-modules)
    (when-not (contains? modules (keyword mainModule))
      (v/fail! :loader-code :module "Include mainModule in the module map."))
    (n/options
      (cond-> options
        true (assoc :modules
               (reduce-kv (fn [out name module]
                            (module-value! module)
                            (aset out
                                  (names/text name)
                                  (if (map? module)
                                    (n/options module
                                               #{:js :cjs :wasm :json :text :data}
                                               :loader-module)
                                    module))
                            out)
                          (js-obj)
                          modules))
        (map? (:env options)) (update :env #(n/options % (set (keys %)) :loader-env))
        (map? (:limits options))
          (update :limits #(n/options % #{:cpuMs :subRequests} :loader-limits))
        (:compatibilityFlags options) (update :compatibilityFlags
                                              #(to-array (mapv names/text %)))
        (:tails options) (update :tails to-array))
      #{:mainModule :modules :compatibilityDate :compatibilityFlags :env :globalOutbound
        :limits :tails}
      :loader-code)))

(defn get-worker
  "Synchronously returns a lazy native named load. get-code executes only on native
  first compilation, so callback failures appear at first use, not necessarily here."
  [loader id get-code]
  (v/check! :string id :loader-id)
  (v/check! [:fn fn?] get-code :loader-code-callback)
  (n/invoke loader "get" [id get-code]))

(defn load
  "Synchronously returns an anonymous native load, preserving native lazy Promise errors."
  [loader native-code]
  (n/invoke loader "load" [native-code]))

(defn get-entrypoint
  "Returns a native Fetch/RPC entrypoint with checked props/limits; omission is preserved."
  ([worker]
   (n/invoke worker "getEntrypoint" []))
  ([worker name]
   (n/invoke worker "getEntrypoint" [(names/identifier name)]))
  ([worker name options]
   (when (contains? options :props) (codec/native-value! (:props options)))
   (n/invoke worker
             "getEntrypoint"
             [(names/identifier name)
              (n/options
                (cond-> options
                  (map? (:limits options))
                    (update :limits #(n/options % #{:cpuMs :subRequests} :loader-limits)))
                #{:props :limits}
                :loader-entrypoint)])))

(defn get-cell-class
  "Returns the native plain-Cell class handle used by facets. Only props is supported."
  ([worker]
   (n/invoke worker "getDurableObjectClass" []))
  ([worker name]
   (n/invoke worker "getDurableObjectClass" [(names/identifier name)]))
  ([worker name options]
   (when (contains? options :props) (codec/native-value! (:props options)))
   (n/invoke worker
             "getDurableObjectClass"
             [(names/identifier name) (n/options options #{:props} :loader-class)])))

(defn dispose!
  "Explicitly releases the native load synchronously; do not persist derived handles."
  [worker]
  (n/invoke worker "dispose" []))
