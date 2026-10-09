(ns fast-twitch.celld.macros
  "Focused native declarations compose into one directly exported CLJS constructor.
  Macro diagnostics are mandatory and carry source locations and repair guidance."
  (:require [cljs.analyzer.api :as ana]
            [cljs.analyzer :as analyzer]
            [fast-twitch.celld.definition :as d]
            [clojure.string :as str]
            [fast-twitch.celld.names :as names]))

(defn- expansion
  [form env sym]
  {:env env
   :source (merge (select-keys (meta form) [:line :column :end-line :end-column])
                  {:file (or (:file env) analyzer/*cljs-file*)})
   :name (symbol (str (get-in env [:ns :name])) (str sym))})

(defn- bad!
  [{:keys [source name]} code path expected value repair]
  (d/fail! code source name path expected value repair))

(defn- resolve-var
  [{:keys [env] :as context} sym]
  (when-not (symbol? sym)
    (bad! context
          :reference
          []
          :symbol
          sym
          "Use a declaration or ordinary function symbol."))
  (let [resolved (ana/resolve env sym)]
    (when-not (and resolved (:name resolved) (:op resolved))
      (bad! context
            :unresolved
            []
            :resolved-symbol
            sym
            "Require and define the referenced declaration before use."))
    resolved))

(defn- schema-options
  [{:keys [source name] :as context} options]
  (into
    options
    (map
      (fn [[key value]]
        (let
          [schema
             (if (symbol? value)
               (or
                 (:fast-twitch.celld/schema (resolve-var context value))
                 (bad!
                   context
                   :schema-reference
                   [key]
                   :portable-reference
                   value
                   "Define a literal schema with defcontract before using its reference."))
               value)]
          [key (d/schema! schema source name [key])]))
      (select-keys options [:args :returns]))))

(defmacro defcontract
  "Defines a portable literal schema with compiler metadata for deterministic references.
  No application code is evaluated on the JVM and no runtime registration occurs."
  [sym schema]
  (let [{:keys [source name]} (expansion &form &env sym)]
    (d/schema! schema source name [:schema]))
  `(def ~(with-meta sym
           {:doc "Portable boundary contract, resolved statically by native declarations."
            :fast-twitch.celld/schema schema})
     ~schema))

(defn- signature
  [context handler]
  (let [literal? (vector? handler)
        resolved (when-not literal? (resolve-var context handler))
        declared (:arglists resolved)
        declared (if (= 'quote (first declared)) (second declared) declared)
        params (when-not (:declared resolved) (:method-params resolved))
        arglists (if literal? [handler] declared)
        variadic-min (if params
                       (when (:variadic? resolved) (:max-fixed-arity resolved))
                       (some (fn [argv]
                               (when (some #{'&} argv)
                                 (count (take-while #(not= '& %) argv))))
                             arglists))
        fixed (if params
                (map count
                  (if variadic-min
                    (remove #(= (inc variadic-min) (count %)) params)
                    params))
                (map count (remove #(some #{'&} %) arglists)))]
    {:name (if literal? (:name context) (:name resolved))
     :known? (boolean (or params arglists))
     :fixed (set fixed)
     :variadic-min variadic-min}))

(defn- arity!
  [context {:keys [known? fixed variadic-min] :as signature} expected path]
  (when (and known?
             (not (or (fixed expected)
                      (and variadic-min (<= variadic-min expected)))))
    (bad! context
          :arity
          path
          expected
          (dissoc signature :name)
          "Match the handler signature, including context, to its declared arguments.")))

(defn- exact-arguments!
  [context argv expected]
  (when-not (and (vector? argv) (= expected (count argv)) (not (some #{'&} argv)))
    (bad! context
          :arity
          [:arguments]
          expected
          argv
          "Use the documented native handler arguments, including context.")))

(defn- normalize-handler
  [kind {:keys [source name] :as context} options handler path]
  (let [options (schema-options context options)
        sig (signature context handler)
        args (:args options)
        member (when (= :rpc kind)
                 (d/native-name! (:method options) source name (conj path :method)))
        expected (or ({:init 1 :fetch 2 :alarm 2 :queue 2 :scheduled 2} kind)
                     (when (and (= kind :rpc)
                                (vector? args)
                                (= :cat (first args))
                                (every? #(not (and (vector? %) (#{:* :+} (first %))))
                                        (rest args)))
                       (count args)))]
    (d/closed! kind options source name)
    (when (= kind :rpc)
      (when (contains? d/cell-events member)
        (d/fail! (if (vector? handler) :reserved-member :duplicate-event)
                 source
                 name
                 (conj path :method)
                 :non-event-method (:method options)
                 "Choose a non-event RPC method name." {:sources [source]})))
    (when expected
      (arity! context sig expected path))
    (cond->
      {:kind kind
       :source source
       :name (:name sig)
       :handler (:name sig)
       :options options
       :signature (dissoc sig :name)}
      member (assoc :member member))))

(defn- focused
  [kind context sym forms]
  (let [[doc forms] (if (string? (first forms)) [(first forms) (next forms)] [nil forms])
        [options forms] (if (map? (first forms)) [(first forms) (next forms)] [{} forms])
        argv (first forms)]
    (when-not (vector? argv)
      (bad! context :arity [] :vector argv "Supply a handler argument vector."))
    (when-let [expected ({:init 1 :fetch 2 :alarm 2 :queue 2 :scheduled 2} kind)]
      (exact-arguments! context argv expected))
    (let [descriptor (normalize-handler kind context options argv [])]
      `(defn ~(with-meta sym
                (merge (meta sym)
                       {:doc (or doc (str "Native " (name kind) " handler."))
                        :fast-twitch.celld/descriptor descriptor}))
         ~argv
         ~@(next forms)))))

(defmacro defcell-init
  "Defines one context initializer, selected explicitly by defcell. The constructor establishes its native gate synchronously; the body may return a Promise."
  [sym & forms]
  (focused :init (expansion &form &env sym) sym forms))

(defmacro deffetch
  "Defines a (context, request-map) handler. :native? passes a native Request. Successful results use one shared response finalization path."
  [sym & forms]
  (focused :fetch (expansion &form &env sym) sym forms))

(defmacro defrpc
  "Defines a native RPC handler with context followed by application arguments. :method is a stable native name; :args/:returns and :codec check both boundaries."
  [sym & forms]
  (focused :rpc (expansion &form &env sym) sym forms))

(defmacro defalarm
  "Defines a (context, alarm-info-map) native alarm handler. It schedules no alarm and owns no additional native event."
  [sym & forms]
  (focused :alarm (expansion &form &env sym) sym forms))

(defmacro defqueue-handler
  "Defines a Worker-only (context, native-batch) handler. Return/Promise completion controls native settlement; include it explicitly in defworker."
  [sym & forms]
  (focused :queue (expansion &form &env sym) sym forms))

(defmacro defscheduled-handler
  "Defines a Worker-only (context, native-controller) callback. :crons declares UTC native schedules and the handler retains no Cell alarm scope."
  [sym & forms]
  (focused :scheduled (expansion &form &env sym) sym forms))

(defn- socket-options!
  [{:keys [source name] :as context} options path]
  (d/closed! :websocket options source name)
  (doseq [key [:message :close :error]
          :when (contains? options key)]
    (let [sig (signature context (key options))]
      (when-not (:known? sig)
        (bad!
          context
          :arity
          (conj path key)
          3
          (key options)
          "Use a CLJS function with the documented arguments; wrap foreign callables in an explicit adapter."))
      (arity! context sig 3 (conj path key))))
  options)

(defmacro defwebsocket-handlers
  "Defines static hibernation/resident dispatch metadata. Handler symbols accept
  context, native socket and the shared event map; version must be a positive integer."
  [sym options]
  (let [{:keys [source name] :as context} (expansion &form &env sym)]
    (socket-options! context options [])
    `(def
       ~(with-meta sym
          {:fast-twitch.celld/descriptor
             {:kind :websocket :name name :handler name :source source :options options}})
       ~options)))

(defn- descriptor-reference
  [context ref allowed path]
  (let [descriptor (:fast-twitch.celld/descriptor (resolve-var context ref))]
    (when-not (and descriptor (allowed (:kind descriptor)))
      (bad! context
            :scope
            path
            allowed
            ref
            "Select a declaration of this entrypoint's scope."))
    descriptor))

(defn- selected
  [{:keys [source name] :as context} options allowed]
  (let [included (mapv #(descriptor-reference context % allowed [:include])
                   (:include options []))
        authored (concat (for [kind [:init :fetch :alarm :queue :scheduled]
                               :when (contains? options kind)]
                           [kind (kind options) [kind] nil])
                         (for [[method spec] (:rpc options)]
                           [:rpc spec [:rpc method] method]))
        ordinary (for [[kind spec path method] authored
                       :let [spec (if (symbol? spec) {:handler spec} spec)]]
                   (normalize-handler kind
                                      context
                                      (cond-> (dissoc spec :handler)
                                        method (assoc :method method))
                                      (:handler spec)
                                      path))
        sockets (when-let [spec (:websocket options)]
                  (if (symbol? spec)
                    (descriptor-reference context spec #{:websocket} [:websocket])
                    {:kind :websocket
                     :handler spec
                     :source source
                     :options (socket-options! context spec [:websocket])}))
        all (vec (concat included ordinary (when sockets [sockets])))
        identity (fn [descriptor]
                   (if (= :rpc (:kind descriptor))
                     [:rpc (:member descriptor)]
                     [(:kind descriptor)]))]
    (doseq [[key values] (group-by identity all)
            :when (> (count values) 1)]
      (d/fail! :duplicate-event
               source
               name
               [:include]
               :single-handler (count values)
               "Select exactly one handler for each native event/method."
                 {:native-event key :sources (mapv :source values)}))
    all))

(defn- event-name
  [kind]
  ({:fetch "fetch" :alarm "alarm" :queue "queue" :scheduled "scheduled"} kind))

(defn- runtime-requires
  [namespaces]
  (cons 'cljs.core/require (map #(list 'quote %) namespaces)))

(defn- native-method
  [{:keys [kind handler options member]} default-worker?]
  (when-let [method (if (= :rpc kind)
                      member
                      (event-name kind))]
    (let [event (gensym "event")
          args (gensym "args")
          env (gensym "env")
          ctx (gensym "ctx")
          this (gensym "this")
          context (if default-worker?
                    `(fast-twitch.celld.context/worker ~env ~ctx)
                    `(fast-twitch.celld.context/of ~this))
          parameters (cond (= :rpc kind) ['& args]
                           default-worker? [event env ctx]
                           :else [event])
          call (case kind
                 :fetch `(fast-twitch.celld.http/handle! ~handler
                                                         ~context
                                                         ~event
                                                         ~(:native? options false))
                 :alarm `(~handler
                          ~context
                          {:retry-count (aget ~event "retryCount")
                           :is-retrying? (aget ~event "isRetry")
                           :scheduled-time (aget ~event "scheduledTime")})
                 :rpc `(fast-twitch.celld.rpc/serve! ~handler
                                                     ~context
                                                     (vec ~args)
                                                     ~{:args-schema (:args options)
                                                       :returns (:returns options)
                                                       :codec (:codec options)})
                 `(~handler ~context ~event))]
      [method
       `(fn ~parameters
          ~(if default-worker? call `(cljs.core/this-as ~this ~call)))])))

(defn- declaration
  [kind {:keys [source name]} export]
  {:kind kind
   :name name
   :source source
   :export export
   :export-root (str "__ft_" (clojure.core/name kind)
                     "_"
                       (str/replace (munge (str name)) "." "_"))})

(defn- dependencies
  [items extras]
  (into ['fast-twitch.celld.context]
        (concat (for [[kind ns] [[:fetch 'fast-twitch.celld.http]
                                 [:rpc 'fast-twitch.celld.rpc]]
                      :when (some #(= kind (:kind %)) items)]
                  ns)
                extras)))

(defn- exported-definition
  [sym descriptor namespaces bindings installations]
  (let [value (first bindings)]
    `(do
       ~(runtime-requires namespaces)
       (def ~(with-meta sym
               {:doc (str "Direct native " (name (:kind descriptor)) " entrypoint.")
                :fast-twitch.celld/descriptor descriptor})
         (let ~bindings
              ~@installations
              (goog/exportSymbol ~(:export-root descriptor) ~value)
              ~value)))))

(defmacro defcell
  "Defines the actual synchronous (ctx, env) Cell constructor and direct export.
  Focused and ordinary handlers share a boundary; context belongs to its receiver."
  [sym options]
  (let [{src :source qsym :name :as context} (expansion &form &env sym)
        _ (d/closed! :cell options src qsym)
        export (d/native-name! (:export options (name sym)) src qsym [:export])
        binding (names/identifier (:binding options (str (.toUpperCase export) "S")))
        _ (when-not (and (<= (count binding) 128)
                         (re-matches #"[A-Za-z_$][A-Za-z0-9_$]*" binding))
            (bad! context
                  :binding
                  [:binding]
                  :binding-name
                  binding
                  "Supply a stable native binding name."))
        constructor (gensym "constructor")
        items (selected context options #{:init :fetch :rpc :alarm :websocket})
        initializer (:handler (first (filter #(= :init (:kind %)) items)))
        sockets (first (filter #(= :websocket (:kind %)) items))
        descriptor (assoc (declaration :cell context export)
                     :binding binding
                     :facet? (:facet? options false)
                     :handlers items)]
    (exported-definition
      sym
      descriptor
      (dependencies items (when sockets ['fast-twitch.celld.websocket]))
      [constructor
       `(fn [ctx# env#]
          (cljs.core/this-as
            this#
            (fast-twitch.celld.context/activate! this# ctx# env# ~initializer)))]
      (concat (for [[method body] (keep #(native-method % false) items)]
                `(aset (.-prototype ~constructor) ~method ~body))
              (when sockets
                [`(fast-twitch.celld.websocket/install-handlers! ~constructor
                                                                 ~(:handler
                                                                    sockets))])))))

(defmacro defworker
  "Defines a default Worker object or a named native WorkerEntrypoint constructor.
  Queue and scheduled callbacks keep their native scope and supplied context."
  [sym options]
  (let [{src :source :as context} (expansion &form &env sym)
        _ (d/closed! :worker options src sym)
        export (names/identifier (:export options "default"))
        named? (not= "default" export)
        _ (when named? (d/native-name! export src sym [:export]))
        worker (gensym "worker")
        items (selected context options #{:fetch :queue :scheduled :rpc})]
    (exported-definition
      sym
      (assoc (declaration :worker context export) :handlers items)
      (dependencies items (when named? ['fast-twitch.celld.worker]))
      [worker
       (if named? `(fast-twitch.celld.worker/named-constructor) `(cljs.core/js-obj))]
      (for [item items
            :let [[method body]
                    (native-method item (and (not named?) (not= :rpc (:kind item))))]]
        `(aset ~(if named? `(.-prototype ~worker) worker) ~method ~body)))))

(defmacro defworkflow
  "Defines a native WorkflowEntrypoint subclass in CLJS, with run(context,event,step).
  Ordinary Cells do not inherit this required workflow interface."
  [sym options argv & body]
  (let [{:keys [source name] :as context} (expansion &form &env sym)
        _ (d/closed! :workflow options source name)
        _ (exact-arguments! context argv 3)
        options (schema-options context options)
        export
          (d/native-name! (:export options (clojure.core/name sym)) source name [:export])
        constructor (gensym "constructor")
        handler (gensym "handler")
        descriptor (assoc (declaration :workflow context export)
                     :binding (names/identifier (:binding options))
                     :workflow-name (names/text (:name options export)))]
    (exported-definition
      sym
      descriptor
      ['fast-twitch.celld.context 'fast-twitch.celld.worker
       'fast-twitch.celld.services.workflows]
      [constructor `(fast-twitch.celld.worker/workflow-constructor) handler
       `(~(with-meta 'cljs.core/fn {:async true}) ~argv ~@body)]
      [`(aset (.-prototype ~constructor)
              "run"
              (fn [event# step#]
                (cljs.core/this-as this#
                                   (fast-twitch.celld.services.workflows/run!
                                     ~handler
                                     (fast-twitch.celld.context/of this#)
                                     event#
                                     step#
                                     ~options))))])))
