(ns fast-twitch.celld.macros
  "Focused native declarations compose into one directly exported CLJS constructor.
  Macro diagnostics are mandatory and carry source locations and repair guidance."
  (:require [cljs.analyzer.api :as ana]
            [cljs.analyzer :as analyzer]
            [cljs.env :as cljs-env]
            [clojure.java.io :as io]
            [fast-twitch.celld.definition :as d]
            [clojure.string :as str]
            [fast-twitch.celld.names :as names]))

(defn- source
  [form env]
  (merge (select-keys (meta form) [:line :column :end-line :end-column])
         {:file (or (:file env) analyzer/*cljs-file*)}))

(defn- qname
  [env sym]
  (symbol (str (get-in env [:ns :name])) (str sym)))

(defn- bad!
  [form env sym code path expected value repair]
  (d/fail! code (source form env) (qname env sym) path expected value repair))

(defn- resolve-var
  [form env owner sym]
  (when-not (symbol? sym)
    (bad! form
          env
          owner
          :reference
          []
          :symbol
          sym
          "Use a declaration or ordinary function symbol."))
  (let [resolved (ana/resolve env sym)]
    (when-not (and resolved (:name resolved) (:op resolved))
      (bad! form
            env
            owner
            :unresolved
            []
            :resolved-symbol
            sym
            "Require and define the referenced declaration before use."))
    resolved))

(defn- schema-options
  [form env owner options]
  (reduce
    (fn [out key]
      (if (contains? out key)
        (let
          [value (key out)
           schema
             (if (symbol? value)
               (let [resolved (resolve-var form env owner value)]
                 (or
                   (:fast-twitch.celld/schema resolved)
                   (bad!
                     form
                     env
                     owner
                     :schema-reference
                     [key]
                     :portable-reference
                     value
                     "Define a literal schema with defcontract before using its reference.")))
               value)]
          (d/schema! schema (source form env) owner [key])
          (assoc out key schema))
        out))
    options
    [:args :returns]))

(defmacro defcontract
  "Defines a portable literal schema with compiler metadata for deterministic references.
  No application code is evaluated on the JVM and no runtime registration occurs."
  [sym schema]
  (d/schema! schema (source &form &env) sym [:schema])
  `(def ~(with-meta sym
           {:doc "Portable boundary contract, resolved statically by native declarations."
            :fast-twitch.celld/schema schema})
     ~schema))

(defn- arity!
  [form env sym argv expected]
  (when-not (and (vector? argv) (= expected (count argv)))
    (bad! form
          env
          sym
          :arity
          [:arguments]
          expected
          argv
          "Use the documented native handler arguments, including context.")))

(defn- focused
  [kind form env sym forms arity]
  (let [[doc forms] (if (string? (first forms)) [(first forms) (next forms)] [nil forms])
        [options forms] (if (map? (first forms)) [(first forms) (next forms)] [{} forms])
        options (schema-options form env sym options)
        argv (first forms)
        body (next forms)
        src (source form env)]
    (d/closed! kind options src (qname env sym))
    (when arity (arity! form env sym argv arity))
    (when-not (vector? argv)
      (bad! form env sym :arity [] :vector argv "Supply a handler argument vector."))
    (when (= kind :rpc)

      (d/native-name! (:method options) src sym [:method])
      (when (contains? d/cell-events (names/identifier (:method options)))
        (bad! form
              env
              sym
              :reserved-member
              [:method]
              :rpc-name
              (:method options)
              "Choose a non-event RPC method name."))
      (doseq [key [:args :returns]
              :when (contains? options key)]
        (d/schema! (key options) src sym [key])))
    (when (and (= kind :rpc)
               (vector? (:args options))
               (= :cat (first (:args options)))
               (not (some #{'&} argv))
               (every? #(not (and (vector? %) (#{:* :+} (first %))))
                       (rest (:args options))))
      (arity! form env sym argv (count (:args options))))
    (let [descriptor {:kind kind
                      :source src
                      :name (qname env sym)
                      :options options
                      :arity (count argv)}]
      `(defn ~(with-meta sym
                (merge
                  (meta sym)
                  {:doc (or doc
                            (str
                              "Native "
                              (name kind)
                              " handler; included explicitly by its owner declaration."))
                   :fast-twitch.celld/descriptor descriptor}))
         ~argv
         ~@body))))

(defmacro defcell-init
  "Defines one context initializer, selected explicitly by defcell. The constructor establishes its native gate synchronously; the body may return a Promise."
  [sym & forms]
  (focused :init &form &env sym forms 1))

(defmacro deffetch
  "Defines a (context, request-map) handler. :native? passes a native Request. Successful results use one shared response finalization path."
  [sym & forms]
  (focused :fetch &form &env sym forms 2))

(defmacro defrpc
  "Defines a native RPC handler with context followed by application arguments. :method is a stable native name; :args/:returns and :codec check both boundaries."
  [sym & forms]
  (focused :rpc &form &env sym forms nil))

(defmacro defalarm
  "Defines a (context, alarm-info-map) native alarm handler. It schedules no alarm and owns no additional native event."
  [sym & forms]
  (focused :alarm &form &env sym forms 2))

(defmacro defqueue-handler
  "Defines a Worker-only (context, native-batch) handler. Return/Promise completion controls native settlement; include it explicitly in defworker."
  [sym & forms]
  (focused :queue &form &env sym forms 2))

(defmacro defscheduled-handler
  "Defines a Worker-only (context, native-controller) callback. :crons declares UTC native schedules and the handler retains no Cell alarm scope."
  [sym & forms]
  (focused :scheduled &form &env sym forms 2))

(defn- handler-arity!
  "Requires analyzable native handler signatures. Foreign callables use an ordinary CLJS adapter."
  [form env owner ref expected path]
  (let [resolved (resolve-var form env owner ref)
        a (:arglists resolved)
        arities (or (:method-params resolved) (if (= 'quote (first a)) (second a) a))
        accepts? (or (and (:variadic? resolved)
                          (number? (:max-fixed-arity resolved))
                          (<= (:max-fixed-arity resolved) expected))
                     (some (fn [argv]
                             (let [fixed (count (take-while #(not= '& %) argv))]
                               (if (some #{'&} argv)
                                 (<= fixed expected)
                                 (= expected (count argv)))))
                           arities))]
    (when-not accepts?
      (bad!
        form
        env
        owner
        :arity
        path
        expected
        arities
        "Use a CLJS function with the documented arguments; wrap foreign callables in an explicit adapter."))
    resolved))

(defmacro defwebsocket-handlers
  "Defines static hibernation/resident dispatch metadata. Handler symbols accept
  context, native socket and the shared event map; version must be a positive integer."
  [sym options]
  (d/closed! :websocket options (source &form &env) sym)
  (when-not (and (or (keyword? (:id options)) (string? (:id options)))
                 (pos-int? (:version options))
                 (#{:resident :hibernating} (:mode options)))
    (bad! &form
          &env
          sym
          :websocket-dispatch
          []
          :stable-id-version-mode
          options
          "Supply :id, positive :version and :mode."))
  (doseq [key [:message :close :error]
          :when (contains? options key)]
    (handler-arity! &form &env sym (key options) 3 [key]))
  `(def ~(with-meta sym
           {:fast-twitch.celld/descriptor {:kind :websocket
                                           :name (qname &env sym)
                                           :source (source &form &env)
                                           :options options}})
     ~options))

(defn- selected
  [form env sym options allowed]
  (when-not (vector? (:include options []))
    (bad! form
          env
          sym
          :include
          [:include]
          :literal-vector
          (:include options)
          "Use an explicit literal vector of declaration references."))
  (let
    [included (mapv (fn [ref]
                      (let [resolved (resolve-var form env sym ref)
                            descriptor (:fast-twitch.celld/descriptor resolved)]
                        (when-not (and descriptor (allowed (:kind descriptor)))
                          (bad!
                            form
                            env
                            sym
                            :scope
                            [:include]
                            allowed
                            ref
                            "Include focused declarations of this entrypoint's scope."))
                        (assoc descriptor :handler (:name resolved))))
                (:include options []))
     explicit (for [[key kind] [[:init :init] [:fetch :fetch] [:alarm :alarm]
                                [:queue :queue] [:scheduled :scheduled]]
                    :when (contains? options key)]
                (let [spec (key options)
                      spec (if (symbol? spec) {:handler spec} spec)
                      r (resolve-var form env sym (:handler spec))
                      _ (d/closed! kind (dissoc spec :handler) (source form env) sym)
                      known-arities (or (:method-params r)
                                        (let [a (:arglists r)]
                                          (if (= (first a) (quote quote)) (second a) a)))
                      expected (if (= kind :init) 1 2)]
                  (when (and known-arities
                             (not (some #(= expected (count %)) known-arities)))
                    (bad! form
                          env
                          sym
                          :arity
                          [key]
                          expected
                          known-arities
                          "Use the documented context/native event handler signature."))
                  {:kind kind
                   :handler (:name r)
                   :source (source form env)
                   :options (dissoc spec :handler)}))
     sockets (when-let [spec (:websocket options)]
               (let [resolved (when (symbol? spec) (resolve-var form env sym spec))
                     descriptor (:fast-twitch.celld/descriptor resolved)]
                 (if resolved
                   (do (when-not (= :websocket (:kind descriptor))
                         (bad! form
                               env
                               sym
                               :scope
                               [:websocket]
                               :websocket
                               spec
                               "Use a WebSocket declaration."))
                       (assoc descriptor :handler (:name resolved)))
                   (do (d/closed! :websocket spec (source form env) sym)
                       (when-not (and (or (keyword? (:id spec)) (string? (:id spec)))
                                      (pos-int? (:version spec))
                                      (#{:resident :hibernating} (:mode spec)))
                         (bad! form
                               env
                               sym
                               :websocket-dispatch
                               [:websocket]
                               :stable-id-version-mode
                               spec
                               "Supply a static ID, version and delivery mode."))
                       (doseq [key [:message :close :error]
                               :when (key spec)]
                         (handler-arity! form env sym (key spec) 3 [:websocket key]))
                       {:kind :websocket
                        :handler spec
                        :options spec
                        :source (source form env)}))))
     rpc
       (for [[method spec] (:rpc options)]
         (let [spec (schema-options form env sym (if (symbol? spec) {:handler spec} spec))
               r (resolve-var form env sym (:handler spec))]
           (d/closed! :rpc (dissoc spec :handler) (source form env) sym)

           (d/native-name! method (source form env) sym [:rpc method])
           (when (contains? d/cell-events (names/identifier method))
             (d/fail! :duplicate-event
                      (source form env)
                      (qname env sym)
                      [:rpc method]
                      :non-event-method method
                      "Choose a non-event RPC method name."
                        {:sources [(source form env)
                                   (or (:source (first (filter #(= :fetch (:kind %))
                                                         included)))
                                       (source form env))]}))
           (doseq [key [:args :returns]
                   :when (contains? spec key)]
             (d/schema! (key spec) (source form env) sym [:rpc method key]))
           (when (and (vector? (:args spec))
                      (= :cat (first (:args spec)))
                      (:method-params r)
                      (not (:variadic? r))
                      (not (some #(= (count (:args spec)) (count %)) (:method-params r))))
             (bad!
               form
               env
               sym
               :arity
               [:rpc method :args]
               (count (:args spec))
               :known-handler
               "Match the handler arity, including context, to the application :args schema."))
           {:kind :rpc
            :handler (:name r)
            :source (source form env)
            :options (assoc (dissoc spec :handler) :method method)}))
     all (vec (concat included explicit rpc (when sockets [sockets])))
     key-for (fn [x]
               (if (= :rpc (:kind x))
                 [:rpc (names/identifier (get-in x [:options :method]))]
                 [(:kind x)]))]
    (doseq [[key values] (group-by key-for all)
            :when (> (count values) 1)]
      (d/fail! :duplicate-event
               (source form env)
               (qname env sym)
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
  ;; Macro-introduced runtime dependencies belong to the selected application
  ;; graph; unrelated feature namespaces are never bootstrap preloads.
  (when cljs-env/*compiler*
    (let [owner analyzer/*cljs-ns*]
      (doseq [namespace namespaces]
        (when-not (get-in @cljs-env/*compiler* [:cljs.analyzer/namespaces namespace])
          (analyzer/analyze-file (io/resource (str (-> (str namespace)
                                                       (str/replace "." "/")
                                                       (str/replace "-" "_"))
                                                   ".cljs"))))
        (swap! cljs-env/*compiler* assoc-in
          [:cljs.analyzer/namespaces owner :requires namespace]
          namespace))))
  (cons 'cljs.core/require (map #(list 'quote %) namespaces)))

(defn- cell-method
  [item]
  (let [{:keys [kind handler options]} item]
    (case kind
      :fetch ["fetch"
              `(fn [request#]
                 (cljs.core/this-as this#
                                    (fast-twitch.celld.http/handle!
                                      ~handler
                                      (fast-twitch.celld.context/of this#)
                                      request#
                                      ~(:native? options false))))]
      :alarm ["alarm"
              `(fn [info#]
                 (cljs.core/this-as this#
                                    (~handler
                                     (fast-twitch.celld.context/of this#)
                                     {:retry-count (aget info# "retryCount")
                                      :is-retrying? (aget info# "isRetry")
                                      :scheduled-time (aget info# "scheduledTime")})))]
      :rpc [(names/identifier (:method options))
            `(fn [& args#]
               (cljs.core/this-as this#
                                  (fast-twitch.celld.rpc/serve!
                                    ~handler
                                    (fast-twitch.celld.context/of this#)
                                    (vec args#)
                                    ~{:args-schema (:args options)
                                      :returns (:returns options)
                                      :codec (:codec options)})))]
      nil)))

(defmacro defcell
  "Defines the actual synchronous (ctx, env) Cell constructor and direct export.
  :include selects focused handlers; ordinary :fetch/:alarm/:init and :rpc maps
  normalize to the same boundary. No runtime base class or second instance is used."
  [sym options]
  (let [src (source &form &env)
        qsym (qname &env sym)
        _ (d/closed! :cell options src qsym)
        export (d/native-name! (:export options (name sym)) src qsym [:export])
        binding (names/identifier (:binding options (str (.toUpperCase export) "S")))
        _ (when-not (and (string? binding)
                         (<= (count binding) 128)
                         (re-matches #"[A-Za-z_$][A-Za-z0-9_$]*" binding))
            (bad! &form
                  &env
                  sym
                  :binding
                  [:binding]
                  :binding-name
                  binding
                  "Supply a stable native binding name."))
        constructor (gensym "constructor")
        items (selected &form &env sym options #{:init :fetch :rpc :alarm :websocket})
        initializer (:handler (first (filter #(= :init (:kind %)) items)))
        sockets (first (filter #(= :websocket (:kind %)) items))
        methods (keep cell-method items)
        root (str "__ft_cell_" (str/replace (munge (str qsym)) "." "_"))
        descriptor {:kind :cell
                    :name qsym
                    :source src
                    :export export
                    :export-root root
                    :binding binding
                    :facet? (:facet? options false)
                    :handlers items}]
    `(do
       ~(runtime-requires (cond-> ['fast-twitch.celld.context]
                            (some #(= :fetch (:kind %)) items) (conj
                                                                 'fast-twitch.celld.http)
                            (some #(= :rpc (:kind %)) items) (conj 'fast-twitch.celld.rpc)
                            sockets (conj 'fast-twitch.celld.websocket)))
       (def
         ~(with-meta sym
            {:doc
               "Direct native Cell constructor. Context is private and receiver-associated."
             :fast-twitch.celld/descriptor descriptor})
         (let [~constructor
                 (fn [ctx# env#]
                   (cljs.core/this-as
                     this#
                     (fast-twitch.celld.context/activate! this# ctx# env# ~initializer)))]
           ~@(for [[method body] methods]
               `(aset (.-prototype ~constructor) ~method ~body))
           ~@(when sockets
               [`(fast-twitch.celld.websocket/install-handlers! ~constructor
                                                                ~(:handler sockets))])
           (goog/exportSymbol ~root ~constructor)
           ~constructor)))))

(defmacro defworker
  "Defines a default Worker object or a named native WorkerEntrypoint constructor.
  Queue and scheduled callbacks keep their native scope and supplied context."
  [sym options]
  (let [src (source &form &env)
        _ (d/closed! :worker options src sym)
        export (names/identifier (:export options "default"))
        _ (when-not (= "default" export) (d/native-name! export src sym [:export]))
        worker (gensym "worker")
        items (selected &form &env sym options #{:fetch :queue :scheduled :rpc})
        root (str "__ft_worker_" (str/replace (munge (str (qname &env sym))) "." "_"))
        named? (not= "default" export)
        desc {:kind :worker
              :name (qname &env sym)
              :source src
              :export export
              :export-root root
              :handlers items}]
    `(do
       ~(runtime-requires (cond-> ['fast-twitch.celld.context]
                            (some #(= :fetch (:kind %)) items) (conj
                                                                 'fast-twitch.celld.http)
                            (some #(= :rpc (:kind %)) items) (conj 'fast-twitch.celld.rpc)
                            named? (conj 'fast-twitch.celld.worker)))
       (def ~(with-meta sym
               {:doc "Native Worker entrypoint; handlers are selected explicitly."
                :fast-twitch.celld/descriptor desc})
         (let [~worker ~(if named?
                          `(fast-twitch.celld.worker/named-constructor)
                          `(cljs.core/js-obj))]
           ~@(for [{:keys [kind handler options]} items]
               (let [target (if named? `(.-prototype ~worker) worker)]
                 (case kind
                   :fetch
                     `(aset ~target
                            "fetch"
                            ~(if named?
                               `(fn [request#]
                                  (cljs.core/this-as this#
                                                     (fast-twitch.celld.http/handle!
                                                       ~handler
                                                       (fast-twitch.celld.context/of
                                                         this#)
                                                       request#
                                                       ~(:native? options false))))
                               `(fn [request# env# ctx#]
                                  (fast-twitch.celld.http/handle!
                                    ~handler
                                    (fast-twitch.celld.context/worker env# ctx#)
                                    request#
                                    ~(:native? options false)))))
                   :rpc (let [[name body] (cell-method {:kind kind
                                                        :handler handler
                                                        :options options})]
                          `(aset ~target ~name ~body))
                   `(aset ~target
                          ~(event-name kind)
                          ~(if named?
                             `(fn [event#]
                                (cljs.core/this-as
                                  this#
                                  (~handler (fast-twitch.celld.context/of this#) event#)))
                             `(fn [event# env# ctx#]
                                (~handler
                                 (fast-twitch.celld.context/worker env# ctx#)
                                 event#)))))))
           (goog/exportSymbol ~root ~worker)
           ~worker)))))

(defmacro defworkflow
  "Defines a native WorkflowEntrypoint subclass in CLJS, with run(context,event,step).
  Ordinary Cells do not inherit this required workflow interface."
  [sym options argv & body]
  (d/closed! :workflow options (source &form &env) sym)
  (arity! &form &env sym argv 3)
  (let [options (schema-options &form &env sym options)]
    (doseq [key [:args :returns]
            :when (contains? options key)]
      (d/schema! (key options) (source &form &env) sym [key]))

    (let [export (d/native-name! (:export options (name sym))
                                 (source &form &env)
                                 sym
                                 [:export])
          root (str "__ft_workflow_" (str/replace (munge (str (qname &env sym))) "." "_"))
          descriptor {:kind :workflow
                      :name (qname &env sym)
                      :source (source &form &env)
                      :export export
                      :export-root root
                      :binding (names/identifier (:binding options))
                      :workflow-name (names/text (:name options export))}]
      `(do
         ~(runtime-requires ['fast-twitch.celld.context 'fast-twitch.celld.worker
                             'fast-twitch.celld.services.workflows])
         (def
           ~(with-meta sym
              {:doc
                 "Native Workflow entrypoint; put replayable effects inside step callbacks."
               :fast-twitch.celld/descriptor descriptor})
           (let [constructor# (fast-twitch.celld.worker/workflow-constructor)
                 handler# (~(with-meta 'cljs.core/fn {:async true}) ~argv ~@body)]
             (aset (.-prototype constructor#)
                   "run"
                   (fn [event# step#]
                     (cljs.core/this-as this#
                                        (fast-twitch.celld.services.workflows/run!
                                          handler#
                                          (fast-twitch.celld.context/of this#)
                                          event#
                                          step#
                                          ~options))))
             (goog/exportSymbol ~root constructor#)
             constructor#))))))

(defmacro with-transaction-sync
  "Evaluates storage once and rejects thenables inside its native sync callback."
  [storage & body]
  (when (some #(or (= 'await %) (= 'cljs.core/await %) (= :async %))
              (tree-seq coll? seq body))
    (bad! &form
          &env
          'with-transaction-sync
          :async-sync
          []
          :synchronous-body
          body
          "Use transaction! for asynchronous work."))
  (runtime-requires ['fast-twitch.celld.storage])
  `(let [storage# ~storage]
     (fast-twitch.celld.storage/transaction-sync! storage#
                                                  (fn []
                                                    ~@body))))
