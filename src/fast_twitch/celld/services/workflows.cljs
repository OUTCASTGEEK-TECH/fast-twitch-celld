(ns fast-twitch.celld.services.workflows
  "Native durable Workflow binding, instance and step operations. Effects belong
  inside do! callbacks because run replays from its first line. Repeated native
  step names inside loops are valid; no closure is serialized by this library."
  (:refer-clojure :exclude [run!])
  (:require [fast-twitch.celld.contracts :as contracts]
            [malli.experimental :as mx]
            [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names])
  (:require-global ["cloudflare:workflows" :as workflows])
  (:refer-global :only [Date Number]))

(def duration-units
  {:second 1000
   :minute 60000
   :hour 3600000
   :day 86400000
   :week 604800000
   :month 2592000000
   :year 31536000000})

(defn duration-ms
  "Checks Celld's numeric milliseconds or fixed native duration string dialect."
  [value]
  (let [duration
          (if (number? value)
            value
            (when (string? value)
              (when-let
                [[_ amount unit]
                   (re-matches
                     #"\s*(\d+(?:\.\d+)?)\s+(second|minute|hour|day|week|month|year)s?\s*"
                     value)]
                (* (Number amount) (get duration-units (keyword unit))))))]
    (when-not (and (number? duration) (Number.isFinite duration) (<= 0 duration))
      (v/fail! :workflow-duration
               :value
               "Use nonnegative milliseconds or a native duration string."))
    duration))

(defn- bounded-duration!
  [value minimum maximum]
  (let [ms (duration-ms value)]
    (when-not (<= minimum ms maximum)
      (v/fail! :workflow-duration
               :bounds
               "Use a duration within the native operation limits."))
    value))

(mx/defn ^{:dynamic true :private true} step-name!
  :-
  [:string {:max 256}]
  [name :- :keyword]
  (let [name (names/text name)]
    (when (re-find #"[\x00-\x1f]" name)
      (v/fail! :workflow-step-name :characters "Omit control characters."))
    name))

(mx/defn ^{:dynamic true :private true} event-type!
  :-
  [:string {:min 1 :max 100}]
  [type :- :keyword]
  (let [type (names/identifier type)]
    (when-not (re-matches #"[A-Za-z0-9_][A-Za-z0-9_-]*" type)
      (v/fail! :workflow-event-type
               :characters
               "Use letters, digits, hyphens or underscores, starting without a hyphen."))
    type))

(mx/defn ^{:dynamic true :private true} instance-id!
  [id :- [:string {:min 1 :max 100}]]
  (when-not (re-matches #"[A-Za-z0-9_][A-Za-z0-9_-]*" id)
    (v/fail! :workflow-id
             :characters
             "Use letters, digits, hyphens or underscores, starting without a hyphen."))
  id)

(defn- workflow-map
  [value]
  (let [result (n/data-map value)]
    (cond-> result
      (string? (:status result)) (update :status names/selector)
      (string? (:type result)) (update :type names/selector))))

(defn ^:async run!
  "Workflow declaration boundary: projects native metadata with keyword keys, decodes payload, validates
  :args against payload before handler effects, then checks/encodes resolved output."
  [handler context event step options]
  (codec/persistence-policy! (:codec options))
  (let [event-map (assoc (workflow-map event)
                    :payload (codec/decode (:codec options)
                                           (n/property event "payload")))
        payload (req! event-map :payload)]
    (when-let [schema (:args options)] (v/check! schema payload :workflow-arguments))
    (let [result (await (handler context event-map step))]
      (when-let [schema (:returns options)] (v/check! schema result :workflow-result))
      (codec/encode (:codec options) result))))

(defn native
  "Returns the live Workflow binding, instance or step capability."
  [handle]
  handle)

(defn- create-options
  [options]
  (when (contains? options :id) (instance-id! (:id options)))
  (doseq [[_ duration] (:retention options)] (bounded-duration! duration 0 2592000000))
  (n/option-fields
    (cond-> (dissoc options :codec)
      (contains? options :params) (update :params #(codec/encode (:codec options) %)))))

(mx/defn ^:dynamic create!
  "Returns a native instance Promise, with native generated ID when omitted.
  :codec selects params encoding; native structured data is the default."
  [binding & [options :as supplied] :- [:? (:workflow-create contracts/schemas)]]
  (n/invoke binding "create" (if supplied [(create-options options)] [])))

(mx/defn ^{:dynamic true} create-batch!
  "Creates 1–100 native instances after checking and encoding every entry's params with its :codec."
  [binding options :- [:vector {:min 1 :max 100} (:workflow-create contracts/schemas)]]
  (n/invoke binding "createBatch" [(to-array (mapv create-options options))]))

(defn get!
  "Looks up an existing instance by stable native string ID; absent instances reject."
  [binding id]
  (n/invoke binding "get" [(instance-id! id)]))

(mx/defn ^:dynamic delete-batch!
  "Returns native per-ID deletion/errors for 1–100 IDs without retries."
  [binding ids :- [:vector {:min 1 :max 100} :string]]
  (n/invoke binding "deleteBatch" [(to-array (mapv instance-id! ids))]))

(defn id
  "Returns the native instance ID synchronously."
  [instance]
  (n/property instance "id"))

(mx/defn ^{:dynamic true :async true} status!
  "Returns native status/output/error; an optional codec decodes present output."
  [instance & [policy :as supplied] :- [:? [:maybe [:enum :native :json]]]]
  (let [result (workflow-map (await (n/invoke instance "status" [])))]
    (cond-> result
      (:error result) (update :error n/data-map)
      (and supplied (contains? result :output)) (update :output
                                                        #(codec/decode policy %)))))

(mx/defn ^{:dynamic true} send-event!
  "Sends a keyword event type and optional payload encoded with :codec; buffering remains native."
  [instance options :-
   (contracts/closed {:type :keyword :payload :any :codec [:enum :native :json]}
                     #{:type})]
  (n/invoke instance
            "sendEvent"
            [(n/option-fields
               (cond-> (update (dissoc options :codec) :type event-type!)
                 (contains? options :payload)
                   (update :payload #(codec/encode (:codec options) %))))]))

(defn pause!
  "Returns the native pause Promise."
  [instance]
  (n/invoke instance "pause" []))

(defn resume!
  "Returns the native resume Promise."
  [instance]
  (n/invoke instance "resume" []))

(mx/defn ^:dynamic restart!
  "Restarts through native instance control; native supported options remain explicit."
  [instance & [options :as supplied] :-
   [:?
    (contracts/closed {:from (contracts/closed {:name :keyword
                                                :count [:int {:min 1}]
                                                :type [:enum :do :sleep :waitForEvent]}
                                               #{:name})})]]
  (n/invoke instance
            "restart"
            (if supplied
              [(n/option-fields (cond-> options
                                  (map? (:from options))
                                    (update :from
                                            #(n/option-fields
                                               (update % :name step-name!)))))]
              [])))

(defn terminate!
  "Terminates natively. Rollback options are unavailable in this target."
  [instance]
  (n/invoke instance "terminate" []))

(defn delete!
  "Deletes natively and preserves native success/error handling."
  [instance]
  (n/invoke instance "delete" []))

(def ^:private StepOptions
  (conj (:workflow-step contracts/schemas) [:sensitive {:optional true} :any]))

(defn- step-options
  [options]
  (when (contains? options :sensitive)
    (v/fail! :workflow-sensitive
             :unavailable
             "Celld 0.6.2 does not implement sensitive step output; omit :sensitive."))
  (when-let [retries (:retries options)]
    (let [delay (req! retries :delay)] (when-not (fn? delay) (duration-ms delay))))
  (when-let [timeout (:timeout options)]
    (when-not (pos? (duration-ms timeout))
      (v/fail! :workflow-timeout :bounds "Use a positive native timeout.")))
  (let [options (cond-> (dissoc options :codec)
                  (map? (:retries options))
                    (update :retries
                            n/option-fields))]
    (n/option-fields options)))

(mx/defn ^{:dynamic true :async true} do!
  "Runs one durable step; defaults/retries/replay are native. The callback receives
  native step/attempt/config context; its awaited result is encoded before persistence."
  [step name callback :- [:fn fn?] & [options] :- [:? [:maybe StepOptions]]]
  (let [policy (:codec options)
        args (cond-> [(step-name! name)] options (conj (step-options options)))
        result (await (n/invoke step
                                "do"
                                (conj args
                                      (^:async fn
                                       [context]
                                       (codec/encode policy
                                                     (await (callback context)))))))]
    (codec/decode policy result)))

(defn sleep!
  "Durably sleeps with native duration units/string dialect; returns its Promise."
  [step name duration]
  (bounded-duration! duration 0 31536000000)
  (n/invoke step "sleep" [(step-name! name) duration]))

(defn sleep-until!
  "Durably sleeps until native milliseconds or Date; returns its Promise."
  [step name timestamp]
  (let [at (if (instance? Date timestamp) (.getTime timestamp) timestamp)]
    (v/safe-number! at :workflow-sleep-until)
    (when (> (- at (Date.now)) 31536000000)
      (v/fail! :workflow-sleep-until :bounds "Sleep at most 365 days ahead.")))
  (n/invoke step "sleepUntil" [(step-name! name) timestamp]))

(mx/defn ^{:dynamic true :async true} wait-for-event!
  "Returns a keyword event map with payload decoded using :codec; buffering remains native."
  [step name options :- (:workflow-wait contracts/schemas)]
  (when-let [timeout (:timeout options)] (bounded-duration! timeout 1000 31536000000))
  (let [event (workflow-map
                (await (n/invoke step
                                 "waitForEvent"
                                 [(step-name! name)
                                  (n/option-fields (update (dissoc options :codec)
                                                           :type
                                                           event-type!))])))]
    (cond-> event
      (contains? event :payload) (update :payload #(codec/decode (:codec options) %)))))

(mx/defn ^:dynamic non-retryable-error
  "Constructs the native permanent Workflow error synchronously, with optional name."
  [message :- :string & [name] :- [:? [:maybe :string]]]
  (if (nil? name)
    (workflows/NonRetryableError. message)
    (workflows/NonRetryableError. message name)))

(v/instrument! step-name!
               event-type!
               instance-id!
               create!
               create-batch!
               delete-batch!
               status!
               send-event!
               restart!
               do!
               wait-for-event!
               non-retryable-error)
