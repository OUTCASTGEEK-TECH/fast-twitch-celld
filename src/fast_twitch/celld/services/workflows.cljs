(ns fast-twitch.celld.services.workflows
  "Native durable Workflow binding, instance and step operations. Effects belong
  inside do! callbacks because run replays from its first line. Repeated native
  step names inside loops are valid; no closure is serialized by this library."
  (:refer-clojure :exclude [run!])
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names])
  (:refer-global :only [globalThis Date Number]))

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

(defn- step-name!
  [name]
  (let [name (names/text name)]
    (v/check! [:string {:max 256}] name :workflow-step-name)
    (when (re-find #"[\x00-\x1f]" name)
      (v/fail! :workflow-step-name :characters "Omit control characters."))
    name))

(defn- event-type!
  [type]
  (let [type (names/identifier type)]
    (v/check! [:string {:min 1 :max 100}] type :workflow-event-type)
    (when-not (re-matches #"[A-Za-z0-9_][A-Za-z0-9_-]*" type)
      (v/fail! :workflow-event-type
               :characters
               "Use letters, digits, hyphens or underscores, starting without a hyphen."))
    type))

(defn- instance-id!
  [id]
  (v/check! [:string {:min 1 :max 100}] id :workflow-id)
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
  (n/options options #{:id :params :retention :locationHint} :workflow-create)
  (when (contains? options :id) (instance-id! (:id options)))
  (when (contains? options :retention)
    (let [retention (:retention options)]
      (n/options retention #{:successRetention :errorRetention} :workflow-retention)
      (doseq [[_ duration] retention] (bounded-duration! duration 0 2592000000))))
  (when (contains? options :params) (codec/native-value! (:params options)))
  (n/options options
             #{:id :params :retention :locationHint}
             :workflow-create))

(defn create!
  "Returns a Promise of a native instance, with native generated ID when omitted.
  Native structured params are checked; selected rich data must be explicitly encoded."
  ([binding]
   (n/invoke binding "create" []))
  ([binding options]
   (n/invoke binding "create" [(create-options options)]))
  ([binding options policy]
   (codec/persistence-policy! policy)
   (create! binding
            (cond-> options
              (contains? options :params) (update :params #(codec/encode policy %))))))

(defn create-batch!
  "Creates 1–100 native instances with native duplicate filtering; optional codec encodes each present params value."
  ([binding options]
   (v/check! [:vector {:min 1 :max 100} :map] options :workflow-create-batch)
   (n/invoke binding "createBatch" [(to-array (mapv create-options options))]))
  ([binding options policy]
   (codec/persistence-policy! policy)
   (create-batch! binding
                  (mapv #(cond-> %
                           (contains? % :params) (update :params
                                                         (fn [params]
                                                           (codec/encode policy params))))
                    options))))

(defn get!
  "Looks up an existing instance by stable native string ID; absent instances reject."
  [binding id]
  (n/invoke binding "get" [(instance-id! id)]))

(defn delete-batch!
  "Returns native per-ID deletion/errors for 1–100 IDs without retries."
  [binding ids]
  (v/check! [:vector {:min 1 :max 100} :string] ids :workflow-delete-batch)
  (n/invoke binding "deleteBatch" [(to-array (mapv instance-id! ids))]))

(defn id
  "Returns the native instance ID synchronously."
  [instance]
  (n/property instance "id"))

(defn ^:async status!
  "Returns native status/output/error; optional codec decodes a present successful output."
  ([instance]
   (let [result (workflow-map (await (n/invoke instance "status" [])))]
     (cond-> result (get result :error) (update :error n/data-map))))
  ([instance policy]
   (codec/persistence-policy! policy)
   (let [result (await (status! instance))]
     (cond-> result
       (contains? result :output) (update :output #(codec/decode policy %))))))

(defn send-event!
  "Sends a native event type/payload; buffered-before-wait behavior remains native."
  ([instance options]
   (let [options (update options :type event-type!)
         _ (req! options :type)]
     (when (contains? options :payload) (codec/native-value! (:payload options)))
     (n/invoke instance
               "sendEvent"
               [(n/options options #{:type :payload} :workflow-event)])))
  ([instance options policy]
   (codec/persistence-policy! policy)
   (send-event! instance
                (cond-> options
                  (contains? options :payload) (update :payload
                                                       #(codec/encode policy %))))))

(defn pause!
  "Returns the native pause Promise."
  [instance]
  (n/invoke instance "pause" []))

(defn resume!
  "Returns the native resume Promise."
  [instance]
  (n/invoke instance "resume" []))

(defn restart!
  "Restarts through native instance control; native supported options remain explicit."
  ([instance]
   (n/invoke instance "restart" []))
  ([instance options]
   (when (contains? options :from) (req! (:from options) :name))
   (n/invoke instance
             "restart"
             [(n/options
                (cond-> options
                  (map? (:from options))
                    (update :from
                            #(n/options (update % :name step-name!)
                                        #{:name :count :type}
                                        :workflow-restart-from)))
                #{:from}
                :workflow-restart)])))

(defn terminate!
  "Terminates natively. Rollback options are unavailable in this target."
  [instance]
  (n/invoke instance "terminate" []))

(defn delete!
  "Deletes natively and preserves native success/error handling."
  [instance]
  (n/invoke instance "delete" []))

(defn- step-options
  [options]
  (when (contains? options :sensitive)
    (v/check! [:enum "output"] (names/text (:sensitive options)) :workflow-sensitive)
    (v/fail! :workflow-sensitive
             :unavailable
             "Celld 0.6.2 does not implement sensitive step output; omit :sensitive."))
  (when-let [retries (:retries options)]
    (req! retries :limit)
    (let [delay (req! retries :delay)] (when-not (fn? delay) (duration-ms delay))))
  (when-let [timeout (:timeout options)]
    (when-not (pos? (duration-ms timeout))
      (v/fail! :workflow-timeout :bounds "Use a positive native timeout.")))
  (let [options (cond-> options
                  (map? (:retries options))
                    (update :retries
                            #(n/options % #{:limit :delay :backoff} :workflow-retries)))]
    (n/options options #{:retries :timeout} :workflow-step)))

(defn do!
  "Runs one native durable step. Callback may return a Promise; defaults/retries/replay
  are native. Callback receives the original native step/attempt/config context.
  Results are checked/encoded inside the step, before native persistence."
  ([step name callback]
   (v/check! [:fn fn?] callback :workflow-step-callback)
   (n/invoke step
             "do"
             [(step-name! name)
              (^:async fn [context] (codec/native-value! (await (callback context))))]))
  ([step name options callback]
   (v/check! [:fn fn?] callback :workflow-step-callback)
   (n/invoke step
             "do"
             [(step-name! name) (step-options options)
              (^:async fn [context] (codec/native-value! (await (callback context))))]))
  ([step name options callback policy]
   (codec/persistence-policy! policy)
   (v/check! [:fn fn?] callback :workflow-step-callback)
   ((^:async fn
     []
     (codec/decode
       policy
       (await (n/invoke
                step
                "do"
                [(step-name! name) (step-options options)
                 (^:async fn [context] (codec/encode policy (await (callback context))))])
       ))))))

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

(defn wait-for-event!
  "Returns native waited event, preserving type/timeout options and native buffering."
  ([step name options]
   (event-type! (req! options :type))
   (when-let [timeout (:timeout options)] (bounded-duration! timeout 1000 31536000000))
   (n/invoke
     step
     "waitForEvent"
     [(step-name! name)
      (n/options (update options :type event-type!) #{:type :timeout} :workflow-wait)]))
  ([step name options policy]
   (codec/persistence-policy! policy)
   (let [pending (wait-for-event! step name options)]
     ((^:async fn
       []
       (update (workflow-map (await pending)) :payload #(codec/decode policy %)))))))

(defn non-retryable-error
  "Constructs the native permanent Workflow error with optional native name; preserves constructor arity."
  ([message]
   (non-retryable-error message nil))
  ([message name]
   (v/check! :string message :workflow-error)
   (when (some? name) (v/check! :string name :workflow-error-name))
   (let [ctor (aget globalThis "__ft_NonRetryableError")]
     (when-not (fn? ctor)
       (v/fail! :workflow-error
                :capability
                "Build with the native Workflow error import."))
     (if (nil? name) (new ctor message) (new ctor message name)))))
