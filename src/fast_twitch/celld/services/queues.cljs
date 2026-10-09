(ns fast-twitch.celld.services.queues
  "Queue producers and native settlement. Handler completion owns the batch's
  native auto-ack/retry policy; the library adds no retries or effect ledger."
  (:require [fast-twitch.celld.contracts :as contracts]
            [malli.experimental :as mx]
            [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.validation :as v]))

(defn native
  "Returns the native queue/message/batch handle."
  [handle]
  handle)

(mx/defn ^:dynamic send!
  "Returns native committed-send Promise. :codec checks/encodes the body once;
  :contentType selects native serialization. Native structured data is the default."
  [queue value & [options] :- [:? [:maybe (:queue-send contracts/schemas)]]]
  (n/invoke queue
            "send"
            (cond-> [(codec/encode (:codec options) value)]
              options (conj (n/option-fields (dissoc options :codec))))))

(mx/defn ^:dynamic send-batch!
  "Encodes/validates every entry before invoking native sendBatch once. Returns its Promise."
  [queue messages :- [:vector {:min 1 :max 100} (:queue-message contracts/schemas)] &
   [options] :- [:? [:maybe (:queue-batch contracts/schemas)]]]
  (let [entries (mapv (fn [message]
                        (n/option-fields
                          (assoc (dissoc message :codec)
                            :body (codec/encode (:codec message) (req! message :body)))))
                  messages)]
    (n/invoke queue
              "sendBatch"
              (cond-> [(to-array entries)] options (conj (n/option-fields options))))))

(defn ^:async metrics!
  "Returns Celld's native queue metrics as a shallow data map."
  [queue]
  (n/data-map (await (n/invoke queue "metrics" []))))

(defn message-map
  "Projects ID/timestamp/attempts while retaining the native settlement capability."
  ([message]
   (message-map message :native))
  ([message policy]
   {:id (n/property message "id")
    :timestamp (n/property message "timestamp")
    :attempts (n/property message "attempts")
    :body (codec/decode policy (n/property message "body"))
    :fast-twitch.queue/message message}))

(defn batch-map
  "Projects one native batch with explicit body codec; settlement handles remain native."
  [batch policy]
  {:queue (n/property batch "queue")
   :messages (mapv #(message-map % policy) (array-seq (n/property batch "messages")))
   :fast-twitch.queue/batch batch})

(defn ack!
  "Settles one native message synchronously; first settlement wins in Celld."
  [message]
  (n/invoke message "ack" []))

(mx/defn ^:dynamic retry!
  "Synchronously requests native retry with optional delay in seconds."
  [message & [options :as supplied] :- [:? (:queue-retry contracts/schemas)]]
  (n/invoke message "retry" (if supplied [(n/option-fields options)] [])))

(defn ack-all!
  "Synchronously acknowledges the native batch's unsettled messages."
  [batch]
  (n/invoke batch "ackAll" []))

(mx/defn ^:dynamic retry-all!
  "Synchronously requests native retry with optional delay in seconds."
  [batch & [options :as supplied] :- [:? (:queue-retry contracts/schemas)]]
  (n/invoke batch "retryAll" (if supplied [(n/option-fields options)] [])))

(v/instrument! send! send-batch! retry! retry-all!)
