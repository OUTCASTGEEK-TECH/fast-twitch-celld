(ns fast-twitch.celld.services.queues
  "Queue producers and native settlement. Handler completion owns the batch's
  native auto-ack/retry policy; the library adds no retries or effect ledger."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.validation :as v]))

(defn native
  "Returns the native queue/message/batch handle."
  [handle]
  handle)

(defn send!
  "Returns native committed-send Promise. Body serialization is explicit through
  :contentType; native v8 data uses the checked structured value domain."
  ([queue value]
   (codec/native-value! value)
   (n/invoke queue "send" [value]))
  ([queue value options]
   (let [options (n/options options #{:contentType :delaySeconds} :queue-send)]
     (codec/native-value! value)
     (n/invoke queue "send" [value options]))))

(defn send-encoded!
  "Wraps CLJS JSON in a version-2 envelope before native send; legacy version-1 reads remain supported."
  [queue value policy options]
  (codec/persistence-policy! policy)
  (send! queue (codec/encode policy value) options))

(defn send-batch!
  "Encodes/validates every entry before invoking native sendBatch once. Returns its Promise."
  ([queue messages]
   (send-batch! queue messages nil))
  ([queue messages options]
   (v/check! [:vector {:min 1 :max 100} :map] messages :queue-batch)
   (let [entries (mapv (fn [message]
                         (let [{:keys! [body]} message]
                           (codec/native-value! body)
                           (n/options message
                                      #{:body :contentType :delaySeconds}
                                      :queue-message)))
                   messages)]
     (n/invoke queue
               "sendBatch"
               (cond-> [(to-array entries)]
                 options (conj (n/options options #{:delaySeconds} :queue-batch)))))))

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

(defn retry!
  "Requests native redelivery, synchronously. Optional delay is in seconds."
  ([message]
   (n/invoke message "retry" []))
  ([message options]
   (n/invoke message "retry" [(n/options options #{:delaySeconds} :queue-retry)])))

(defn ack-all!
  "Synchronously acknowledges the native batch's unsettled messages."
  [batch]
  (n/invoke batch "ackAll" []))

(defn retry-all!
  "Synchronously retries unsettled messages, preserving native first-call semantics."
  ([batch]
   (n/invoke batch "retryAll" []))
  ([batch options]
   (n/invoke batch "retryAll" [(n/options options #{:delaySeconds} :queue-retry)])))
