(ns example.services-app
  "Native D1/KV/R2/queue/cron fixture. No library-owned application tables or resources."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.bindings :as bindings]
            [fast-twitch.celld.services.d1 :as d1]
            [fast-twitch.celld.services.kv :as kv]
            [fast-twitch.celld.services.r2 :as r2]
            [fast-twitch.celld.services.queues :as queues]
            [fast-twitch.celld.services.cron :as cron]
            [fast-twitch.codecs.json :as json]
            [fast-twitch.util.streams.readers :as readers])
  (:require-macros [fast-twitch.celld.macros :refer
                    [deffetch defworker defqueue-handler defscheduled-handler]])
  (:refer-global :only [Uint8Array ReadableStream Headers Date Number]))

(defn ^:async d1-checks
  [db]
  (let
    [executed (await
                (d1/exec!
                  db
                  {:create-table [:records :if-not-exists]
                   :with-columns [[:id :integer :primary-key] [:value :text]]}))
     _ (await (d1/run! (d1/prepare db {:delete-from :records})))
     bound (d1/prepare db
                       {:insert-into :records
                        :columns [:id :value]
                        :values [[[:param :id] [:param :value]]]
                        :returning [:id :value]}
                       {:id 1 :value "one"})
     inserted (await (d1/run! bound))
     _ (await (d1/batch! db
                         [(d1/prepare db
                                      {:insert-into :records
                                       :columns [:id :value]
                                       :values [[[:param :id] [:param :value]]]}
                                      {:id 2 :value "two"})]))
     boolean (await (d1/first!
                      (d1/prepare db {:select [[[:param :value] :flag]]} {:value true})
                      :flag))
     blob (await (d1/first! (d1/prepare db
                                        {:select [[[:hex [:param :bytes]] :data]]}
                                        {:bytes #js [65 66]})
                            :data))
     ordered-batch (await (d1/batch! db
                                     [(d1/prepare db {:select [[[:inline 1] :n]]})
                                      (d1/prepare db {:select [[[:inline 2] :n]]})]))
     all (await (d1/all! (d1/prepare db {:select [:*] :from [:records] :order-by [:id]})))
     first-row (await (d1/first! (d1/prepare db
                                             {:select [:*]
                                              :from [:records]
                                              :where [:= :id [:inline 1]]})))
     column (await (d1/first! (d1/prepare db
                                          {:select [:*]
                                           :from [:records]
                                           :where [:= :id [:inline 1]]})
                              :value))
     raw (await (d1/raw! (d1/prepare db {:select [:*] :from [:records] :order-by [:id]})
                         {:columnNames true}))
     raw-default
       (await (d1/raw! (d1/prepare db {:select [:*] :from [:records] :order-by [:id]})))
     session (d1/with-session db :first-primary)
     _ (await (d1/batch! session
                         [(d1/prepare session {:select [:id] :from [:records]})]))]
    {:d1-exec-metadata (and (number? (get executed :count))
                            (number? (get executed :duration)))
     :d1-ordered-batch (= [1 2] (mapv #(get-in % [:results 0 :n]) ordered-batch))
     :d1-raw-default (= [[1 "one"] [2 "two"]] raw-default)
     :d1-boolean (= 1 boolean)
     :d1-native-byte-array (= "4142" blob)
     :d1-run (:success? inserted)
     :d1-all (= 2 (count (:results all)))
     :d1-first (= "one" (get first-row :value))
     :d1-column (= "one" column)
     :d1-missing (nil? (await (d1/first!
                                (d1/prepare db
                                            {:select [:*]
                                             :from [:records]
                                             :where [:= :id [:inline 99]]}))))
     :d1-raw (= [:id :value] (first raw))
     :d1-session (= "celld:primary" (d1/bookmark session))}))

(defn ^:async kv-checks
  [binding]
  (doseq [key [:text :json :bytes :missing :ttl]] (await (kv/delete! binding key)))
  (await (kv/put! binding :text "hello" {:metadata #js {:kind "text"}}))
  (await (kv/put-json! binding :json {:value false} {}))
  (await (kv/put! binding :bytes (Uint8Array. #js [1 2 3])))
  (await (kv/put! binding :ttl "temporary" {:expirationTtl 60}))
  (let [metadata (await (kv/get-with-metadata! binding :text))
        bulk (await (kv/get-with-metadata! binding [:text :missing]))
        bulk-options (await (kv/get-with-metadata! binding [:text :missing] "text"))
        stream (await (kv/get! binding :text "stream"))
        page (await (kv/list! binding {:limit 1}))
        next-page (await (kv/list! binding {:limit 1 :cursor (:cursor page)}))
        ttl-page (await (kv/list! binding {:prefix :ttl}))
        invalid-metadata? (try
                            (kv/put! binding
                                     :text
                                     "changed"
                                     {:metadata (fn []
                                                  nil)})
                            false
                            (catch :default _ true))]
    {:kv-text (= "hello" (await (kv/get! binding :text)))
     :kv-json (= {:value false} (await (kv/get-json! binding :json)))
     :kv-native-json-mode (= "fast-twitch/cljs-json"
                             (first (await (kv/get! binding :json {:type :json}))))
     :kv-continuation (and (string? (:cursor page))
                           (= 1 (count (:keys next-page)))
                           (not= (get (first (:keys page)) :name)
                                 (get (first (:keys next-page)) :name)))
     :kv-expiration (> (get (first (:keys ttl-page)) :expiration) (/ (Date.now) 1000))
     :kv-metadata-before-effects (and invalid-metadata?
                                      (= "hello" (await (kv/get! binding :text))))
     :kv-bytes (= 3 (.-byteLength (await (kv/get! binding :bytes "arrayBuffer"))))
     :kv-stream (= "hello" (await (readers/read! stream {:codec :text})))
     :kv-metadata (= "text" (get-in metadata [:metadata :kind]))
     :kv-bulk (= "hello" (get-in bulk [:text :value]))
     :kv-bulk-hole (nil? (get-in bulk [:missing :value]))
     :kv-overloads (= (keys bulk) (keys bulk-options))
     :kv-list (= 1 (count (:keys page)))
     :kv-delete (do (await (kv/delete! binding :bytes))
                    (await (kv/delete! binding :ttl))
                    (nil? (await (kv/get! binding :bytes))))}))

(defn ^:async r2-checks
  [bucket]
  (await (r2/delete! bucket ["record" "multipart" "json" "missing"]))
  (let [_stored (await (r2/put! bucket
                                "record"
                                "abcdef"
                                {:httpMetadata {:contentType "text/plain"}
                                 :customMetadata {:kind "fixture"}}))
        head (await (r2/head! bucket "record"))
        found (await (r2/get! bucket "record" {:range {:offset 1 :length 3}}))
        native-found (:fast-twitch.r2/object (:object found))
        unused? (not (r2/body-used? native-found))
        text (await (r2/text! native-found))
        _ (await (r2/put! bucket "json" (json/encode {:value false})))
        buffer (await (r2/array-buffer! (:fast-twitch.r2/object
                                          (:object (await (r2/get! bucket "record"))))))
        bytes (await (r2/bytes! (:fast-twitch.r2/object
                                  (:object (await (r2/get! bucket "record"))))))
        blob (await (r2/blob! (:fast-twitch.r2/object
                                (:object (await (r2/get! bucket "record"))))))
        json-value (await (r2/json! (:fast-twitch.r2/object
                                      (:object (await (r2/get! bucket "json"))))))
        headers (Headers.)
        _ (r2/write-http-metadata! (:fast-twitch.r2/object head) headers)
        refused-write
          (await (r2/put! bucket "record" "changed" {:onlyIf {:etagMatches "invalid"}}))
        refused (await (r2/get! bucket "record" {:onlyIf {:etagMatches "invalid"}}))
        page (await (r2/list! bucket
                              {:prefix :rec :include [:httpMetadata :customMetadata]}))
        upload (await (r2/create-multipart! bucket "multipart"))
        upload-info (r2/upload-map upload)
        resumed (r2/resume-multipart bucket "multipart" (get upload-info :uploadId))
        part (await (r2/upload-part! resumed 1 "part"))
        complete (await (r2/complete! resumed
                                      [{:partNumber (req! part :partNumber)
                                        :etag (req! part :etag)}]))
        aborted (await (r2/create-multipart! bucket "aborted"))
        _ (await (r2/abort! aborted))
        aborted? (try (await (r2/upload-part! aborted 1 "invalid"))
                      false
                      (catch :default _ true))]
    {:r2-head (= 6 (get head :size))
     :r2-stream-range (= "bcd" text)
     :r2-condition (= :condition-unmet (:state refused))
     :r2-refused-write (nil? refused-write)
     :r2-body-used (and unused? (r2/body-used? native-found))
     :r2-body-handle (identical? (get (:object found) :body) (r2/body native-found))
     :r2-array-buffer (= 6 (.-byteLength buffer))
     :r2-bytes (= 6 (.-byteLength bytes))
     :r2-blob (= "abcdef" (await (.text blob)))
     :r2-json (false? (get json-value :value))
     :r2-write-http-metadata (= "text/plain" (.get headers "content-type"))
     :r2-metadata-fields (and (= "record" (get head :key))
                              (string? (get head :version))
                              (string? (get head :etag))
                              (string? (get head :httpEtag))
                              (instance? Date (get head :uploaded))
                              (= "text/plain"
                                 (get-in head [:httpMetadata :contentType]))
                              (= "fixture" (get-in head [:customMetadata :kind]))
                              (some? (get head :checksums))
                              (= "Standard" (get head :storageClass)))
     :r2-range-metadata (and (= 1 (get-in found [:object :range :offset]))
                             (= 3 (get-in found [:object :range :length])))
     :r2-upload-metadata (and (= "multipart" (get upload-info :key))
                              (string? (get upload-info :uploadId)))
     :r2-resume (= "multipart" (get (r2/upload-map resumed) :key))
     :r2-abort-terminal aborted?
     :r2-missing (= :missing (:state (await (r2/get! bucket "missing"))))
     :r2-list (= 1 (count (:objects page)))
     :r2-multipart (= "multipart" (req! complete :key))
     :r2-delete (do (await (r2/delete! bucket ["record" "multipart" "json"]))
                    (= :missing (:state (await (r2/get! bucket "record")))))}))

(defqueue-handler
  ^:async consume
  [ctx batch]
  (let [store (bindings/get-binding ctx :KV)
        projected (queues/batch-map batch :native)
        retry-batch? (atom false)]
    (doseq [message-map (req! projected :messages)]
      (let [{:keys! [id timestamp attempts body]} message-map
            message (:fast-twitch.queue/message message-map)]
        (await (kv/put! store
                        :queue/metadata
                        (str (and (string? id)
                                  (instance? Date timestamp)
                                  (number? attempts)
                                  (string? body)
                                  (string? (:queue projected))
                                  (identical? batch
                                              (:fast-twitch.queue/batch projected))))))
        (cond
          (and (= body "retry-single") (= attempts 1)) (queues/retry! message
                                                                      {:delaySeconds 0})
          (and (= body "retry-batch") (= attempts 1)) (reset! retry-batch? true)
          :else (do (await (kv/put! store (keyword (str "queue/" body)) (str attempts)))
                    (queues/ack! message)))))
    (when @retry-batch? (queues/retry-all! batch {:delaySeconds 0}))
    ;; First settlement wins; ackAll only settles messages left unsettled above.
    (queues/ack-all! batch)))

(defscheduled-handler ^:async scheduled
                      {:crons ["* * * * *"]}
                      [ctx controller]
                      (cron/no-retry! controller)
                      (await (kv/put! (bindings/get-binding ctx :KV)
                                      :cron-fired
                                      (str (:scheduled-time (cron/controller-map
                                                              controller))))))

(deffetch
  ^:async fetch-handler
  [ctx request]
  (let [db (bindings/get-binding ctx :DB)
        store (bindings/get-binding ctx :KV)
        bucket (bindings/get-binding ctx :BUCKET)
        queue (bindings/get-binding ctx :JOBS)]
    {:status 200
     :headers {:content-type "application/json"}
     :body
       (json/encode
         (case (:uri request)
           "/kv" (await (kv-checks store))
           "/r2" (await (r2-checks bucket))
           "/queue-metrics"
             (let [metrics (await (queues/metrics! queue))
                   {:keys! [backlogCount backlogBytes]} metrics
                   oldest (:oldestMessageTimestamp metrics)]
               {:keyword-fields (every? keyword? (keys metrics))
                :backlog-count (and (number? backlogCount) (>= backlogCount 0))
                :backlog-bytes (and (number? backlogBytes) (>= backlogBytes 0))
                :oldest-native-date (or (nil? oldest) (instance? Date oldest))})
           "/queue" (do (doseq [key ["one" "two" "metadata" "retry-single" "retry-batch"]]
                          (await (kv/delete! store (keyword (str "queue/" key)))))
                        (await (queues/send! queue "one" {:contentType :text}))
                        (await (queues/send-batch!
                                 queue
                                 [{:body "two" :contentType :text}
                                  {:body "retry-single" :contentType :text}
                                  {:body "retry-batch" :contentType :text}]))
                        {:sent true})
           "/queue-state"
             {:one (boolean (await (kv/get! store :queue/one)))
              :two (boolean (await (kv/get! store :queue/two)))
              :metadata (= "true" (await (kv/get! store :queue/metadata)))
              :retry-single (>= (Number (await (kv/get! store :queue/retry-single))) 2)
              :retry-batch (>= (Number (await (kv/get! store :queue/retry-batch))) 2)}
           "/prepare-cron" (do (await (kv/delete! store :cron-fired)) {:prepared true})
           "/cron" {:fired (boolean (await (kv/get! store :cron-fired)))}
           (await (d1-checks db))))}))

(defworker App {:include [fetch-handler consume scheduled]})
