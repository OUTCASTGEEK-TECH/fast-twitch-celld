(ns example.storage-app
  "Native storage, SQL, alarm and contract verification through the exported Cell."
  (:require [fast-twitch.codecs.json :as json]
            [cljs.core :refer [await]]
            [fast-twitch.celld.context :as context]
            [fast-twitch.celld.rpc :as rpc]
            [fast-twitch.celld.storage :as storage]
            [fast-twitch.celld.storage.kv :as kv]
            [fast-twitch.celld.sql :as sql]
            [fast-twitch.celld.alarms :as alarms]
            [fast-twitch.celld.bindings :as bindings]
            [fast-twitch.celld.identity :as identity]
            [fast-twitch.celld.names :as names]
  )
  (:require-macros [fast-twitch.celld.macros :refer
                    [defcell defrpc deffetch defworker defcell-init defalarm
                     with-transaction-sync]])
  (:refer-global :only [Promise Date Map Set BigInt Uint8Array Array crypto]))

(defcell-init
  initialize
  [ctx]
  (sql/drain!
    (sql/exec (sql/native (context/storage ctx))
              {:create-table [:checks :if-not-exists]
               :with-columns [[:id :integer :primary-key] [:value :text]]})))

(defrpc
  ^:async run-checks
  {:method :runChecks :args [:cat] :returns [:map-of :keyword :boolean] :codec :json}
  [ctx]
  (let [s (context/storage ctx)
        k (kv/native s)
        db (sql/native s)
        _ (await (storage/put! s :nil nil))
        nil-value (await (storage/get! s :nil))
        missing (await (storage/get! s :missing))
        _ (await (storage/put-many! s
                                    {:false false :json {:items [nil false 42]}}
                                    {:codec :json}))
        batch (await (storage/get! s [:nil :missing]))
        _ (kv/put! k :sync 7)
        sync-value (kv/get-value k :sync)
        _ (with-transaction-sync s (kv/put! k :committed 9))
        rollback? (try (with-transaction-sync s
                                              (kv/put! k :rolled 1)
                                              (throw (ex-info "rollback" {})))
                       false
                       (catch :default _ (= {:found? false} (kv/get-value k :rolled))))
        thenable? (try (storage/transaction-sync! s
                                                  (fn []
                                                    (kv/put! k :thenable 1)
                                                    (Promise.resolve 3)))
                       false
                       (catch :default _ (= {:found? false} (kv/get-value k :thenable))))
        _ (await (storage/transaction!
                   s
                   (^:async fn [txn] (await (storage/put! txn :async 11)))))
        row (sql/exactly-one (sql/exec
                               db
                               {:insert-into :checks
                                :columns [:value]
                                :values [[[:param :record/value]]]
                                :returning [:value]}
                               {:record/value "written"}))
        _ (await (storage/delete! s :alarmed))
        _ (await (storage/delete! s :alarm-metadata))
        _ (await (alarms/set-after! s 150))
        alarm-time (await (alarms/get-time! s))]
    {:nil-present (= {:found? true :value nil} nil-value)
     :missing (= {:found? false} missing)
     :batch (= {:nil nil} batch)
     :codec (= {:items [nil false 42]}
               (:value (await (storage/get! s :json {:codec :json}))))
     :sync (= {:found? true :value 7} sync-value)
     :rollback rollback?
     :thenable-rollback thenable?
     :async-transaction (= 11 (:value (await (storage/get! s :async))))
     :returning-drained (= "written" (get row :value))
     :alarm-scheduled (number? alarm-time)}))

(defrpc ^:async alarm-state
        {:method :alarmState :args [:cat] :returns :boolean}
        [ctx]
        (= true (:value (await (storage/get! (context/storage ctx) :alarmed)))))

(defrpc ^:async alarm-metadata
        {:method :alarmMetadata :args [:cat] :returns :boolean}
        [ctx]
        (= true (:value (await (storage/get! (context/storage ctx) :alarm-metadata)))))

(defrpc effect
        {:method :effect :args [:cat :int] :returns :int}
        [ctx value]
        (kv/put! (kv/native (context/storage ctx)) :effect value)
        value)

(defrpc bad-output
        {:method :badOutput :args [:cat] :returns :int}
        [_ctx]
        "invalid")

(defalarm ^:async wake
          [ctx info]
          (let [{:keys! [retry-count is-retrying? scheduled-time]} info]
            (await (storage/put! (context/storage ctx)
                                 :alarm-metadata
                                 (and (zero? retry-count)
                                      (false? is-retrying?)
                                      (number? scheduled-time))))
            (await (storage/put! (context/storage ctx) :alarmed true))))

(defrpc graph
        {:method :graph :args [:cat :any] :returns :any}
        [_ctx value]
        value)

(defrpc
  ^:async storage-coverage
  {:method :storageCoverage
   :args [:cat]
   :returns [:map-of :keyword :boolean]
   :codec :json}
  [ctx]
  (try
    (let [s (context/storage ctx)
          sync (kv/native s)
          value #js {:big (BigInt "9007199254740993")
                     :bytes (.subarray (Uint8Array. #js [99 1 2 3 99]) 1 4)
                     :date (Date. 1234)
                     :map (Map. #js [#js [1 false]])
                     :set (Set. #js [7])}
          _ (aset value (names/text :self) value)
          _ (await (storage/put! s :native-graph value))
          restored (:value (await (storage/get! s :native-graph)))
          _ (await (storage/put! s (keyword "") false))
          empty-key (:value (await (storage/get! s (keyword ""))))
          _ (await (storage/put! s {:coverage/a 1 :coverage/b 2 :coverage/c 3}))
          listed (await (storage/list! s
                                       {:prefix (keyword "coverage/")
                                        :startAfter :coverage/a
                                        :end :coverage/d
                                        :reverse true
                                        :limit 2}))
          _ (await (storage/put! s
                                 (into {}
                                       (for [n (range 14)]
                                         [(keyword (str "ordered/"
                                                        (.padStart (str n) 2 "0"))) n]))))
          ordered (await (storage/list-entries! s
                                                {:prefix (keyword "ordered/")
                                                 :startAfter (keyword "ordered/00")
                                                 :end (keyword "ordered/13")
                                                 :reverse true}))
          deleted (await (storage/delete! s
                                          [:coverage/a :coverage/b :coverage/c
                                           :coverage/missing]))
          _ (kv/put! sync :coverage/sync nil)
          sync-list (kv/list-values sync {:prefix :coverage/sync} :native)
          iterator (kv/list-native sync {:prefix (keyword "coverage/")})
          _ (kv/list-native sync)
          invalidated? (try (.next iterator) false (catch :default _ true))
          sync-deleted (kv/delete! sync :coverage/sync)
          escaped (atom nil)
          _ (await (storage/transaction! s
                                         (^:async fn
                                          [txn]
                                          (reset! escaped txn)
                                          (await
                                            (storage/put! txn :coverage/escape 1)))))
          terminal? (try (await (storage/get! @escaped :coverage/escape))
                         false
                         (catch :default _ true))
          rollback? (try (await (storage/transaction!
                                  s
                                  (^:async fn
                                   [txn]
                                   (await (storage/put! txn :coverage/rolled 1))
                                   (storage/rollback! txn))))
                         (= {:found? false} (await (storage/get! s :coverage/rolled)))
                         (catch :default _
                           (= {:found? false}
                              (await (storage/get! s :coverage/rolled)))))
          batch-rejected? (try (await (storage/put! s
                                                    {:coverage/before-bad 1
                                                     :coverage/bad (fn []
                                                                     nil)}))
                               false
                               (catch :default _
                                 (= {:found? false}
                                    (await (storage/get! s :coverage/before-bad)))))
          _ (await (storage/sync! s))]
      {:native-cycle (identical? restored (aget restored "self"))
       :native-bigint (= (BigInt "9007199254740993") (aget restored "big"))
       :native-bytes (= [1 2 3] (vec (array-seq (Array.from (aget restored "bytes")))))
       :native-date (= 1234 (.getTime (aget restored "date")))
       :native-map (false? (.get (aget restored "map") 1))
       :native-set (.has (aget restored "set") 7)
       :ordered-reverse-range (= (mapv (fn [n] [(keyword (str "ordered/"
                                                              (.padStart (str n) 2 "0")))
                                                n])
                                   (range 12 0 -1))
                                 ordered)
       :empty-key (false? empty-key)
       :batch-put-list (= {:coverage/c 3 :coverage/b 2} listed)
       :batch-delete-count (= 3 deleted)
       :sync-list-null (= [[:coverage/sync nil]] sync-list)
       :sync-iterator-invalidated invalidated?
       :sync-delete sync-deleted
       :transaction-view-terminal terminal?
       :explicit-rollback rollback?
       :batch-before-effects batch-rejected?})
    (catch :default error (throw (js/Error. (str error " " (pr-str (ex-data error))))))))

(defrpc
  resource-checks
  {:method :resourceChecks :args [:cat] :returns [:map-of :keyword :boolean] :codec :json}
  [ctx]
  (let
    [db (sql/native (context/storage ctx))
     _ (sql/drain! (sql/exec db {:delete-from :checks}))
     write (sql/exec db
                     {:insert-into :checks
                      :columns [:value]
                      :values [[[:param :a]] [[:param :b]] [[:param :c]]]
                      :returning [:value]}
                     {:a "a" :b "b" :c "c"})
     overflow? (try (sql/all-rows write 1) false (catch :default _ true))
     written (= 3
                (get (sql/exactly-one
                       (sql/exec db {:select [[[:count :*] :n]] :from [:checks]}))
                     :n))
     throwing-write (sql/exec db
                              {:insert-into :checks
                               :columns [:value]
                               :values [[[:param :d]] [[:param :e]] [[:param :f]]]
                               :returning [:value]}
                              {:d "d" :e "e" :f "f"})
     primary? (try (sql/reduce-rows throwing-write
                                    (fn [_ _]
                                      (throw (js/Error. "primary-fixture")))
                                    nil)
                   false
                   (catch :default error (= "primary-fixture" (.-message error))))
     thrown-drained (= 6
                       (get (sql/exactly-one
                              (sql/exec db {:select [[[:count :*] :n]] :from [:checks]}))
                            :n))
     cursor (sql/exec
              db
              {:with-recursive [[[:n {:columns [:x]}]
                                 {:union-all [{:values [[[:inline 1]]]}
                                              {:select [[[:+ :x [:inline 1]] :x]]
                                               :from [:n]
                                               :where [:< :x [:param :maximum]]}]}]]
               :select [:x]
               :from [:n]}
              {:maximum 10000})
     calls (atom 0)
     result (sql/reduce-rows
              cursor
              (fn [acc _row]
                (if (= 10 (swap! calls inc)) (reduced (inc acc)) (inc acc)))
              0)]
    {:bounded-overflow overflow?
     :write-drained written
     :primary-error primary?
     :throwing-write-drained thrown-drained
     :reduced-count (= 10 result)
     :reducer-stopped (= 10 @calls)
     :native-cursor-drained (:done? (sql/next-row cursor))
     :metadata (= [:x] (:column-names (sql/metadata cursor)))
     :database-size (pos? (sql/database-size db))}))

(defrpc
  ^:async member-checks
  {:method :memberChecks :args [:cat] :returns [:map-of :keyword :boolean] :codec :json}
  [ctx]
  (let [s (context/storage ctx)
        k (kv/native s)
        db (sql/native s)
        _ (await (storage/put! s :owned 1))
        flushed (await (storage/sync! s))
        _ (await (alarms/set-at! s (Date. (+ (Date.now) 60000))))
        scheduled (await (alarms/get-time! s))
        _ (await (alarms/delete! s))
        absent-alarm (await (alarms/get-time! s))
        raw-cursor (sql/exec db
                             {:union-all [{:select [[[:inline 1] :n]]}
                                          {:select [[[:inline 2] :n]]}]})
        raw-rows (mapv vec (array-seq (Array.from (sql/raw raw-cursor))))
        native-rows (mapv #(aget % "n")
                      (array-seq
                        (Array.from
                          (sql/cursor-native
                            (sql/exec db
                                      {:union-all [{:select [[[:inline 3] :n]]}
                                                   {:select [[[:inline 4] :n]]}]})))))
        counters (sql/metadata raw-cursor)
        collected (sql/all-rows (sql/exec db
                                          {:union-all [{:select [[[:inline 1] :n]]}
                                                       {:select [[[:inline 2] :n]]}]}))
        empty-row (sql/zero-or-one (sql/exec db
                                             {:select [[[:inline 1] :n]]
                                              :where [:= [:inline 0] [:inline 1]]}))
        one-error? (try (sql/exactly-one (sql/exec db
                                                   {:select [[[:inline 1] :n]]
                                                    :where [:= [:inline 0] [:inline 1]]}))
                        false
                        (catch :default _ true))
        formatted (sql/exactly-one (sql/exec db {:select [[[:inline 42] :answer]]}))
        _ (await (storage/delete-all! s))
        cleared (await (storage/list-entries! s))]
    {:storage-native (identical? s (storage/native s))
     :storage-sync (nil? flushed)
     :storage-delete-all (empty? cleared)
     :storage-kv-view (identical? k (kv/native s))
     :storage-sql-view (identical? db (sql/native s))
     :alarm-date (number? scheduled)
     :alarm-delete (nil? absent-alarm)
     :sql-raw (= [[1] [2]] raw-rows)
     :sql-column-names (= [:n] (:column-names counters))
     :sql-native-iterator (= [3 4] native-rows)
     :sql-row-counters (and (number? (:rows-read counters))
                            (number? (:rows-written counters)))
     :sql-collected (= [{:n 1} {:n 2}] collected)
     :sql-one-empty-reject one-error?
     :sql-zero-or-one (nil? empty-row)
     :sql-honeysql (= 42 (get formatted :answer))}))

(defcell StorageCell
         {:binding :STORAGE
          :include [initialize run-checks alarm-state alarm-metadata effect bad-output
                    wake resource-checks storage-coverage graph member-checks]})

(deffetch
  ^:async worker-fetch
  [ctx request]
  (let [stub (identity/get-by-name (bindings/get-binding ctx :STORAGE) "suite")]
    (case (:uri request)
      "/resources" {:status 200
                    :body (json/encode
                            (await (rpc/call! stub :resourceChecks [] {:codec :json})))}
      "/coverage" {:status 200
                   :body (json/encode
                           (await (rpc/call! stub :storageCoverage [] {:codec :json})))}
      "/members" (let [owned (identity/get-by-name (bindings/get-binding ctx :STORAGE)
                                                   (str "owned-clear-"
                                                        (crypto.randomUUID)))]
                   {:status 200
                    :body (json/encode
                            (await (rpc/call! owned :memberChecks [] {:codec :json})))})
      "/native-rpc-graph" (let [value #js {:big (BigInt "9007199254740993")}
                                _ (aset value (names/text :self) value)
                                result (await (rpc/call! stub :graph [value]))]
                            {:status 200
                             :body (json/encode
                                     {:rpc-cycle (identical? result (aget result "self"))
                                      :rpc-bigint (= (BigInt "9007199254740993")
                                                     (aget result "big"))})})
      "/alarm" {:status 200 :body (str (await (rpc/call! stub :alarmState [])))}
      "/alarm-metadata" {:status 200
                         :body (str (await (rpc/call! stub :alarmMetadata [])))}
      "/bad-input" (try (await (rpc/call! stub :effect ["bad"]))
                        {:status 500 :body "guard missed"}
                        (catch :default _ {:status 200 :body "rejected"}))
      "/bad-output" (try (await (rpc/call! stub :badOutput []))
                         {:status 500 :body "guard missed"}
                         (catch :default _ {:status 200 :body "rejected"}))
      {:status 200
       :headers {:content-type "application/json"}
       :body (json/encode (await (rpc/call! stub :runChecks [] {:codec :json})))})))

(defworker App {:include [worker-fetch]})
