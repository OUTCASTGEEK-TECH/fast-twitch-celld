(ns fast-twitch.celld.http
  "Celld entrypoint and binding Fetch adapters using the shared HTTP conversions."
  (:require [cljs.core :refer [await]]
            [fast-twitch.client.http :as client]
            [fast-twitch.util.http.request :as request]
            [fast-twitch.util.http.response :as response]
            [fast-twitch.server.websocket :as upgrade]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.websocket :as websocket]))

(defn client-for
  "Returns a Fast-Twitch client for a native fetch capability; keeps its receiver.
  AbortSignal and current request edits pass through the existing converter."
  ([binding]
   (client-for binding {}))
  ([binding options]
   (v/method! binding "fetch")
   (let [user-check (:request-check options)]
     (when (contains? options :request-check)
       (v/check! [:fn fn?] user-check :fetch-options))
     (client/make-client
       (assoc options
         :request-check (fn [request-map effective]
                          (when (contains? (:request-init effective) :cache)
                            (v/fail! :fetch :unavailable
                                     "Celld 0.6.2 does not support Request cache options."
                                       {:path [:request-init :cache] :expected :omitted}))
                          (when user-check (user-check request-map effective)))
         :transport (fn [native _]
                      (n/invoke binding "fetch" [native])))))))

(defn ^:async forward!
  "Forwards the current request map through the native binding using shared conversion; resolves to a preserved response map."
  [binding request]
  (await ((client-for binding) request)))

(defn ^:async handle!
  "Invokes one map handler, retaining the native Request in Fast-Twitch metadata.
  Native mode passes the native Request directly. All results share finalization."
  [handler context native native?]
  (let [_ (when (= :cell (:kind context))
            (upgrade/bind! native (websocket/upgrade-capability context)))
        view (if native?
               native
               (assoc (request/request->map native) :fast-twitch.celld/context context))
        value (await (handler context view))]
    (if (upgrade/listener-response? value)
      (upgrade/upgrade! [native value])
      (response/map->response (response/normalize value)))))

(defn mount
  "Edits the current Ring URI while retaining native request metadata and body ownership."
  [request path]
  (assoc request :uri path))
