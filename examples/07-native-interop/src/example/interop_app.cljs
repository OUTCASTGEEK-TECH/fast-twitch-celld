(ns example.interop-app
  "Selected ordinary native interop, compiled and executed in the final Celld bundle."
  (:require [cljs.core :refer [await]]
            [fast-twitch.codecs.json :as json]
            [fast-twitch.client.core :as client])
  (:require-global ["node:path" :as path])
  (:require-macros [fast-twitch.celld.macros :refer [deffetch defworker]])
  (:refer-global :only
                 [WebAssembly Uint8Array TextEncoder TextDecoder Headers URL
                  URLSearchParams Response HTMLRewriter caches crypto Promise setTimeout
                  clearTimeout]))

(deffetch
  ^:async interop
  [_ctx _request]
  (let [bytes (Uint8Array. #js [0 97 115 109 1 0 0 0 1 7 1 96 2 127 127 1 127 3 2 1 0 7 7
                                1 3 97 100 100 0 0 10 9 1 7 0 32 0 32 1 106 11])
        module (await (.instantiate WebAssembly bytes))
        digest (await
                 (.digest (.-subtle crypto) "SHA-256" (.encode (TextEncoder.) "fixture")))
        rewritten (.transform (.on (HTMLRewriter.)
                                   "p"
                                   #js {:element (fn [element]
                                                   (.setInnerContent element "after"))})
                              (Response. "<p>before</p>"))
        timer-called (atom false)
        canceled-timer (setTimeout (fn []
                                     (reset! timer-called true))
                                   1)
        _ (clearTimeout canceled-timer)
        _ (await (Promise. (fn [resolve _]
                             (setTimeout resolve 20))))
        cache (.-default caches)
        _ (await (.put cache "http://fixture/cache" (Response. "ignored")))
        cached (await (.match cache "http://fixture/cache"))
        cache-rejected (try (await (client/request! {:url "http://127.0.0.1:18991/"
                                                     :request-method :get}
                                                    {:transport :http
                                                     :request-middleware
                                                       [(fn [req]
                                                          (assoc-in req
                                                            [:fast-twitch.client/options
                                                             :request-init :cache]
                                                            :no-store))]}))
                            false
                            (catch :default _ true))]
    {:status 200
     :body (json/encode
             {:wasm (= 7 ((aget (.-exports (.-instance module)) "add") 3 4))
              :text-codec (= "fixture"
                             (.decode (TextDecoder.)
                                      (.encode (TextEncoder.) "fixture")))
              :url-search-params (= "x y" (.get (URLSearchParams. "a=x+y") "a"))
              :timers (not @timer-called)
              :webcrypto (= 32 (.-byteLength digest))
              :htmlrewriter (= "<p>after</p>" (await (.text rewritten)))
              :cache-always-miss (nil? cached)
              :fetch-cache-after-middleware cache-rejected
              :node-path (= "/a/b" (path/join "/a" "b"))
              :standard-url
                (= "1" (.get (.-searchParams (URL. "https://fixture/path?a=1")) "a"))
              :headers (= "native"
                          (.get (Headers. #js {:x-fixture "native"}) "x-fixture"))})}))

(defworker App {:include [interop]})
