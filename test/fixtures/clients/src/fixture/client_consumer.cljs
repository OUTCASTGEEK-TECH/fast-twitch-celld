(ns fixture.client-consumer
  (:require [cljs.core :refer [await]]
            [fixture.client-server]
            [fast-twitch.celld.generated.client-fixture :as clients]
            [fast-twitch.celld.bindings :as bindings]
            [fast-twitch.celld.identity :as identity]
            [fast-twitch.codecs.json :as json])
  (:require-macros [fast-twitch.celld.macros :refer [deffetch defworker]]))

(deffetch ^:async fetch
          [ctx _request]
          (let [upper (identity/get-by-name (bindings/get-binding ctx :UPPER) "fixture")
                lower (identity/get-by-name (bindings/get-binding ctx :LOWER) "fixture")
                a (await (clients/Foo-first! upper 3))
                b (await (clients/Foo-second! upper 4))
                c (await (clients/foo-first! lower 5))
                invalid? (try (await (clients/Foo-first! upper "invalid"))
                              false
                              (catch :default _ true))]
            {:status 200
             :body (json/encode {:upper-first (= 3 a)
                                 :upper-second (= 4 b)
                                 :lower-first (= 5 c)
                                 :invalid-before-call invalid?})}))

(defworker App {:include [fetch]})
