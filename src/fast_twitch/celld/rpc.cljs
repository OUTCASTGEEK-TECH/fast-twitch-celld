(ns fast-twitch.celld.rpc
  "Native RPC checks application arguments before effects and successful results after.
  Native rejections propagate; no operation is retried by these helpers."
  (:require [malli.experimental :as mx]
            [cljs.core :refer [await]]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.contracts :as contracts])
  (:refer-global :only [Reflect]))

(mx/defn ^{:dynamic true :async true} serve!
  "Decodes application arguments, checks before effects, awaits once, then checks and encodes successful output; rejects unchanged."
  [handler :- [:fn fn?] context args :- [:vector :any] options :-
   (:rpc-options contracts/schemas)]
  (let [{:keys [args-schema returns codec]} options
        identity {:method (:fast-twitch.celld/method context)}
        values (v/at! identity
                      #(mapv (fn [value]
                               (fast-twitch.celld.codec/decode codec value))
                         args))]
    (when args-schema
      (v/check! args-schema
                values
                :rpc-input
                {:method (:fast-twitch.celld/method context)}))
    (let [result (await (apply handler context values))]
      (when returns
        (v/check! returns
                  result
                  :rpc-output
                  {:method (:fast-twitch.celld/method context)}))
      (v/at! identity #(fast-twitch.celld.codec/encode codec result)))))

(mx/defn ^{:dynamic true :async true} call!
  "Calls one stable native method. Contract schemas describe application values."
  [stub method :- contracts/identity-value args :- [:vector :any] & [options] :-
   [:? (:rpc-options contracts/schemas)]]
  (let [{:keys [args-schema returns codec]} options
        identity {:method method}]
    (when args-schema (v/check! args-schema args :rpc-input identity))
    (let [native-args (v/at! identity
                             #(mapv (partial fast-twitch.celld.codec/encode codec) args))
          f (v/method! stub method)
          native-result (await (Reflect.apply f stub (to-array native-args)))
          result (v/at! identity #(fast-twitch.celld.codec/decode codec native-result))]
      (when returns (v/check! returns result :rpc-output identity))
      result)))

(set! serve!
      (v/instrument 'fast-twitch.celld.rpc/serve!
                    serve!
                    #(hash-map :method (:fast-twitch.celld/method (nth % 1 nil)))))

(set! call!
      (v/instrument 'fast-twitch.celld.rpc/call!
                    call!
                    #(hash-map :method (nth % 1 nil))))
