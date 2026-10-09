(ns fast-twitch.celld.rpc
  "Native RPC checks application arguments before effects and successful results after.
  Native rejections propagate; no operation is retried by these helpers."
  (:require [cljs.core :refer [await]]
            [fast-twitch.celld.codec :as codec]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.contracts :as contracts])
  (:refer-global :only [Reflect]))

(defn ^:async serve!
  "Decodes application arguments, checks before effects, awaits once, then checks and encodes successful output; rejects unchanged."
  [handler context args options]
  (let [_ (v/check! (:rpc-options contracts/schemas) options :rpc-options)
        {:keys [args-schema returns codec]} options
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

(defn ^:async call!
  "Calls one stable native method. Contract schemas describe application values."
  ([stub method args]
   (call! stub method args {}))
  ([stub method args options]
   (let [_ (v/check! (:rpc-options contracts/schemas)
                     options
                     :rpc-options
                     {:method method})
         {:keys [args-schema returns codec]} options
         _ (v/check! contracts/identity-value method :rpc-method {:method method})
         identity {:method method}]

     (v/check! [:vector :any] args :rpc-arguments identity)
     (when args-schema (v/check! args-schema args :rpc-input {:method method}))
     (let [native-args (v/at! identity
                              #(mapv (fn [value]
                                       (fast-twitch.celld.codec/encode codec value))
                                 args))
           f (v/method! stub method)
           native-result (await (Reflect.apply f stub (to-array native-args)))
           result (v/at! identity #(fast-twitch.celld.codec/decode codec native-result))]
       (when returns (v/check! returns result :rpc-output {:method method}))
       result))))
