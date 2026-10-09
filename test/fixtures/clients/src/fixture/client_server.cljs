(ns fixture.client-server
  (:require-macros [fast-twitch.celld.macros :refer [defcell defcontract]]))

(defcontract Arguments [:cat :int])

(defn echo
  [_ctx value]
  value)

(defcell Foo
         {:binding :UPPER
          :rpc {:first {:handler echo :args Arguments :returns :int}
                :second {:handler echo :args Arguments :returns :int}}})

(defcell foo
         {:binding :LOWER :rpc {:first {:handler echo :args Arguments :returns :int}}})
