;; Source inventory: reads forms only; application code is never evaluated.
(require '[clojure.java.io :as io] '[clojure.data.json :as json])

(defn forms
  [file]
  (with-open [reader (java.io.PushbackReader. (io/reader file))]
    (binding [*data-readers* {'js identity}]
      (loop [out []]
        (let [form (read {:eof ::done :read-cond :allow :features #{:cljs}} reader)]
          (if (= ::done form) out (recur (conj out form))))))))

(def output
  (for [file (sort-by str (file-seq (io/file "src/fast_twitch/celld")))
        :when (and (.isFile file) (re-find #"\.cljs$" (.getName file)))
        :let [fs (forms file)
              namespace (second (first fs))]
        form fs
        :when (and (seq? form)
                   (= 'defn (first form))
                   (not (or (:private (meta (second form)))
                            (:no-doc (meta (second form))))))
        :let [[_ name & rest] form
              [doc rest] (if (string? (first rest)) [(first rest) (next rest)] [nil rest])
              arglists (if (vector? (first rest)) [(first rest)] (map first rest))]]
    {:namespace (str namespace)
     :name (str name)
     :file (str file)
     :doc doc
     :arglists (mapv pr-str arglists)
     :native-members (vec (distinct (for [x (tree-seq coll? seq form)
                                          :when (and (seq? x)
                                                     (symbol? (first x))
                                                     (#{"invoke" "property"}
                                                      (clojure.core/name (first x)))
                                                     (string? (nth x 2 nil)))]
                                      (nth x 2))))}))

(spit "target/public-api.json" (json/write-str (vec output)))

(prn (count output))

(require 'clojure.edn)

(spit "target/catalog.json"
      (json/write-str (clojure.edn/read-string
                        (slurp "resources/fast_twitch/celld/api-catalog.edn"))))
