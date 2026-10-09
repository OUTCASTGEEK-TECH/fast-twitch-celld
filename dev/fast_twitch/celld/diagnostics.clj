(ns fast-twitch.celld.diagnostics
  "Structured public diagnostics traverse compiler causes without dumping source payloads.")

(defn diagnostic
  "Returns bounded semantic/reader/compiler category, location and repair from a cause chain; never includes raw source or arbitrary exception data."
  [error]
  (let [causes (take-while some? (iterate #(.getCause ^Throwable %) error))
        semantic (some #(when (:code (ex-data %)) (ex-data %)) causes)
        locations (keep (fn [cause]
                          (let [data (ex-data cause)
                                line (or (:clojure.error/line data) (:line data))
                                column (or (:clojure.error/column data) (:column data))
                                file (or (:clojure.error/source data)
                                         (:file data)
                                         (:source data))]
                            (when line
                              (cond-> {:line line}
                                column (assoc :column column)
                                (string? file) (assoc :file file)))))
                        causes)
        source (when (seq locations)
                 (merge (select-keys (first (filter :file locations)) [:file])
                        (last locations)))]
    (merge
      {:code (if source :fast-twitch.celld.build/source :fast-twitch.celld.build/compiler)
       :declaration :application
       :path []
       :expected :valid-source
       :received {:type (str (type error))}
       :repair "Correct the application source or selected build input."}
      (when source {:source source :compiler-source source})
      (select-keys (cond-> semantic (nil? (:source semantic)) (dissoc :source))
                   [:code :source :sources :declaration :path :expected :received
                    :repair]))))
