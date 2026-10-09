(ns fast-twitch.celld.validation
  "Installs declared Malli contracts in release output.")

(defmacro instrument!
  "Installs the namespace's declared Malli contracts using the shared bounded reporter."
  [& functions]
  `(do ~@(for [f functions]
           `(set! ~f
                  (fast-twitch.celld.validation/instrument
                    '~(symbol (str (get-in &env [:ns :name])) (name f))
                    ~f)))))
