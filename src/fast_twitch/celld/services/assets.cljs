(ns fast-twitch.celld.services.assets
  "Assets/service Fetch selects the shared client; no file server or address is synthesized."
  (:require [fast-twitch.celld.http :as http]))

(defn native
  "Returns the original native Fetch binding."
  [binding]
  binding)

(defn client-for
  "Returns the receiver-preserving Fast-Twitch client for an assets or service binding."
  ([binding]
   (http/client-for binding))
  ([binding options]
   (http/client-for binding options)))

(defn fetch!
  "Forwards the current request map with native origin/stream/upgrade preservation."
  [binding request]
  (http/forward! binding request))
