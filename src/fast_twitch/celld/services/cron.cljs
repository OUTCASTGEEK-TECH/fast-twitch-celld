(ns fast-twitch.celld.services.cron
  "Native UTC scheduled controllers. Cell alarms have a distinct native scope."
  (:require [fast-twitch.celld.native :as n]))

(defn controller-map
  "Returns the original controller plus cron text and absolute scheduled milliseconds."
  [controller]
  {:cron (n/property controller "cron")
   :scheduled-time (n/property controller "scheduledTime")
   :fast-twitch.cron/controller controller})

(defn no-retry!
  "Synchronously suppresses retries for this native scheduled occurrence."
  [controller]
  (n/invoke controller "noRetry" []))
