(ns fast-twitch.celld.alarms
  "One native alarm per Cell; times are absolute milliseconds. No scheduler is installed."
  (:require [fast-twitch.celld.native :as n] [fast-twitch.celld.validation :as v])
  (:refer-global :only [Date]))

(defn set-at!
  "Returns the native setAlarm Promise for an absolute millisecond timestamp or Date. Facet alarms reject natively."
  [storage timestamp]
  (let [ms (if (instance? Date timestamp) (.getTime timestamp) timestamp)]
    (v/check! [:and :int [:>= 0]] ms :alarm-time)
    (v/safe-number! ms :alarm-time)
    (n/invoke storage "setAlarm" [ms])))

(defn set-after!
  "Schedules after a positive duration in milliseconds, explicitly."
  [storage duration-ms]
  (v/check! [:int {:min 1}] duration-ms :alarm-duration)
  (set-at! storage (+ (Date.now) duration-ms)))

(defn get-time!
  "Returns native absolute milliseconds or nil when absent."
  [storage]
  (n/invoke storage "getAlarm" []))

(defn delete!
  "Returns the native deleteAlarm Promise; removes the one Cell alarm."
  [storage]
  (n/invoke storage "deleteAlarm" []))
