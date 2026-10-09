(ns fast-twitch.celld.services.containers
  "Experimental Celld container/process/port capabilities. Disk is ephemeral.
  Monitor and exec do not keep a Cell alive after its handler returns; sockets
  belong to the event that opened them. Ordinary Cells need no engine or SDK."
  (:require [fast-twitch.celld.contracts :as contracts]
            [malli.experimental :as mx]
            [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [cljs.core :refer [await]]
            [fast-twitch.celld.http :as http]
            [fast-twitch.celld.communication :as communication]))

(defn native
  "Returns the original native container/process/port handle."
  [handle]
  handle)

(defn running?
  "Reads native engine state synchronously, without caching a local flag."
  [container]
  (n/property container "running"))

(mx/defn ^:dynamic start!
  "Starts synchronously; native engine startup errors surface through monitor.
  Positive hardTimeout is accepted but ineffective natively and excluded here."
  [container & [options :as supplied] :- [:? (:container-start contracts/schemas)]]
  (n/invoke container "start" (if supplied [(n/option-fields options)] [])))

(defn monitor!
  "Returns the native exit Promise. It is owned by the current invocation."
  [container]
  (n/invoke container "monitor" []))

(mx/defn ^:dynamic destroy!
  "Stops the native container; optional error is preserved as the monitor rejection."
  [container & [error :as supplied] :- [:? :any]]
  (n/invoke container "destroy" (if supplied [error] [])))

(mx/defn ^:dynamic signal!
  "Synchronously requests a native numeric signal; native engine errors remain native."
  [container number :- [:int {:min 1 :max 64}]]
  (n/invoke container "signal" [number]))

(mx/defn ^:dynamic tcp-port
  "Returns a native container port capability synchronously."
  [container port :- [:int {:min 1 :max 65535}]]
  (n/invoke container "getTcpPort" [port]))

(defn client-for
  "Selects receiver-preserving Fast-Twitch Fetch for a native port; use HTTP URLs."
  ([port]
   (http/client-for port))
  ([port options]
   (http/client-for port options)))

(mx/defn ^{:dynamic true} connect!
  "Returns the shared TCP representation of a native event-scoped port socket."
  ([port]
   (communication/adapt-socket (n/invoke port "connect" [])))
  ([port options :- (:tcp-connect contracts/schemas) shared-options]
   (communication/check-options! shared-options)
   (communication/adapt-socket (n/invoke port "connect" [nil (n/option-fields options)])
                               shared-options)))

(mx/defn ^:dynamic exec!
  "Returns a Promise of a native process with explicit stdin/stdout/stderr modes."
  [container command :- [:vector {:min 1} :string] & [options :as supplied] :-
   [:? (:container-exec contracts/schemas)]]
  (n/invoke container
            "exec"
            (cond-> [(to-array command)] supplied (conj (n/option-fields options)))))

(mx/defn ^:dynamic set-inactivity-timeout!
  "Returns the native Promise setting the positive idle window in milliseconds; await to observe rejection."
  [container duration :- [:int {:min 1}]]
  (n/invoke container "setInactivityTimeout" [duration]))

(defn process-map
  "Projects live process streams/PID/exit Promise; consuming them remains caller-owned."
  [process]
  {:pid (n/property process "pid")
   :stdin (n/property process "stdin")
   :stdout (n/property process "stdout")
   :stderr (n/property process "stderr")
   :exit-code (n/property process "exitCode")
   :fast-twitch.container/process process})

(defn ^:async output!
  "Returns native whole process output once; cannot follow separate stream consumption."
  [process]
  (n/data-map (await (n/invoke process "output" []))))

(mx/defn ^:dynamic kill!
  "Synchronously requests a process signal; omitted native signal defaults to 15."
  [process & [signal :as supplied] :- [:? [:int {:min 1 :max 64}]]]
  (n/invoke process "kill" (if supplied [signal] [])))

(v/instrument! start!
               destroy!
               signal!
               tcp-port
               connect!
               exec!
               set-inactivity-timeout!
               kill!)
