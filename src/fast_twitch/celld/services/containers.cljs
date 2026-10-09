(ns fast-twitch.celld.services.containers
  "Experimental Celld container/process/port capabilities. Disk is ephemeral.
  Monitor and exec do not keep a Cell alive after its handler returns; sockets
  belong to the event that opened them. Ordinary Cells need no engine or SDK."
  (:require [fast-twitch.celld.native :as n]
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

(defn start!
  "Starts synchronously; native engine startup errors surface through monitor.
  Positive hardTimeout is accepted but ineffective natively and excluded here."
  ([container]
   (n/invoke container "start" []))
  ([container options]
   (n/invoke
     container
     "start"
     [(n/options options #{:entrypoint :env :enableInternet :labels} :container-start)])))

(defn monitor!
  "Returns the native exit Promise. It is owned by the current invocation."
  [container]
  (n/invoke container "monitor" []))

(defn destroy!
  "Stops the native container; optional error is preserved as the monitor rejection."
  ([container]
   (n/invoke container "destroy" []))
  ([container error]
   (n/invoke container "destroy" [error])))

(defn signal!
  "Synchronously requests a native numeric signal; native engine errors remain native."
  [container number]
  (n/invoke container
            "signal"
            [(v/check! [:int {:min 1 :max 64}] number :container-signal)]))

(defn tcp-port
  "Returns a native container port capability synchronously."
  [container port]
  (n/invoke container
            "getTcpPort"
            [(v/check! [:int {:min 1 :max 65535}] port :container-port)]))

(defn client-for
  "Selects receiver-preserving Fast-Twitch Fetch for a native port; use HTTP URLs."
  ([port]
   (http/client-for port))
  ([port options]
   (http/client-for port options)))

(defn connect!
  "Returns the shared TCP representation of a native event-scoped port socket."
  ([port]
   (communication/adapt-socket (n/invoke port "connect" [])))
  ([port options shared-options]
   (communication/check-options! shared-options)
   (communication/adapt-socket
     (n/invoke port
               "connect"
               [nil (n/options options #{:secureTransport :allowHalfOpen} :tcp-connect)])
     shared-options)))

(defn exec!
  "Returns a Promise of a native process with explicit stdin/stdout/stderr modes."
  ([container command]
   (v/check! [:vector {:min 1} :string] command :container-command)
   (n/invoke container "exec" [(to-array command)]))
  ([container command options]
   (v/check! [:vector {:min 1} :string] command :container-command)
   (n/invoke container
             "exec"
             [(to-array command)
              (n/options options
                         #{:env :cwd :user :stdin :stdout :stderr}
                         :container-exec)])))

(defn set-inactivity-timeout!
  "Returns the native Promise setting the positive idle window in milliseconds; await to observe rejection."
  [container duration]
  (n/invoke container
            "setInactivityTimeout"
            [(v/check! [:int {:min 1}] duration :container-inactivity)]))

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

(defn kill!
  "Synchronously requests a process signal; omitted native signal defaults to 15."
  ([process]
   (n/invoke process "kill" []))
  ([process signal]
   (n/invoke process "kill" [(v/check! [:int {:min 1 :max 64}] signal :process-signal)])))
