(ns fast-twitch.celld.services.facets
  "Native facets retain root ownership and independent storage. Parent transactions
  cannot roll back facet effects. Facet sockets keep the root resident, without hibernation."
  (:require [fast-twitch.celld.native :as n]
            [fast-twitch.celld.validation :as v]
            [fast-twitch.celld.names :as names]
            [fast-twitch.celld.codec :as codec]))

(defn native
  "Returns the original native facet registry/stub/class handle."
  [handle]
  handle)

(defn startup
  "Builds native {class,id?}; class must come from a native loader or unmigrated
  ctx.exports class. Omitted ID inherits root ID and retains its name."
  ([class]
   #js {:class class})
  ([class id]
   #js {:class class :id id}))

(defn specialize-class
  "Copies native startup props through the native loopback class specialization."
  [class props]
  (v/check! [:fn fn?] class :facet-class)
  (codec/native-value! props)
  (class #js {:props props}))

(defn get-facet
  "Returns a lazy native facet stub; startup executes only when a new facet starts."
  [registry name startup-callback]
  (v/check! :string (names/text name) :facet-name)
  (v/check! [:fn fn?] startup-callback :facet-startup)
  (n/invoke registry "get" [(names/text name) startup-callback]))

(defn abort!
  "Stops the named native facet synchronously while retaining its database; omission of a reason is preserved."
  ([registry name]
   (n/invoke registry "abort" [(v/check! :string (names/text name) :facet-name)]))
  ([registry name reason]
   (n/invoke registry "abort" [(v/check! :string (names/text name) :facet-name) reason])))

(defn delete!
  "Returns native deletion Promise; deletes the facet database and descendant databases."
  [registry name]
  (n/invoke registry "delete" [(v/check! :string (names/text name) :facet-name)]))
