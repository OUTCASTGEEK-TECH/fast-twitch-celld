(ns fast-twitch.celld.services.facets
  "Native facets retain root ownership and independent storage. Parent transactions
  cannot roll back facet effects. Facet sockets keep the root resident, without hibernation."
  (:require [malli.experimental :as mx]
            [fast-twitch.celld.native :as n]
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

(mx/defn ^:dynamic specialize-class
  "Copies native startup props through the native loopback class specialization."
  [class :- [:fn fn?] props]
  (codec/native-value! props)
  (class #js {:props props}))

(mx/defn ^:dynamic get-facet
  "Returns a lazy native facet stub; startup executes only when a new facet starts."
  [registry name :- [:or :keyword :string] startup-callback :- [:fn fn?]]
  (n/invoke registry "get" [(names/text name) startup-callback]))

(mx/defn ^:dynamic abort!
  "Stops the named native facet synchronously while retaining its database; omission of a reason is preserved."
  ([registry name :- [:or :keyword :string]]
   (n/invoke registry "abort" [(names/text name)]))
  ([registry name :- [:or :keyword :string] reason]
   (n/invoke registry "abort" [(names/text name) reason])))

(mx/defn ^:dynamic delete!
  "Returns native deletion Promise; deletes the facet database and descendant databases."
  [registry name :- [:or :keyword :string]]
  (n/invoke registry "delete" [(names/text name)]))

(v/instrument! specialize-class get-facet abort! delete!)
