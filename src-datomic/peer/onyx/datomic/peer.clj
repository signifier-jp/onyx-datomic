(ns onyx.datomic.peer
  (:require [onyx.datomic.protocols :as dp]
            [datomic.api :as d]))

(deftype DatomicPeer []
  dp/DatomicHelpers
  (safe-connect [_ task-map]
    (if-let [uri (:datomic/uri task-map)]
      (d/connect uri)
      (throw (ex-info ":datomic/uri missing from write-datoms task-map." task-map))))
  (safe-as-of [_ task-map conn]
    (if-let [t (:datomic/t task-map)]
      (d/as-of (d/db conn) t)
      (throw (ex-info ":datomic/t missing from write-datoms task-map." task-map))))
(instance-of-datomic-function? [this v]
  (instance? datomic.function.Function v)
  )

  dp/DatomicFns
  (as-of [_] d/as-of)
  (connect [_] d/connect)
  (create-database [_] d/create-database)
  (datoms [_] d/datoms)
  (db [_] d/db)
  (delete-database [_] d/delete-database)
  (entity [_] d/entity)
  (ident [_] d/ident)
  (index-range [_] d/index-range)
  (log [_] d/log)
  (next-t [_] d/next-t)
  (q [_] d/q)
  (tempid [_] d/tempid)
  (transact [_] d/transact)
  (transact-async [_] d/transact-async)
  (tx-range [_] d/tx-range))

(defn new-datomic-impl []
  (->DatomicPeer))
