(ns onyx.datomic.peer
  (:require [clojure.string :as str]
            [onyx.datomic.protocols :as dp]
            [datomic.api :as d]))

(defrecord DatomicPeer [lib-type]
  dp/DatomicHelpers
  (cas-key [_] :db.fn/cas)
  (create-database [_ {:keys [datomic/uri] :as task-map}]
    (d/create-database uri))
  (delete-database [_ {:keys [datomic/uri] :as task-map}]
    (d/delete-database uri))
  (instance-of-datomic-function? [this v]
    (instance? datomic.function.Function v))
  (next-t [_ db]
    (d/next-t db))
  (safe-connect [_ task-map]
    (if-let [uri (:datomic/uri task-map)]
      (d/connect uri)
      (throw (ex-info ":datomic/uri missing from write-datoms task-map." task-map))))
  (safe-as-of [_ task-map conn]
    (if-let [t (:datomic/t task-map)]
      (d/as-of (d/db conn) t)
      (throw (ex-info ":datomic/t missing from write-datoms task-map." task-map))))
  (transact [_ conn data]
    @(d/transact conn data))
  (tx-range [_ conn start-tx]
    (let [log (d/log conn)]
      (d/tx-range log start-tx nil)))

  dp/DatomicFns
  (as-of [_] d/as-of)
  (datoms [_] d/datoms)
  (db [_] d/db)
  (entity [_] d/entity)
  (ident [_] d/ident)
  (index-range [_] d/index-range)
  (q [_] d/q)
  (tempid [_] d/tempid)
  (transact-async [_] d/transact-async))

(defn new-datomic-impl [lib-type]
  (->DatomicPeer lib-type))
