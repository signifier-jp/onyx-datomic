(ns onyx.datomic.protocols)

(defprotocol DatomicHelpers
  (cas-key [this] "returns `db.fn/cas` in peer API, and `db/cas` in client API.")
  (create-database [this datomic-config] "Create a db.")
  (delete-database [this datomic-config] "Delete a db.")
  (instance-of-datomic-function? [this v] "Checks if the value is an instance of datomic.function.Function.")
  (next-t [this db] "Return next-t.")
  (safe-connect [this datomic-config] "Return datomic connection.")
  (safe-as-of [this datomic-config conn] "Returns the value of the database as of some time-point.")
  (transact [this conn data] "datomic transact")
  (tx-range [this conn start-tx] "Get transaction since start-tx"))

(defprotocol DatomicFns
  (as-of [this] "datomic as-of fn")
  (db [this] "datomic db fn")
  (datoms [this] "datomic datoms fn")
  (entity [this] "datomic entity fn")
  (ident [this] "datomic ident fn")
  (index-range [this] "datomic index-range fn")
  (q [this] "datomic q fn")
  (tempid [this] "datomic tempid fn")
  (transact-async [this] "datomic transact-async fn"))
