(ns onyx.datomic.client
  (:require [clojure.string :as str]
            [datomic.client.api :as d]
            [onyx.datomic.protocols :as dp]
            [taoensso.timbre :as log])
  (:import [java.net URI]))

(defn- _cloud-client [{:keys [datomic-cloud/system
                              datomic-cloud/region
                              datomic-cloud/query-group
                              datomic-cloud/endpoint
                              datomic-cloud/proxy-port] :as task-map}]
  (let [region (or region (System/getenv "DATOMIC_CLOUD_REGION"))
        system (or system (System/getenv "DATOMIC_CLOUD_SYSTEM"))
        proxy-port (or proxy-port (System/getenv "DATOMIC_CLOUD_PROXY_PORT"))
        proxy-port (when (string? proxy-port) (Integer/parseInt proxy-port))]
    (d/client  {:server-type :cloud
                :region region
                :system system
                :query-group (or query-group (System/getenv "DATOMIC_CLOUD_QUERY_GROUP") system)
                :endpoint (or endpoint (format "http://entry.%s.%s.datomic.net:8182/" system region))
                :proxy-port (or proxy-port 8182)})))

(defn- _peer-server-client [{:keys [datomic-client/access-key
                                    datomic-client/secret
                                    datomic-client/endpoint] :as task-map}]
  (let [access-key (or access-key (System/getenv "DATOMIC_CLIENT_ACCESS_KEY"))
        secret (or secret (System/getenv "DATOMIC_CLIENT_SECRET"))
        endpoint (or endpoint (System/getenv "DATOMIC_CLIENT_ENDPOINT"))]
    (d/client  {:server-type :peer-server
                :access-key access-key
                :secret secret
                :endpoint endpoint})))

(def cloud-client (memoize _cloud-client))
(def peer-server-client (memoize _peer-server-client))

(defn- safe-connect-cloud [{:keys [datomic-cloud/db-name] :as task-map}]
  (when (nil? db-name)
    (throw (ex-info "either :datomic-cloud/db-name is required to connect." task-map)))
  (d/connect (cloud-client task-map) {:db-name db-name}))

(defn- safe-connect-client [{:keys [datomic-client/db-name] :as task-map}]
  (when (nil? db-name)
    (throw (ex-info ":datomic-client/db-name is required to connect." task-map)))
  (log/info "Connecting to database " db-name)
  (d/connect (peer-server-client task-map) {:db-name db-name}))

(def not-implemented-yet #(throw (ex-info "not implmented yet" {})))

(def _ident #(-> (d/pull % '[:db/ident] %2) :db/ident))

(defrecord DatomicClient [lib-type]
  dp/DatomicHelpers
  (cas-key [_] :db/cas)
  (create-database [_ task-map]
    (if (= :cloud lib-type)
      (d/create-database (cloud-client task-map) {:db-name (:datomic-cloud/db-name task-map)})
      (throw (ex-info "Datomic client for On-Prem doesn't support create-database. Use peer API." task-map))))
  (delete-database [_ task-map]
    (if (= :cloud lib-type)
      (d/delete-database (cloud-client task-map) {:db-name (:datomic-cloud/db-name task-map)})
      (throw (ex-info "Datomic client for On-Prem doesn't support delete-database. Use peer API." task-map))))
  (instance-of-datomic-function? [this v] false)
  (next-t [_ db]
    (:next-t db))
  (safe-connect [_ task-map]
    (if (= :cloud lib-type)
      (safe-connect-cloud task-map)
      (safe-connect-client task-map)))
  (safe-as-of [_ task-map conn]
    (if-let [t (:datomic/t task-map)]
      (d/as-of (d/db conn) t)
      (throw (ex-info ":datomic/t missing from write-datoms task-map." task-map))))
  (transact [this conn data]
    (d/transact conn {:tx-data data}))
  (tx-range [this conn start-tx]
    (d/tx-range conn {:start start-tx :end nil})) dp/DatomicFns
  (as-of [_] d/as-of)
  (datoms [_] (fn [db index & components]
                (let [arg-map {:index index}
                      arg-map (when-not (empty? components)
                                (assoc arg-map :components components))]
                  (d/datoms db arg-map))))
  (db [_] d/db)
  (entity [_] #(d/pull % '[*] %2))
  (ident [_] _ident)
  (index-range [_] d/index-range)
  (q [_] d/q)
  (tempid [_] str)
  (transact-async [_] d/transact))

(defn new-datomic-impl [lib-type]
  (->DatomicClient lib-type))
