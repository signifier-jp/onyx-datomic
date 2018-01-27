(ns onyx.datomic.cloud
  (:require [clojure.string :as str]
            [onyx.datomic.protocols :as dp]
            [datomic.client.api :as d])
  (:import [java.net URI]))

(comment
 ;; TODO delete this before commit!
  (def task-map {:datomic-client/system "signifier-dev"
                 :datomic-client/region "us-west-2"
                 :datomic-client/query-group "signifier-dev"
                 :datomic-client/proxy-port 8182
                 :datomic-client/db-name "test1"}))

(defn- _cloud-client [{:keys [datomic-client/system
                              datomic-client/region
                              datomic-client/query-group
                              datomic-client/endpoint
                              datomic-client/proxy-port] :as task-map}]
  (let [region (or region (System/getenv "DATOMIC_REGION"))
        system (or system (System/getenv "DATOMIC_SYSTEM"))
        proxy-port (or proxy-port (System/getenv "DATOMIC_PROXY_PORT"))]
    (d/client  {:server-type :cloud
                :region region
                :system system
                :query-group (or query-group (System/getenv "DATOMIC_QUERY_GROUP") system)
                :endpoint (or endpoint (format "http://entry.%s.%s.datomic.net:8182/" system region))
                :proxy-port (or proxy-port 8182)})))

(def cloud-client (memoize _cloud-client))

(defn- db-name-in-uri [uri]
  (-> uri
      URI.
      .getSchemeSpecificPart
      (str/split #"//")
      last))

(defn- safe-connect-cloud [{:keys [datomic-client/db-name datomic/uri] :as task-map}]
  (when (and (nil? db-name) (nil? uri))
    (throw (ex-info "either :datomic-client/db-name or :datomic-uri is required to connect.")))
  (d/connect (cloud-client task-map) {:db-name (or db-name (db-name-in-uri uri))}))

(def _create-database
  #(d/create-database (cloud-client {}) {:db-name (db-name-in-uri %)}))

(def not-implemented-yet #(throw (ex-info "not implmented yet" {})))

(deftype DatomicCloud []
  dp/DatomicHelpers
  (safe-connect [_ {:keys [datomic-client/db-name] :as task-map}]
    (when-not db-name (throw (ex-info ":datomic-client/db-name missing from task-map." task-map)))
    (d/connect (cloud-client task-map) {:db-name db-name}))
  (safe-as-of [_ task-map conn]
    (if-let [t (:datomic/t task-map)]
      (d/as-of (d/db conn) t)
      (throw (ex-info ":datomic/t missing from write-datoms task-map." task-map))))
  (instance-of-datomic-function? [this v] false)

  dp/DatomicFns
  (as-of [_] d/as-of)
  (connect [_] d/connect)
  (create-database [_] _create-database)
  (datoms [_] d/datoms)
  (db [_] d/db)
  (delete-database [_] d/delete-database)
  (entity [_] not-implemented-yet)
  (ident [_] not-implemented-yet)
  (index-range [_] d/index-range)
  (log [_] not-implemented-yet)
  (next-t [_] not-implemented-yet)
  (q [_] d/q)
  (tempid [_] not-implemented-yet)
  (transact [_] d/transact)
  (transact-async [_] (future d/transact))
  (tx-range [_] d/tx-range))

(defn new-datomic-impl []
  (->DatomicCloud))
