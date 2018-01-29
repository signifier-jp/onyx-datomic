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
        proxy-port (or proxy-port (System/getenv "DATOMIC_CLOUD_PROXY_PORT"))]
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

(defn- db-name-in-uri [uri]
  (-> uri
      URI.
      .getSchemeSpecificPart
      (str/split #"/")
      last))

(def cloud-client (memoize _cloud-client))
(def peer-server-client (memoize _peer-server-client))

(defn- safe-connect-cloud [{:keys [datomic-cloud/db-name] :as task-map}]
  (when (nil? db-name)
    (throw (ex-info "either :datomic-cloud/db-name is required to connect.")))
  (d/connect (cloud-client task-map) {:db-name db-name}))

(defn- safe-connect-client [{:keys [datomic-client/db-name datomic/uri] :as task-map}]
  (when (and (nil? db-name) (nil? uri))
    (throw (ex-info "either :datomic-client/db-name or :datomic-uri is required to connect.")))
  (log/info "Connecting to database " db-name " or " uri)
  (d/connect (peer-server-client task-map) {:db-name (or db-name (db-name-in-uri uri))}))

(def not-implemented-yet #(throw (ex-info "not implmented yet" {})))

(def _ident #(-> (d/pull % '[:db/ident] %2) :db/ident))

(deftype DatomicCloud []
  dp/DatomicHelpers
  (safe-connect [_ {:keys [datomic-cloud/db-name] :as task-map}]
    (log/spy task-map)
    (if db-name
      (d/connect (cloud-client task-map) task-map)
      (d/connect (client-client task-map) task-map)))
  (safe-as-of [_ task-map conn]
    (if-let [t (:datomic/t task-map)]
      (d/as-of (d/db conn) t)
      (throw (ex-info ":datomic/t missing from write-datoms task-map." task-map))))
  (instance-of-datomic-function? [this v] false)
  (tx-range [this conn start-tx]
    (d/tx-range conn {:start start-tx :end nil}))

  dp/DatomicFns
  (as-of [_] d/as-of)
  (datoms [_] d/datoms)
  (db [_] d/db)
  (ident [_] _ident)
  (index-range [_] d/index-range)
  (tempid [_] str)
  (transact [_] d/transact)
  (transact-async [_] (future d/transact)))

(defn new-datomic-impl []
  (->DatomicCloud))
