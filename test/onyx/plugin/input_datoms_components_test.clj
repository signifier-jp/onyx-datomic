(ns onyx.plugin.input-datoms-components-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is]]
            [onyx.datomic.api :as d]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [read-datoms]]
             [core-async :as core-async]]))

(defn build-job [datomic-config t batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-datoms :persist]]
                         :catalog []
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (read-datoms :read-datoms
                               (merge {:datomic/t t
                                       :datomic/datoms-index :avet
                                       :datomic/datoms-per-segment 20
                                       :datomic/datoms-components [:user/name "Mike"]
                                       :onyx/max-peers 1}
                                      datomic-config
                                      batch-settings)))
        (add-task (core-async/output :persist batch-settings)))))

(defn ensure-datomic!
  ([task-map data]
   (d/create-database task-map)
   (d/transact
    (d/connect task-map)
    data)))

(def schema
  [{:db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/ident :user/name
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def people
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Derek"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Kristen"}])

(defn type-specific-schema []
  (if (= :cloud (d/datomic-lib-type))
    (mapv #(dissoc % :db.install/_partition) schema)
    schema))

(defn type-specific-people []
  (mapv #(dissoc % :db/id) people))

(deftest datomic-datoms-components-test
  (let [{:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        datomic-config (:datomic-config (read-config
                                         (clojure.java.io/resource "config.edn")
                                         {:profile (d/datomic-lib-type)}))
        db-name (str (java.util.UUID/randomUUID))
        db-uri (str (:datomic/uri datomic-config) db-name)
        datomic-config (assoc datomic-config
                              :datomic/uri db-uri
                              :datomic-cloud/db-name db-name)
        datomic-config (if (string? (:datomic-cloud/proxy-port datomic-config))
                         (assoc datomic-config
                                :datomic-cloud/proxy-port (Integer/parseInt
                                                           (:datomic-cloud/proxy-port datomic-config)))
                         datomic-config)
        _ (mapv (partial ensure-datomic! datomic-config) [[] (type-specific-schema) (type-specific-people)])
        t (d/next-t (d/db (d/connect datomic-config)))
        job (build-job datomic-config t 10 1000)
        {:keys [persist]} (get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> job
             (onyx.api/submit-job peer-config)
             :job-id
             (onyx.test-helper/feedback-exception! peer-config))
        (is (= #{"Mike"}
               (set (map #(nth % 2) (mapcat :datoms (take-segments! persist 50)))))))
      (finally (d/delete-database datomic-config)))))
