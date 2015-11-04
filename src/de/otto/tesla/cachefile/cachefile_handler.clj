(ns de.otto.tesla.cachefile.cachefile-handler
  (:require
    [com.stuartsierra.component :as c]
    [hdfs.core :as hdfs]
    [de.otto.tesla.cachefile.hdfs-helpers :as helpers]
    [clojure.tools.logging :as log]
    [de.otto.tesla.cachefile.hdfs-generations :as hdfsgens]
    [de.otto.tesla.zk.zk-observer :as zk])
  (:import (org.apache.hadoop.hdfs.server.namenode.ha.proto HAZKInfoProtos$ActiveNodeInfo)))

(def ZK_NAMENODE_PLACEHOLDER "{ZK_NAMENODE}")

(defprotocol GenerationHandling
  (folder-to-write-to [self] "Creates new generation directory and returns the path.")
  (folder-to-read-from [self] "Finds newest generation wit a success file and returns the path.")
  (write-success-file [self path] "Creates a file named _SUCCESS in the given , which is a marker for the other functions of this protocol")
  (cleanup-generations [self] "Determines n last successful generations and deletes any older generation."))

(defn- configured-toplevel-path [config which-data]
  (get-in config [:config (keyword (str which-data "-toplevel-path"))]))

(defn- configured-nr-generations-to-keep [config which-data]
  (if-let [gens-to-keep (get-in config [:config (keyword (str which-data "-nr-gens-to-keep"))])]
    (Integer/parseInt gens-to-keep)))

(defn- parse-hostname [zk-response]
  (when-not (nil? zk-response)
    (.getHostname (HAZKInfoProtos$ActiveNodeInfo/parseFrom zk-response))))

(defn- namenode-from-zookeeper [zk]
  (let [path "/hadoop-ha/hadoop-ha/ActiveBreadCrumb"]
    (log/info "choosing zookeeper to determine namenode. Zk-path: " path)
    (zk/observe! zk path parse-hostname)))

(defn- replace-namenode-placholder [path zookeeper]
  (let [current-namenode (namenode-from-zookeeper zookeeper)]
    (clojure.string/replace path ZK_NAMENODE_PLACEHOLDER current-namenode)))

(defn- inject-current-namenode [path zookeeper]
  (if (.contains path ZK_NAMENODE_PLACEHOLDER)
    (replace-namenode-placholder path zookeeper)
    path))

(defn- build-toplevel-path [{:keys [zookeeper toplevel-path]}]
  (some-> toplevel-path
          (inject-current-namenode zookeeper)))

(defn build-folder-path [self read-or-write]
  (some-> (build-toplevel-path self)
          (hdfsgens/inject-hdfs-generation read-or-write)))

(defrecord CacheFileHandler [which-data zookeeper config toplevel-path nr-gens-to-keep]
  c/Lifecycle
  (start [self]
    (log/info "-> starting " which-data "-cache-file-handler")
    (assoc self
      :nr-gens-to-keep (configured-nr-generations-to-keep config which-data)
      :toplevel-path (configured-toplevel-path config which-data)))

  (stop [self]
    (log/info "<- stopping " which-data "-cache-file-handler")
    self)

  GenerationHandling
  (folder-to-write-to [self]
    (build-folder-path self :write))

  (folder-to-read-from [self]
    (build-folder-path self :read))

  (write-success-file [_ path]
    (helpers/write-file (str path "/_SUCCESS") [""]))

  (cleanup-generations [self]
    (if (hdfsgens/should-cleanup-generations nr-gens-to-keep toplevel-path)
      (hdfsgens/cleanup-generations (build-toplevel-path self) nr-gens-to-keep))))

(defn new-cachefile-handler
  ([which-data] (map->CacheFileHandler {:which-data which-data})))
