(ns de.otto.tesla.cachefile.cachefile-handler
  (:require
    [com.stuartsierra.component :as c]
    [hdfs.core :as hdfs]
    [clojure.tools.logging :as log]
    [de.otto.tesla.cachefile.hdfs-generations :as hdfsgens]
    [de.otto.tesla.zk.zk-observer :as zk])
  (:import (org.apache.hadoop.hdfs.server.namenode.ha.proto HAZKInfoProtos$ActiveNodeInfo)))

(def ZK_NAMENODE_PLACEHOLDER "{ZK_NAMENODE}")

(defprotocol CfAccess
  (read-cache-file [self filename read-fn])
  (slurp-cache-file [self filename])
  (write-cache-file [self filename lines])
  (write-success-file [self])
  (cache-file-exists [self filename]))

(defn- configured-toplevel-path [config which-data]
  (get-in config [:config (keyword (str which-data "-toplevel-path"))]))

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

(defn build-file-path [{:keys [zookeeper toplevel-path]} file-name read-or-write]
  (let [tl-path (some-> toplevel-path
                        (inject-current-namenode zookeeper)
                        (hdfsgens/inject-hdfs-generation read-or-write))]
    (format "%s/%s" tl-path file-name)))

(defrecord CacheFileHandler [which-data zookeeper config file-path-fn toplevel-path]
  c/Lifecycle
  (start [self]
    (log/info "-> starting cache-file-handler")
    (let [new-self (assoc self :toplevel-path (configured-toplevel-path config which-data))]
      (assoc new-self :file-path-fn (partial build-file-path new-self))))

  (stop [self]
    (log/info "<- stopping cache-file-handler")
    self)

  CfAccess
  (write-cache-file [_ filename lines]
    (let [file-path (file-path-fn filename :write)]
      (hdfs/make-parents file-path)
      (hdfs/write-lines file-path lines)))

  (write-success-file [self]
    (write-cache-file self "_SUCCESS" [""]))

  (slurp-cache-file [self filename]
    (read-cache-file self filename #(clojure.string/join \newline (line-seq %))))

  (read-cache-file [_ filename read-fn]
    (with-open [rdr (hdfs/buffered-reader (file-path-fn filename :read))]
      (read-fn rdr)))

  (cache-file-exists [_ filename]
    (if-let [file-path (file-path-fn filename :read)]
      (hdfs/exists? file-path)
      false)))

(defn new-cachefile-handler
  ([which-data] (map->CacheFileHandler {:which-data which-data})))
