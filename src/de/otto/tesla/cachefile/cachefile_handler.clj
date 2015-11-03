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
  (cleanup-generations [self])
  (read-cache-file [self filename read-fn])
  (slurp-cache-file [self filename])
  (write-cache-file [self filename lines])
  (write-success-file [self])
  (cache-file-exists [self filename]))

(defn- configured-toplevel-path [config which-data]
  (get-in config [:config (keyword (str which-data "-toplevel-path"))]))

(defn- configured-nr-generations-to-keep [config which-data]
  (get-in config [:config (keyword (str which-data "-nr-gens-to-keep"))]))

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

(defn build-file-path [self file-name read-or-write]
  (let [tl-path (some-> (build-toplevel-path self)
                        (hdfsgens/inject-hdfs-generation read-or-write))]
    (format "%s/%s" tl-path file-name)))

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

  CfAccess
  (write-cache-file [self filename lines]
    (let [file-path (build-file-path self filename :write)]
      (hdfs/make-parents file-path)
      (hdfs/write-lines file-path lines)))

  (write-success-file [self]
    (write-cache-file self "_SUCCESS" [""]))

  (slurp-cache-file [self filename]
    (read-cache-file self filename #(clojure.string/join \newline (line-seq %))))

  (read-cache-file [self filename read-fn]
    (with-open [rdr (hdfs/buffered-reader (build-file-path self filename :read))]
      (read-fn rdr)))

  (cleanup-generations [self]
    (if (hdfsgens/should-cleanup-generations nr-gens-to-keep toplevel-path)
      (hdfsgens/cleanup-generations (build-toplevel-path self) nr-gens-to-keep)))

  (cache-file-exists [self filename]
    (if-let [file-path (build-file-path self filename :read)]
      (hdfs/exists? file-path)
      false)))

(defn new-cachefile-handler
  ([which-data] (map->CacheFileHandler {:which-data which-data})))
