(ns de.otto.tesla.cachefile.cachefile-handler
  (:require
    [com.stuartsierra.component :as c]
    [hdfs.core :as hdfs]
    [clojure.tools.logging :as log]
    [de.otto.tesla.zk.zk-observer :as zk])
  (:import (org.apache.hadoop.hdfs.server.namenode.ha.proto HAZKInfoProtos$ActiveNodeInfo)))

(def ZK_NAMENODE_PLACEHOLDER "{ZK_NAMENODE}")
(def LATEST_GENERATION "{LATEST_GENERATION}")

(defprotocol CfAccess
  (read-cache-file [self])
  (write-cache-file [self content])
  (cache-file-exists [self])
  (cache-file-defined [self]))

(defn- get-config-key [file-type]
  (if (= file-type "") :cache-file (keyword (str "cache-file-" file-type))))

(defn- parse-hostname [zk-response]
  (when-not (nil? zk-response)
    (.getHostname (HAZKInfoProtos$ActiveNodeInfo/parseFrom zk-response))))

(defn- namenode-from-zookeeper [zk]
  (let [path "/hadoop-ha/hadoop-ha/ActiveBreadCrumb"]
    (log/info "choosing zookeeper to determine namenode. Zk-path: " path)
    (zk/observe! zk path parse-hostname)))

(defn- replace-namenode-placholder [cache-file zookeeper]
  (let [current-namenode (namenode-from-zookeeper zookeeper)]
    (clojure.string/replace cache-file ZK_NAMENODE_PLACEHOLDER current-namenode)))

(defn- replace-latest-generation-placholder [cache-file]
  ;to be implemented
  cache-file)

(defn- inject-current-namenode [cache-file zookeeper]
  (if (.contains cache-file ZK_NAMENODE_PLACEHOLDER)
    (replace-namenode-placholder cache-file zookeeper)
    cache-file))

(defn- inject-latest-hdfs-generation [cache-file]
  (if (.contains cache-file LATEST_GENERATION)
    (replace-latest-generation-placholder cache-file)
    cache-file))

(defn current-cache-file [{:keys [zookeeper cache-file]}]
  (some-> cache-file
          (inject-current-namenode zookeeper)
          (inject-latest-hdfs-generation)))

(defrecord CacheFileHandler [file-type zookeeper config current-cache-file-fn cache-file]
  c/Lifecycle
  (start [self]
    (log/info "-> starting cache-file-handler")
    (let [new-self (assoc self :cache-file (get-in config [:config (get-config-key file-type)]))]
      (assoc new-self :current-cache-file-fn (partial current-cache-file new-self))))
  (stop [self]
    (log/info "<- stopping cache-file-handler")
    self)

  CfAccess
  (write-cache-file [_ content]
    (let [lines (if (coll? content)
                  content
                  [content])]
      (hdfs/write-lines (current-cache-file-fn) lines)))

  (read-cache-file [_]
    (with-open [rdr (hdfs/buffered-reader (current-cache-file-fn))]
      (clojure.string/join \newline (line-seq rdr))))

  (cache-file-exists [_]
    (if-let [file-path (current-cache-file-fn)]
      (hdfs/exists? file-path)
      false))

  (cache-file-defined [_]
    (not (nil? cache-file))))

(defn new-cachefile-handler
  ([] (map->CacheFileHandler {:file-type ""}))
  ([file-type] (map->CacheFileHandler {:file-type file-type})))
