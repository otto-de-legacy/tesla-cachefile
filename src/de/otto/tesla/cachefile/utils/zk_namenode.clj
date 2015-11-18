(ns de.otto.tesla.cachefile.utils.zk-namenode
  (:require [de.otto.tesla.zk.zk-observer :as zk]
            [clojure.tools.logging :as log])
  (:import (org.apache.hadoop.hdfs.server.namenode.ha.proto HAZKInfoProtos$ActiveNodeInfo)))

(def ZK_NAMENODE_PLACEHOLDER "{ZK_NAMENODE}")

(defn- parse-hostname [zk-response]
  (when-not (nil? zk-response)
    (.getHostname (HAZKInfoProtos$ActiveNodeInfo/parseFrom zk-response))))

(defn- namenode-from-zookeeper [zk]
  (let [path "/hadoop-ha/hadoop-ha/ActiveBreadCrumb"]
    (log/debug "choosing zookeeper to determine namenode. Zk-path: " path)
    (zk/observe! zk path parse-hostname)))

(defn- replace-namenode-placholder [path zookeeper]
  (let [current-namenode (namenode-from-zookeeper zookeeper)]
    (clojure.string/replace path ZK_NAMENODE_PLACEHOLDER current-namenode)))

(defn with-zk-namenode [zookeeper path]
  (if (.contains path ZK_NAMENODE_PLACEHOLDER)
    (replace-namenode-placholder path zookeeper)
    path))
