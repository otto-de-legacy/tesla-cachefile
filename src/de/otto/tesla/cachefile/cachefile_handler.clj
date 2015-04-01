(ns de.otto.tesla.cachefile.cachefile-handler
  (:require
    [com.stuartsierra.component :as c]
    [clojure.java.io :as io]
    [clojure.tools.logging :as log]
    [de.otto.tesla.zk.zk-observer :as zk])
  (:import (org.apache.hadoop.fs FileSystem Path)
           (org.apache.hadoop.conf Configuration)))

(defn is-hdfs-file-path [path]
  (if (not (nil? path))
    (.startsWith path "hdfs://")
    false))

(defn get-hdfs-conf [namenode]
  (if (nil? namenode)
    (throw (IllegalStateException. "No hdfs namenode defined...")))
  (let [c (Configuration.)]
    (.set c "fs.defaultFS" namenode)
    c))

(defn build-file-system [namenode]
  (FileSystem/get (get-hdfs-conf namenode)))

(defn write-hdfs-file [namenode file-path content]
  (let [fs (build-file-system namenode)
        output-stream (.create fs (Path. file-path))]
    (spit output-stream content)))

(defn hdfs-file-exist [namenode path]
  (let [fs (build-file-system namenode)]
    (.exists fs (Path. path))))

(defn read-hdfs-file [namenode path]
  (let [fs (build-file-system namenode)
        input-stream (.open fs (Path. path))]
    (slurp input-stream)))

(defn without-hdfs-prefix [path]
  (if (not (nil? path))
    (.replaceAll path "hdfs://" "")))

(defn extract-url [zk-response]
  (if-not (nil? zk-response)
    (let [matcher #"^.*#([^\s]+)\s.*$"
          without-line-breaks (.replaceAll zk-response "\n" "")]
      (if-let [name (second (re-matches matcher without-line-breaks))]
        name
        zk-response))))

(defn namenode-resolution-fn [zk path]
  (log/info "choosing zookeeper to determine namenode. Zk-path: " path)
  (fn []
    (let [zk-response (zk/observe! zk path)]
      (extract-url zk-response))))

(defn property-resolution-fn [namenode]
  (log/info "choosing properties to determine namenode")
  (fn [] namenode))

(defn namenode-fn [zk config]
  (let [conf-namenode (:hdfs-namenode config)]
    (if (= "zookeeper" conf-namenode)
      (namenode-resolution-fn zk "/hadoop-ha/hadoop-ha/ActiveBreadCrumb")
      (property-resolution-fn conf-namenode))))

(defprotocol CfAccess
  (read-cache-file [self])
  (write-cache-file [self content])
  (cache-file-exists [self])
  (cache-file-defined [self]))


(defrecord CacheFileHandler [zookeeper config]
  c/Lifecycle
  (start [self]
    (log/info "-> starting cache-file-handler")
    (let [cache-file (get-in config [:config :cache-file])
          is-hdfs-cache-file (is-hdfs-file-path cache-file)
          namenode-fn (if is-hdfs-cache-file
                        (namenode-fn zookeeper (:config config)))]
      (if-not (nil? namenode-fn)
        (log/info "Current Namenode is \"" (namenode-fn) "\""))
      (assoc self :name-node-fn namenode-fn
                  :is-hdfs-cache-file is-hdfs-cache-file
                  :cache-file (without-hdfs-prefix cache-file))))
  (stop [self]
    (log/info "<- stopping cache-file-handler")
    self)

  CfAccess
  (write-cache-file [self content]
    (if (:is-hdfs-cache-file self)
      (write-hdfs-file ((:name-node-fn self)) (:cache-file self) content)
      (spit (:cache-file self) content)))

  (read-cache-file [self]
    (if (:is-hdfs-cache-file self)
      (read-hdfs-file ((:name-node-fn self)) (:cache-file self))
      (slurp (:cache-file self))))

  (cache-file-exists [self]
    (if (nil? (:cache-file self))
      false
      (if (:is-hdfs-cache-file self)
        (hdfs-file-exist ((:name-node-fn self)) (:cache-file self))
        (.exists (io/file (:cache-file self))))))

  (cache-file-defined [self]
    (not (nil? (:cache-file self)))))

(defn new-cachefile-handler []
  (map->CacheFileHandler {}))
