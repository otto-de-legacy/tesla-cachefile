(ns de.otto.tesla.cachefile.cachefile-handler
  (:require
    [com.stuartsierra.component :as c]
    [de.otto.tesla.cachefile.utils.hdfs-helpers :as helpers]
    [clojure.tools.logging :as log]
    [de.otto.tesla.cachefile.utils.zk-namenode :as nn]
    [de.otto.tesla.cachefile.utils.reading-properties :as rpr]
    [de.otto.tesla.cachefile.strategy.generations :as gens]))

(defprotocol GenerationHandling
  (folder-to-write-to [self] "Creates new generation directory and returns the path.")
  (folder-to-read-from [self] "Finds newest generation wit a success file and returns the path.")
  (write-success-file [self path] "Creates a file named _SUCCESS in the given , which is a marker for the other functions of this protocol")
  (cleanup-generations [self] "Determines n last successful generations and deletes any older generation."))

(defn build-folder-path [{:keys [zookeeper toplevel-path]} read-or-write]
  (some-> (nn/with-zk-namenode zookeeper toplevel-path)
          (gens/with-hdfs-generation read-or-write)))

(defrecord CacheFileHandler [which-data zookeeper config toplevel-path nr-gens-to-keep]
  c/Lifecycle
  (start [self]
    (log/info "-> starting " which-data "-cache-file-handler")
    (assoc self
      :nr-gens-to-keep (rpr/configured-nr-generations-to-keep config which-data)
      :toplevel-path (rpr/configured-toplevel-path config which-data)))

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

  (cleanup-generations [_]
    (when (gens/should-cleanup-generations? nr-gens-to-keep toplevel-path)
      (gens/cleanup-generations! (nn/with-zk-namenode zookeeper toplevel-path) nr-gens-to-keep))))

(defn new-cachefile-handler
  ([which-data] (map->CacheFileHandler {:which-data which-data})))
