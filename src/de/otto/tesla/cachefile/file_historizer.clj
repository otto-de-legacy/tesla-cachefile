(ns de.otto.tesla.cachefile.file-historizer
  (:require [com.stuartsierra.component :as c]
            [overtone.at-at :as at]
            [de.otto.tesla.cachefile.strategy.historization :as hist]
            [de.otto.tesla.cachefile.utils.zk-namenode :as zknn]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [de.otto.tesla.cachefile.utils.reading-properties :as rpr]
            )
  (:import (java.io PrintWriter)))

(defprotocol HistorizationHandling
  (writer-for-timestamp [self timestamp] "Returns a PrintWriter-instance for the given timestamp (see historization strategy)")
  (write-to-hdfs [self msg-map] "Writes to the HDFS, expects a map with timestamp and message {:ts :msg}")
  )

(defrecord FileHistorizer [config which-historizer zookeeper in-channel filter-fn]
  c/Lifecycle
  (start [self]
    (log/info "-> starting FileHistorizer")
    (let [output-path (rpr/configured-toplevel-path config which-historizer)
          executor (at/mk-pool)
          max-writer-age (rpr/configured-max-writer-age config which-historizer)
          schedule-closing-time (rpr/configured-schedule-closing-time config which-historizer)
          dev-null (async/chan (async/dropping-buffer 1))
          new-self (assoc self
                     :executor executor
                     :output-path output-path
                     :writers (atom {}))]
      (assoc new-self
        :schedule (at/every schedule-closing-time
                            #(hist/close-old-writers! (:writers new-self) max-writer-age)
                            executor))
      (async/pipeline 1 dev-null (keep (partial write-to-hdfs new-self (keep (partial filter-fn)))) in-channel)
      ))

  (stop [{:keys [writers schedule executor] :as self}]
    (log/info "<- stopping HdfsWriter")
    (hist/close-writers! writers)
    (when schedule
      (at/kill schedule))
    (at/stop-and-reset-pool! executor)
    self)

  HistorizationHandling
  (writer-for-timestamp [{:keys [output-path writers]} millis]
    (-> (zknn/with-zk-namenode zookeeper output-path)
        (hist/lookup-writer-or-create! writers millis)
        :writer))
  (write-to-hdfs [self {:keys [ts msg]}]
    (let [^PrintWriter writer (writer-for-timestamp self ts)]
      (.println writer msg))
    msg))

(defn new-file-historizer
  ([which-historizer in-channel]
  (map->FileHistorizer {:which-historizer which-historizer
                        :in-channel in-channel
                        :filter-fn identity}))
  ([which-historizer in-channel filter-fn]
   (map->FileHistorizer {:which-historizer which-historizer
                         :in-channel in-channel
                         :filter-fn filter-fn})))
