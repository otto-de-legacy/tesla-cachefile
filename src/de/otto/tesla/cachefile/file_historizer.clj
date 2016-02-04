(ns de.otto.tesla.cachefile.file-historizer
  (:require [com.stuartsierra.component :as c]
            [overtone.at-at :as at]
            [de.otto.tesla.cachefile.strategy.historization :as hist]
            [de.otto.tesla.cachefile.utils.zk-namenode :as zknn]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [de.otto.tesla.cachefile.utils.reading-properties :as rpr]
            )
  (:import (java.io BufferedWriter)))

(defprotocol HistorizationHandling
  (writer-for-timestamp [self timestamp] "Returns a BufferedWriter-instance for the given timestamp (see historization strategy)")
  (write-to-hdfs [self msg-map] "Writes to the HDFS, expects a map with timestamp and message {:ts :msg}"))

(defrecord FileHistorizer [config which-historizer zookeeper in-channel transform-or-nil-fn]
  c/Lifecycle
  (start [self]
    (log/info "-> starting FileHistorizer " which-historizer)
    (let [output-path (rpr/toplevel-path config which-historizer)
          pool (at/mk-pool)
          max-age (rpr/max-age config which-historizer)
          close-interval (rpr/close-interval config which-historizer)
          dev-null (async/chan (async/dropping-buffer 1))
          writers (atom {})
          new-self (assoc self
                     :pool pool
                     :output-path output-path
                     :writers writers
                     :scheduler (at/every close-interval
                                          #(hist/close-old-writers! writers max-age)
                                          pool))]
      (async/pipeline 1 dev-null (comp
                                   (keep transform-or-nil-fn)
                                   (map (partial write-to-hdfs new-self))) in-channel)
      new-self))

  (stop [{:keys [writers schedule pool] :as self}]
    (log/info "<- stopping FileHistorizer")
    (hist/close-writers! writers)
    (when schedule (at/kill schedule))
    (at/stop-and-reset-pool! pool)
    self)

  HistorizationHandling
  (writer-for-timestamp [{:keys [output-path writers]} millis]
    (-> (zknn/with-zk-namenode zookeeper output-path)
        (hist/lookup-writer-or-create! writers millis)
        :writer))
  (write-to-hdfs [self {:keys [ts msg]}]
    (let [^BufferedWriter writer (writer-for-timestamp self ts)]
      (.write writer msg)
      (.newLine writer))
    msg))

(defn new-file-historizer
  ([which-historizer in-channel]
   (map->FileHistorizer {:which-historizer    which-historizer
                         :in-channel          in-channel
                         :transform-or-nil-fn identity}))
  ([which-historizer in-channel transform-or-nil-fn]
   (map->FileHistorizer {:which-historizer    which-historizer
                         :in-channel          in-channel
                         :transform-or-nil-fn transform-or-nil-fn})))
