(ns de.otto.tesla.cachefile.file-historizer
  (:require [com.stuartsierra.component :as c]
            [overtone.at-at :as at]
            [metrics.counters :as counters]
            [de.otto.tesla.cachefile.strategy.historization :as hist]
            [de.otto.tesla.cachefile.utils.zk-namenode :as zknn]
            [clojure.tools.logging :as log]
            [de.otto.tesla.stateful.app-status :as apps]
            [clojure.core.async :as async]
            [de.otto.tesla.cachefile.utils.metrics :as util-metrics]
            [de.otto.tesla.cachefile.utils.reading-properties :as rpr]
            [de.otto.status :as s])
  (:import (java.io IOException)))

(defn writer-for-timestamp [{:keys [output-path writers zookeeper]} millis]
  (-> (zknn/with-zk-namenode zookeeper output-path)
      (hist/lookup-writer-or-create writers millis)))

(defn write-to-hdfs [{:keys [writers which-historizer] :as self} app-status {:keys [ts msg]}]
  (try
    (-> (writer-for-timestamp self ts)
        (hist/write-line! msg)
        (hist/touch-writer)
        (hist/store-writer writers))
    (counters/inc! (counters/counter ["file-historizer" which-historizer "write-to-hdfs"]))
    (catch IOException e
      (log/error e "Error occured when writing message: " msg " with ts: " ts)
      (apps/register-status-fun app-status (partial hist/writing-error-status-fn which-historizer msg e))))
  msg)

(defrecord FileHistorizer [app-status config which-historizer zookeeper in-channel transform-or-nil-fn]
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
      (apps/register-status-fun app-status (partial hist/historization-status-fn writers which-historizer))
      (async/pipeline 1 dev-null (comp
                                   (keep transform-or-nil-fn)
                                   (map (partial util-metrics/metered-execution
                                                 (str which-historizer "write-to-hdfs")
                                                 write-to-hdfs new-self app-status))) in-channel)
      new-self))

  (stop [{:keys [writers schedule pool] :as self}]
    (log/info "<- stopping FileHistorizer")
    (hist/close-writers! writers)
    (when schedule (at/kill schedule))
    (at/stop-and-reset-pool! pool)
    self))

(defn new-file-historizer
  ([which-historizer in-channel]
   (map->FileHistorizer {:which-historizer    which-historizer
                         :in-channel          in-channel
                         :transform-or-nil-fn identity}))
  ([which-historizer in-channel transform-or-nil-fn]
   (map->FileHistorizer {:which-historizer    which-historizer
                         :in-channel          in-channel
                         :transform-or-nil-fn transform-or-nil-fn})))
