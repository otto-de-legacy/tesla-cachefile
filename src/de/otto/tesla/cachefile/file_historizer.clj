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
            [de.otto.tesla.stateful.scheduler :as scheduler])
  (:import (java.io IOException)))

(defn writer-for-timestamp [{:keys [output-path writers zookeeper zero-padded?]} millis]
  (-> (zknn/with-zk-namenode zookeeper output-path)
      (hist/lookup-writer-or-create writers millis zero-padded?)))

(defn dispose [writers {:keys [writer path]}]
  (hist/close-single-writer! writer path)
  (hist/remove-path! writers path))

(defn write-to-hdfs [{:keys [writers which-historizer last-error] :as self} {:keys [ts msg]}]
  (let [writer (writer-for-timestamp self ts)]
    (try
      (-> writer
          (hist/write-line! msg)
          (hist/touch-writer)
          (hist/store-writer writers))
      (counters/inc! (counters/counter ["file-historizer" which-historizer "write-to-hdfs"]))
      (catch IOException e
        (log/error e "Error occured when writing message: " msg " with ts: " ts)
        (reset! last-error {:msg       msg
                            :ts        ts
                            :exception e})
        (dispose writers writer))))
  msg)

(defrecord FileHistorizer [app-status config scheduler which-historizer zookeeper in-channel transform-or-nil-fn zero-padded?]
  c/Lifecycle
  (start [self]
    (log/info "-> starting FileHistorizer " which-historizer)
    (let [output-path (rpr/toplevel-path config which-historizer)
          max-age (rpr/max-age config which-historizer)
          close-interval (rpr/close-interval config which-historizer)
          dev-null (async/chan (async/dropping-buffer 1))
          writers (atom {})
          new-self (assoc self
                     :output-path output-path
                     :zero-padded? zero-padded?
                     :last-error (atom nil)
                     :writers writers)]
      (at/every close-interval #(hist/close-old-writers! writers max-age) (scheduler/pool scheduler) :desc (str "close old writers for " which-historizer))
      (apps/register-status-fun app-status (partial hist/historization-status-fn new-self))
      (async/pipeline 1 dev-null (comp
                                   (keep transform-or-nil-fn)
                                   (map (partial util-metrics/metered-execution
                                                 (str which-historizer "write-to-hdfs")
                                                 write-to-hdfs new-self))) in-channel)
      new-self))

  (stop [{:keys [writers] :as self}]
    (log/info "<- stopping FileHistorizer")
    (hist/close-writers! writers)
    self))

(defn new-file-historizer
  ([which-historizer in-channel]
   (new-file-historizer which-historizer in-channel identity false))
  ([which-historizer in-channel transform-or-nil-fn]
   (new-file-historizer which-historizer in-channel transform-or-nil-fn false))
  ([which-historizer in-channel transform-or-nil-fn zero-padded?]
   (map->FileHistorizer {:which-historizer    which-historizer
                         :in-channel          in-channel
                         :transform-or-nil-fn transform-or-nil-fn
                         :zero-padded?        zero-padded?})))
