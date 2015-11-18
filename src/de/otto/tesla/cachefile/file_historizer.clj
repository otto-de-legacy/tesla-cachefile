(ns de.otto.tesla.cachefile.file-historizer
  (:require [com.stuartsierra.component :as c]
            [overtone.at-at :as at]
            [de.otto.tesla.cachefile.strategy.historization :as hist]
            [de.otto.tesla.cachefile.utils.zk-namenode :as zknn]
            [clojure.tools.logging :as log]
            [de.otto.tesla.cachefile.utils.reading-properties :as rpr]))

(defprotocol HistorizationHandling
  (writer-for-timestamp [self timestamp] "Returns a PrintWriter-instance for the given timestamp (see historization strategy)"))

(defrecord FileHistorizer [config which-historizer zookeeper]
  c/Lifecycle
  (start [self]
    (log/info "-> starting FileHistorizer")
    (let [output-path (rpr/configured-toplevel-path config which-historizer)
          executor (at/mk-pool)
          max-writer-age (rpr/configured-max-writer-age config which-historizer)
          schedule-closing-time (rpr/configured-schedule-closing-time config which-historizer)
          new-self (assoc self
                     :executor executor
                     :output-path output-path
                     :writers (atom {}))]
      (assoc new-self
        :schedule (at/every schedule-closing-time
                            #(hist/close-old-writers! (:writers new-self) max-writer-age)
                            executor))))

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
        :writer)))

(defn new-file-historizer [which-historizer]
  (map->FileHistorizer {:which-historizer which-historizer}))
