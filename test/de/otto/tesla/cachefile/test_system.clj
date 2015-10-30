(ns de.otto.tesla.cachefile.test-system
  (:require [de.otto.tesla.zk.zk-observer :as zk]
            [de.otto.tesla.cachefile.cachefile-handler :as cfh]
            [com.stuartsierra.component :as c]
            [de.otto.tesla.system :as system]))

(defn test-system [runtime-conf]
  (-> (system/base-system runtime-conf)
      (assoc :zookeeper (c/using (zk/new-zkobserver) [:config]))
      (assoc :cachefile-handler (c/using (cfh/new-cachefile-handler) [:config :zookeeper]))))