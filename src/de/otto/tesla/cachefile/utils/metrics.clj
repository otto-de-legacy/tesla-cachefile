(ns de.otto.tesla.cachefile.utils.metrics
  (:require [clojure.tools.logging :as log]
            [metrics.meters :as meters]
            [metrics.timers :as timers]))

(defn metered-execution [namespace fn & fn-params]
  (let [timing (timers/start (timers/timer (conj namespace "time")))
        exception-meter (meters/meter (conj namespace "exception"))
        messages-meter (meters/meter (conj namespace "messages" "processed"))]
    (try
      (let [return-value (apply fn fn-params)]
        (meters/mark! messages-meter)
        return-value)
      (catch Exception e
        (meters/mark! exception-meter)
        (log/error e (str "Exception in " namespace))
        (throw e))
      (finally
        (timers/stop timing)))))
