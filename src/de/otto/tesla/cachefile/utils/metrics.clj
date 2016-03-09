(ns de.otto.tesla.cachefile.utils.metrics
  (:require [clojure.tools.logging :as log]
            [metrics.meters :as meters]
            [metrics.timers :as timers]))

(defn metered-execution [component-name fn & fn-params]
  (let [timing (timers/start (timers/timer [component-name "time"]))
        exception-meter (meters/meter [component-name "exception"])
        messages-meter (meters/meter [component-name "messages" "processed"])]
    (try
      (let [return-value (apply fn fn-params)]
        (meters/mark! messages-meter)
        return-value)
      (catch Exception e
        (meters/mark! exception-meter)
        (log/error e (str "Exception in " component-name))
        (throw e))
      (finally
        (timers/stop timing)))))
