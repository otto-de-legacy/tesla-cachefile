(ns de.otto.tesla.cachefile.file-historizer-test
  (:require
    [clojure.test :refer :all]
    [de.otto.tesla.cachefile.file-historizer :as fh]
    [de.otto.tesla.cachefile.utils.zk-namenode :as zknn]
    [de.otto.tesla.cachefile.utils.test-utils :as u]
    [de.otto.tesla.system :as system]
    [com.stuartsierra.component :as c]
    [de.otto.tesla.zk.zk-observer :as zk]
    [clojure.core.async :as async]
    [de.otto.tesla.stateful.app-status :as apps])
  (:import (java.io IOException)))

(defn test-system [runtime-conf in-channel]
  (-> (system/base-system runtime-conf)
      (assoc :zookeeper (c/using (zk/new-zkobserver) [:config]))
      (assoc :file-historizer (c/using (fh/new-file-historizer "test-historizer" in-channel) [:config :app-status :zookeeper]))))

(def three-seconds 3000)

(defn too-much-time-passed-since [start-time]
  (let [time-taken (- (System/currentTimeMillis) start-time)]
    (> time-taken three-seconds)))

(deftest integration
  (let [in-channel (async/chan 1)]
    (u/with-started [started (test-system {:test-historizer-toplevel-path "target/test-historizer"} in-channel)]
                    (let [file-historizer (:file-historizer started)
                          start-time (System/currentTimeMillis)]
                      (testing "should initialize writer-instance for incoming message"
                        (async/>!! in-channel {:ts  (u/to-utc-timestamp 2016 3 2 11 11)
                                               :msg "FOO-BAR"})
                        (while
                          (and
                            (empty? @(:writers file-historizer))
                            (not (too-much-time-passed-since start-time))))

                        (is (= [2016 3 2 11]
                               (get-in @(:writers file-historizer) [2016 3 2 11 :path])))
                        (is (= 1
                               (get-in @(:writers file-historizer) [2016 3 2 11 :write-count]))))))))


(deftest handling-errors-on-write
  (testing "should catch exceptions when writing"
    (with-redefs [apps/register-status-fun (constantly nil)
                  zknn/with-zk-namenode (fn [_ _] (throw (IOException. "some dummy exception")))]
      (let [test-fh (-> (fh/new-file-historizer "test-histo" (async/chan 0))
                        (c/start))
            last-error (:last-error test-fh)]
        (try
          (is (= "dummy-msg"
                 (fh/write-to-hdfs test-fh {:msg "dummy-msg"})))
          (is (= "dummy-msg"
                 (:msg @last-error)))
          (is (= "some dummy exception"
                 (.getMessage (:exception @last-error))))
          (finally
            (c/stop test-fh)))))))
