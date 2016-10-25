(ns de.otto.tesla.cachefile.file-historizer-test
  (:require
    [clojure.test :refer :all]
    [de.otto.tesla.cachefile.file-historizer :as fh]
    [de.otto.tesla.cachefile.utils.zk-namenode :as zknn]
    [de.otto.tesla.cachefile.utils.test-utils :as u]
    [com.stuartsierra.component :as c]
    [clojure.core.async :as async]
    [de.otto.tesla.stateful.app-status :as apps]
    [com.stuartsierra.component :as comp]
    [de.otto.tesla.cachefile.strategy.historization :as hist])
  (:import (java.io IOException BufferedWriter Writer)
           (org.joda.time DateTimeZone)))

(defn test-system [runtime-conf in-channel]
  (-> (comp/system-map
        :config {:config runtime-conf}
        :app-status {} :zookeeper {}
        :file-historizer (c/using (fh/new-file-historizer "test-historizer" in-channel) [:config :app-status :zookeeper]))))

(def mock-writer (proxy [BufferedWriter] [(proxy [Writer] [])]
                   (write [_]) (newLine []) (flush []) (close [])))

(deftest integration
  (let [in-channel (async/chan 1)]
    (with-redefs [apps/register-status-fun (constantly nil)
                  hist/time-zone (constantly DateTimeZone/UTC)
                  hist/new-print-writer (constantly mock-writer)]
      (u/with-started [started (test-system {:test-historizer-toplevel-path "not used because of mock"} in-channel)]
                      (let [file-historizer (:file-historizer started)
                            start-time (System/currentTimeMillis)]
                        (testing "should initialize writer-instance for incoming message"
                          (async/>!! in-channel {:ts  (u/to-timestamp DateTimeZone/UTC 2016 3 2 11 11)
                                                 :msg "FOO-BAR"})
                          (Thread/sleep 200)
                          (println "Done waiting for result after " (- (System/currentTimeMillis) start-time)
                                   " millis. nr-results: " (count @(:writers file-historizer))
                                   "  writers: " @(:writers file-historizer))
                          (is (= [2016 3 2 11]
                                 (get-in @(:writers file-historizer) [2016 3 2 11 :path])))
                          (is (= 1
                                 (get-in @(:writers file-historizer) [2016 3 2 11 :write-count])))))))))

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
