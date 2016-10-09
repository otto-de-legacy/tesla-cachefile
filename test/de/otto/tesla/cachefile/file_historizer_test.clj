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
    [de.otto.tesla.stateful.app-status :as apps]
    [de.otto.tesla.cachefile.strategy.historization :as hist])
  (:import (java.io IOException)))

(defn test-system [runtime-conf in-channel]
  (-> (system/base-system runtime-conf)
      (assoc :zookeeper (c/using (zk/new-zkobserver) [:config]))
      (assoc :file-historizer (c/using (fh/new-file-historizer "test-historizer" in-channel) [:config :app-status :zookeeper]))))

(deftest integration
  (let [in-channel (async/chan 1)]
    (u/with-started [started (test-system {:test-historizer-toplevel-path "target/test-historizer"} in-channel)]
                    (let [file-historizer (:file-historizer started)]
                      (async/>!! in-channel {:ts  (u/to-utc-timestamp 2016 3 2 11 11)
                                             :msg "FOO-BAR"})
                      (testing "should initialize writer-instance for incoming message"
                          (Thread/sleep 200)
                        (is (= [2016 3 2 11]
                               (get-in @(:writers file-historizer) [2016 3 2 11 :path])))
                        (is (= 1
                               (get-in @(:writers file-historizer) [2016 3 2 11 :write-count]))))))))


(deftest handling-errors-on-write
  (testing "should catch exceptions when writing"
    (with-redefs [apps/register-status-fun (fn [_ fun] (fun))
                  hist/writing-error-status-fn (fn [which-historizer msg e]
                                                 (do
                                                   (is (= "test-histo" which-historizer))
                                                   (is (= "dummy-msg" msg))
                                                   (is (= "some dummy exception" (.getMessage e)))))
                  zknn/with-zk-namenode (fn [_ _] (throw (IOException. "some dummy exception")))]
      (let [test-fh (fh/new-file-historizer "test-histo" nil)]
        (is (= "dummy-msg"
               (fh/write-to-hdfs test-fh nil {:msg "dummy-msg"})))))))
