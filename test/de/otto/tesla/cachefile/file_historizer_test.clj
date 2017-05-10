(ns de.otto.tesla.cachefile.file-historizer-test
  (:require
    [clojure.test :refer :all]
    [de.otto.tesla.cachefile.file-historizer :as fh]
    [de.otto.tesla.cachefile.utils.test-utils :as u]
    [com.stuartsierra.component :as c]
    [clojure.core.async :as async]
    [de.otto.tesla.cachefile.strategy.historization :as hist]
    [de.otto.tesla.system :as system]
    [clojure.tools.logging :as log])
  (:import (java.io IOException BufferedWriter Writer)
           (org.joda.time DateTimeZone)))

(defn test-system [runtime-conf in-channel]
  (-> (-> (system/base-system runtime-conf)
          (assoc
            :zookeeper {}
            :file-historizer (c/using (fh/new-file-historizer "test-historizer" in-channel) [:config :app-status :zookeeper :scheduler])))))

(def mock-writer (proxy [BufferedWriter] [(proxy [Writer] [])]
                   (write [_]) (newLine []) (flush []) (close [])))

(deftest channel-closed?-test
  (testing "should return true if channel is closed before timeout"
    (is (fh/pipeline-finished!? (async/timeout 10) 20)))
  (testing "should return false if channel is open after timeout"
    (is (not (fh/pipeline-finished!? (async/timeout 20) 10)))))

(defn avoid-flakyness-in-error-case-by-waiting []
  (Thread/sleep 100))

(deftest integration
  (let [in-channel (async/chan 1)]
    (with-redefs [hist/time-zone (constantly DateTimeZone/UTC)
                  hist/new-print-writer (constantly mock-writer)]
      (u/with-started [started (test-system {:test-historizer-toplevel-path "not used because of mock"} in-channel)]
                      (let [file-historizer (:file-historizer started)]
                        (testing "should initialize writer-instance for incoming message"
                          (async/>!! in-channel {:ts  (u/to-timestamp DateTimeZone/UTC 2016 3 2 11 11)
                                                 :msg "FOO-BAR"})
                          (Thread/sleep 200)
                          (is (= [2016 3 2 11]
                                 (get-in @(:writers file-historizer) [2016 3 2 11 :path])))
                          (is (= 1
                                 (get-in @(:writers file-historizer) [2016 3 2 11 :write-count]))))
                        (testing "closing the in channel means closing the writers"
                          (is (not-empty @(:writers file-historizer)))
                          (let [stop-future (future (c/stop file-historizer))]
                            (async/>!! in-channel {:ts  (u/to-timestamp DateTimeZone/UTC 2016 3 2 11 11)
                                                   :msg "FOO-BAR"})
                            (avoid-flakyness-in-error-case-by-waiting)
                            (async/close! in-channel)
                            @stop-future
                            (is (= {} @(:writers file-historizer))))
                          ))))))

(deftest handling-errors-on-write
  (testing "If a write fails should close & dispose writer"
    (let [time          ["2017" "05" "30" "12"]
          writer        {:writer "writer" :path time}
          writers       (atom (assoc-in {} (:path writer) writer))
          last-error    (atom nil)
          closed-writer (atom nil)
          logs          (atom [])]
      (with-redefs [fh/writer-for-timestamp (constantly writer)
                    hist/write-line! (fn [_ _] (throw (IOException. "write failed")))
                    hist/close-single-writer! (fn [writer-to-close _] (reset! closed-writer writer-to-close))
                    log/log* (fn [_ _ _ msg] (swap! logs conj msg) nil)]
        (fh/write-to-hdfs {:writers          writers
                           :which-historizer "test-historizer"
                           :last-error       last-error}
                          {:msg "dummy-msg"})
        (is (= "writer" @closed-writer))
        (is (= nil (get-in @writers time)))
        (is (= ["Error occured when writing message:  dummy-msg  with ts:  nil"] @logs))))))
