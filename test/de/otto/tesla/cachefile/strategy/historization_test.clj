(ns de.otto.tesla.cachefile.strategy.historization-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.strategy.historization :as hist])
  (:import (java.io Flushable Closeable IOException)
           (org.joda.time DateTime)))

(def ts->time-map #'hist/ts->time-map)

(deftest extracting-the-time
  (testing "should extract the timestamp"
    (is (= {:hour  9
            :day   17
            :month 11
            :year  2015}
           (ts->time-map (.getMillis (DateTime. 2015 11 17 9 0))))))
  (testing "should extract the timestamp as 0"
    (is (= {:hour  0
            :day   1
            :month 1
            :year  1970}
           (ts->time-map (.getMillis (DateTime. 1970 1 1 0 0)))))))

(def output-file-path #'hist/output-file-path)
(deftest creating-the-ouput-path
  (with-redefs [hist/unique-id (constantly "a-unique-id")]
    (testing "should create an output-path with a unique file-name"
      (is (= "output/path/2015/1/17/10/a-unique-id.hist.gz"
             (output-file-path "output/path" {:year  2015
                                              :month 1
                                              :day   17
                                              :hour  10}))))))
(def unique-id #'hist/unique-id)
(deftest creating-unique-ids
  (testing "should create a unique id"
    (let [ids (atom #{})]
      (dotimes [_ 1000]
        (swap! ids conj (unique-id)))
      (is (= 1000 (count @ids))))))


(def lookup-writer-or-create! #'hist/lookup-writer-or-create)
(deftest looking-up-writers
  (testing "should look up existing writer from map"
    (with-redefs [hist/current-time (constantly 999)]
      (let [writer {:writer      "WRITER"
                    :file-path   "some/path"
                    :path        [2015 11 17 9]
                    :write-count 0
                    :last-access 123}
            writers-map (atom {2015 {11 {17 {9 writer}}}})]
        (is (= writer
               (lookup-writer-or-create! "some-path" writers-map (.getMillis (DateTime. 2015 11 17 9 0))))))))


  (testing "should create a new writer and store it"
    (with-redefs [hist/current-time (constantly 999)
                  hist/unique-id (constantly "unique-id")
                  hist/new-print-writer (constantly "WRITER")]
      (let [writers-map (atom {})
            expected-writer {:file-path   "some-path/2015/11/17/9/unique-id.hist.gz"
                             :last-access 999
                             :path        [2015 11 17 9]
                             :write-count 0
                             :writer      "WRITER"}]
        (is (= expected-writer
               (lookup-writer-or-create! "some-path" writers-map (.getMillis (DateTime. 2015 11 17 9 0)))))))))


(def find-all-writers #'hist/find-all-writers)
(deftest finding-all-writers
  (testing "should find all writers in a map"
    (let [the-search-map {2015 {10 {1 {2 {:writer      "WRITER-A"
                                          :file-path   "some/path"
                                          :path        [2015 10 1 2]
                                          :write-count 2
                                          :last-access 123}}}
                                11 {11 {11 {:writer      "WRITER-B"
                                            :file-path   "some/path"
                                            :path        [2015 11 11 11]
                                            :write-count 3
                                            :last-access 123}}
                                    17 {10 {:writer      "WRITER-C"
                                            :file-path   "some/path"
                                            :path        [2015 11 17 10]
                                            :write-count 4
                                            :last-access 123}}}}}]
      (is (= [{:file-path   "some/path"
               :last-access 123
               :path        [2015 10 1 2]
               :write-count 2
               :writer      "WRITER-A"}
              {:file-path   "some/path"
               :last-access 123
               :path        [2015 11 11 11]
               :write-count 3
               :writer      "WRITER-B"}
              {:file-path   "some/path"
               :last-access 123
               :path        [2015 11 17 10]
               :write-count 4
               :writer      "WRITER-C"}]
             (find-all-writers the-search-map))))))


(defn CloseableMock [atm mockname]
  (proxy [Closeable Flushable] []
    (flush []
      (swap! atm update :flushed conj mockname))
    (close []
      (swap! atm update :closed conj mockname))))

(def close-writers! #'hist/close-writers!)
(deftest closing-all-writers
  (testing "should remove all writers"
    (let [closed-writers (atom {:closed  []
                                :flushed []})
          the-search-map (atom {2015 {10 {1 {2 {:writer      (CloseableMock closed-writers "WRITER-A")
                                                :file-path   "some/path"
                                                :path        [2015 10 1 2]
                                                :write-count 0
                                                :last-access 123}}}
                                      11 {11 {11 {:writer      (CloseableMock closed-writers "WRITER-B")
                                                  :file-path   "some/path"
                                                  :path        [2015 11 11 11]
                                                  :write-count 0
                                                  :last-access 123}}
                                          17 {10 {:writer      (CloseableMock closed-writers "WRITER-C")
                                                  :file-path   "some/path"
                                                  :path        [2015 11 17 10]
                                                  :write-count 0
                                                  :last-access 123}}}}})]
      (close-writers! the-search-map)
      (is (= {}
             @the-search-map))
      (is (= {:closed  ["WRITER-A" "WRITER-B" "WRITER-C"]
              :flushed ["WRITER-A" "WRITER-B" "WRITER-C"]}
             @closed-writers)))))

(def writer-too-old? #'hist/writer-too-old?)
(deftest closing-all-unused-writers
  (testing "should close all writers which have not been used for some time"
    (with-redefs [hist/current-time (constantly 250)]
      (let [closed-writers (atom {:closed  []
                                  :flushed []})
            writer-c (CloseableMock closed-writers "WRITER-C")
            the-search-map (atom {2015 {10 {1 {2 {:writer      (CloseableMock closed-writers "WRITER-A")
                                                  :file-path   "some/path"
                                                  :path        [2015 10 1 2]
                                                  :write-count 0
                                                  :last-access 100}}}
                                        11 {11 {11 {:writer      (CloseableMock closed-writers "WRITER-B")
                                                    :file-path   "some/path"
                                                    :path        [2015 11 11 11]
                                                    :write-count 2
                                                    :last-access 150}}
                                            17 {10 {:writer      writer-c
                                                    :file-path   "some/path"
                                                    :path        [2015 11 17 10]
                                                    :write-count 3
                                                    :last-access 200}}}}})]

        (close-writers! the-search-map (partial writer-too-old? 100))
        (is (= {2015 {11 {17 {10 {:writer      writer-c
                                  :file-path   "some/path"
                                  :path        [2015 11 17 10]
                                  :write-count 3
                                  :last-access 200}}}}}
               @the-search-map))
        (is (= {:closed  ["WRITER-A" "WRITER-B"]
                :flushed ["WRITER-A" "WRITER-B"]}
               @closed-writers))))))



(deftest exceptions-on-closing
  (testing "should catch exception and not set writer-instance to nil if an exception occures"
    (with-redefs [hist/close-single-writer! (fn [_ _] (throw (IOException. "a dummy exception")))
                  hist/find-all-writers (constantly [{:path [:foo]}])]
      (let [writers (atom {:foo "some-writer"})]
        (is (= nil (hist/close-writers! writers)))
        (is (= "some-writer" (:foo @writers)))))))

(deftest the-status-fn
  (testing "should build status response"
    (let [closed-writers (atom {:closed  []
                                :flushed []})
          some-data (atom {2015 {10 {1 {2 {:writer      (CloseableMock closed-writers "WRITER-A")
                                           :file-path   "some/path"
                                           :path        [2015 10 1 2]
                                           :write-count 2
                                           :last-access 100}}}
                                 11 {11 {10 nil
                                         11 {:writer      (CloseableMock closed-writers "WRITER-B")
                                             :file-path   "some/path"
                                             :path        [2015 11 11 11]
                                             :write-count 3
                                             :last-access 150}}}}})]
      (is (= {:some-name {:message "all ok"
                          :status  :ok
                          :writers {2015 {10 {1 {2 {:file-path   "some/path"
                                                    :write-count 2
                                                    :last-access 100}}}
                                          11 {11 {10 nil
                                                  11 {:file-path   "some/path"
                                                      :write-count 3
                                                      :last-access 150}}}}}}}
             (hist/historization-status-fn some-data "some-name"))))))

(deftest storing-writers
  (testing "should store a writer at the path of the writer"
    (let [writers (atom {})
          the-writer {:writer      "foobar"
                      :file-path   "some/path"
                      :path        [2015 11 11 11]
                      :write-count 3
                      :last-access 150}]
      (hist/store-writer the-writer writers)
      (is (= {2015 {11 {11 {11 the-writer}}}}
             @writers)))))

(deftest touching
  (testing "touching a writer"
    (with-redefs [hist/current-time (constantly 250)]
      (is (= {:last-access 250
              :write-count 51}
             (hist/touch-writer {:write-count 50}))))))
