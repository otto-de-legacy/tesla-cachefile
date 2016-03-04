(ns de.otto.tesla.cachefile.file-historizer-test
  (:require
    [clojure.test :refer :all]
    [de.otto.tesla.cachefile.file-historizer :as fh]))


(deftest handling-errors-on-write
  (testing "should catch exceptions when writing"
    (with-redefs [fh/writer-for-timestamp (fn [_ _] (throw (RuntimeException. "some dummy exception")))]
      (let [test-fh (fh/new-file-historizer "test-histo" nil)]
        (is (= "dummy-msg"
               (fh/write-to-hdfs test-fh {:msg "dummy-msg"})))))))
