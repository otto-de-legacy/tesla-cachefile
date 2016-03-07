(ns de.otto.tesla.cachefile.file-historizer-test
  (:require
    [clojure.test :refer :all]
    [de.otto.tesla.cachefile.file-historizer :as fh]
    [de.otto.tesla.cachefile.utils.zk-namenode :as zknn])
  (:import (java.io IOException)))


(deftest handling-errors-on-write
  (testing "should catch exceptions when writing"
    (with-redefs [zknn/with-zk-namenode (fn [_ _] (throw (IOException. "some dummy exception")))]
      (let [test-fh (fh/new-file-historizer "test-histo" nil)]
        (is (= "dummy-msg"
               (fh/write-to-hdfs test-fh {:msg "dummy-msg"})))))))
