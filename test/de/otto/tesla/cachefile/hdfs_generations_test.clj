(ns de.otto.tesla.cachefile.hdfs-generations-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.hdfs-generations :as hdfsgens]
            [de.otto.tesla.cachefile.cachefile-handler :as cfh]
            [clojure.java.io :as io]
            [de.otto.tesla.cachefile.test-system :as ts]
            [de.otto.tesla.util.test-utils :as u]
            )
  (:import (org.apache.hadoop.fs FileUtil)
           (java.io File)))

(deftest ^:unit test-hdfs-generation-injection
         (let [file-path "/tmp/foo/{LATEST_GENERATION}/foo"]
           (u/with-started [started (ts/test-system {:cache-file file-path})]
                           (let [cfh (:cachefile-handler started)]
                             (testing "should inject latest generation"
                                      (FileUtil/fullyDelete (File. "/tmp/foo"))
                                      (io/make-parents "/tmp/foo/000029/foo")
                                      (io/make-parents "/tmp/foo/000030/foo")
                                      (io/make-parents "/tmp/foo/000031/foo")
                                      (spit "/tmp/foo/000031/foo" "bar")
                                      (is (= "/tmp/foo/000031/foo" (cfh/current-cache-file cfh)))
                                      (is (= "bar" (cfh/read-cache-file cfh))))))))

(def parent-of-latest-generation #'hdfsgens/parent-of-latest-generation)
(deftest parent-paths
         (testing "should return parent path for latest generation"
                  (is (= "/tmp/foo/"  (parent-of-latest-generation "/tmp/foo/{LATEST_GENERATION}/foo/bar"))))
         (testing "should return parent path for latest generation"
                  (is (= nil  (parent-of-latest-generation "/tmp/foo/bar")))))


(def all-generations #'hdfsgens/all-generations)
(deftest generations
         (testing "should determine all generations"
                  (FileUtil/fullyDelete (File. "/tmp/foo"))
                  (io/make-parents "/tmp/foo/000029/foo")
                  (io/make-parents "/tmp/foo/000030/foo")
                  (io/make-parents "/tmp/foo/000031/foo")
                  (is (= ["000029" "000030" "000031"]  (all-generations "/tmp/foo/")))))

(def latest-generation #'hdfsgens/latest-generation)
(deftest test-latest-generation
         (testing "should determine latest generation"
                  (is (= "000031"  (latest-generation ["000029" "000030" "000031"])))))


