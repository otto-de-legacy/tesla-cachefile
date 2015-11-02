(ns de.otto.tesla.cachefile.hdfs-generations-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.hdfs-generations :as hdfsgens]
            [de.otto.tesla.cachefile.cachefile-handler :as cfh]
            [clojure.java.io :as io]
            [de.otto.tesla.cachefile.test-system :as ts]
            [de.otto.tesla.util.test-utils :as u])
  (:import (org.apache.hadoop.fs FileUtil)
           (java.io File)))

(deftest ^:unit test-hdfs-generation-injection
  (u/with-started [started (ts/test-system {:test-data-toplevel-path "/tmp/foo/{GENERATION}/subfolder"})]
                  (let [cfh (:cachefile-handler started)]
                    (testing "should inject latest generation"
                      (FileUtil/fullyDelete (File. "/tmp/foo"))
                      (io/make-parents "/tmp/foo/000029/subfolder/<-")
                      (io/make-parents "/tmp/foo/000030/subfolder/<-")
                      (io/make-parents "/tmp/foo/000031/subfolder/<-")
                      (spit "/tmp/foo/000030/subfolder/_SUCCESS" "")
                      (spit "/tmp/foo/000030/subfolder/foo.bar" "baz")
                      (is (= "/tmp/foo/000030/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :read)))
                      (is (= "/tmp/foo/000031/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :write)))
                      (is (= "baz" (cfh/slurp-cache-file cfh "foo.bar")))))))

(deftest ^:unit test-hdfs-generation-injection-with-write-ahead
  (u/with-started [started (ts/test-system {:test-data-toplevel-path "/tmp/foo/{GENERATION}/subfolder"})]
                  (let [cfh (:cachefile-handler started)]
                    (testing "should inject latest generation"
                      (FileUtil/fullyDelete (File. "/tmp/foo"))
                      (io/make-parents "/tmp/foo/000029/subfolder/<-")
                      (io/make-parents "/tmp/foo/000030/subfolder/<-")
                      (io/make-parents "/tmp/foo/000031/subfolder/<-")
                      (spit "/tmp/foo/000031/subfolder/_SUCCESS" "")
                      (spit "/tmp/foo/000031/subfolder/foo.bar" "baz")
                      (is (= "/tmp/foo/000031/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :read)))
                      (is (= "/tmp/foo/000032/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :write)))
                      (is (= "baz" (cfh/slurp-cache-file cfh "foo.bar")))))))

(deftest ^:unit test-hdfs-generation-injection-no-generation-present
  (u/with-started [started (ts/test-system {:test-data-toplevel-path "/tmp/foo/{GENERATION}/subfolder"})]
                  (let [cfh (:cachefile-handler started)]
                    (testing "should inject latest generation"
                      (FileUtil/fullyDelete (File. "/tmp/foo"))
                      (io/make-parents "/tmp/foo/<-")
                      (is (= "/tmp/foo/000000/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :read)))
                      (is (= "/tmp/foo/000000/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :write)))
                      (cfh/write-cache-file cfh "foo.bar" ["baz"])
                      (is (= "baz" (cfh/slurp-cache-file cfh "foo.bar")))))))

(def parent-of-latest-generation #'hdfsgens/parentpath-of-generation-placeholder)
(deftest parent-paths
  (testing "should return parent path for latest generation"
    (is (= "/tmp/foo/" (parent-of-latest-generation "/tmp/foo/{GENERATION}/foo/bar"))))
  (testing "should return parent path for latest generation"
    (is (= nil (parent-of-latest-generation "/tmp/foo/bar")))))

(def all-generations #'hdfsgens/all-generations)
(deftest generations
  (testing "should determine all generations"
    (FileUtil/fullyDelete (File. "/tmp/foo"))
    (io/make-parents "/tmp/foo/000029/foo")
    (io/make-parents "/tmp/foo/000030/foo")
    (io/make-parents "/tmp/foo/000031/foo")
    (is (= ["000029" "000030" "000031"] (all-generations "/tmp/foo/"))))
  (testing "should determine all generations not present case"
    (FileUtil/fullyDelete (File. "/tmp/foo"))
    (io/make-parents "/tmp/foo/foo")
    (is (= [] (all-generations "/tmp/foo/")))))

(def latest-generation #'hdfsgens/latest-generation)
(deftest test-latest-generation
  (testing "should determine latest generation"
    (is (= "000031" (latest-generation ["000029" "000030" "000031"]))))
  (testing "should determine latest generation with fallback"
    (is (= "000000" (latest-generation [])))))

(def increase-generation #'hdfsgens/increase-generation)
(deftest test-increase-generation
  (testing "should-increase-generation-by-1"
    (is (= "000001" (increase-generation "000000"))))
  (testing "should-increase-generation-by-1-again"
    (is (= "000100" (increase-generation "000099"))))
  (testing "should-increase-generation-by-1-one-more-time"
    (is (= "100000" (increase-generation "099999"))))
  (testing "should-increase-generation-by-1-again-again-again"
    (is (= "000000" (increase-generation "999999")))))

(def as-generation-string #'hdfsgens/as-generation-string)
(deftest test-as-generation-string
  (testing "should-return generation string with 6 digits"
    (is (= "000000" (as-generation-string 0))))
  (testing "should-return generation string with 6 digits again"
    (is (= "000099" (as-generation-string 99))))
  (testing "should-return generation string with 6 digits one-more-time"
    (is (= "099999" (as-generation-string 99999))))
  (testing "should-return generation string with 6 digits again-again"
    (is (= "000000" (as-generation-string 9999999))))
  (testing "should-return generation string with 6 digits again-again-again"
    (is (= "000000" (as-generation-string -1)))))
