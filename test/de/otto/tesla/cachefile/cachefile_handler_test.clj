(ns de.otto.tesla.cachefile.cachefile-handler-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.cachefile-handler :as cfh]
            [clojure.java.io :as io]
            [de.otto.tesla.cachefile.test-system :as ts]
            [de.otto.tesla.util.test-utils :as u])
  (:import (org.apache.hadoop.hdfs.server.namenode.ha.proto HAZKInfoProtos$ActiveNodeInfo)
           (org.apache.hadoop.fs FileUtil)
           (java.io File)))

(def get-config-key #'cfh/get-config-key)
(deftest ^:unit check-for-get-config-key
  (testing "should return keyword without postfix if file type is missing"
    (is (= (get-config-key "") :cache-file)))
  (testing "should return keyword with file type as postfix"
    (is (= (get-config-key "csv") :cache-file-csv))))

(deftest ^:unit check-the-existence-of-files
  (let [file-path "/tmp/testlocalfile.txt"]
    (u/with-started [started (ts/test-system {:cache-file file-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "check if local file exists"
                        (spit file-path "")
                        (is (= true (cfh/cache-file-exists cfh))))
                      (testing "check if local file does not exist"
                        (.delete (io/file file-path))
                        (is (not (cfh/cache-file-exists cfh))))))))

(deftest ^:unit if-cache-file-isnot-defined-it-should-not-exist
  (u/with-started [started (ts/test-system {})]
                  (let [cf-handler (:cachefile-handler started)]
                    (is (= (cfh/cache-file-exists cf-handler)
                           false)))))

(deftest ^:unit check-reading-the-content-of-files
  (let [file-path "/tmp/testlocalfile.txt"
        crc-file "/tmp/.testlocalfile.txt.crc"]
    (u/with-started [started (ts/test-system {:cache-file file-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "reading content from a local file"
                        (.delete (io/file file-path))
                        (.delete (io/file crc-file))
                        (spit file-path "somevalue=foo")
                        (is (= "somevalue=foo"
                               (cfh/read-cache-file cfh))))))))

(deftest ^:unit check-writing-files
  (let [file-path "/tmp/testlocalfile.txt"]
    (u/with-started [started (ts/test-system {:cache-file file-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "writing a local file"
                        (.delete (io/file file-path))
                        (cfh/write-cache-file cfh "some-content")
                        (is (= "some-content"
                               (cfh/read-cache-file cfh))))))))

(deftest ^:unit check-writing-files-with-latest-generation
  (let [file-path "/tmp/foo/{GENERATION}/testlocalfile.txt"]
    (u/with-started [started (ts/test-system {:cache-file file-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "should write a local file to a generation path"
                        (FileUtil/fullyDelete (File. "/tmp/foo"))
                        (is (= "/tmp/foo/000000/testlocalfile.txt" (cfh/current-cache-file cfh :read)))
                        (is (= "/tmp/foo/000000/testlocalfile.txt" (cfh/current-cache-file cfh :write)))
                        (cfh/write-cache-file cfh "some-content")
                        (is (= "/tmp/foo/000000/testlocalfile.txt" (cfh/current-cache-file cfh :read)))
                        (is (= "/tmp/foo/000001/testlocalfile.txt" (cfh/current-cache-file cfh :write)))
                        (is (= "some-content"
                               (cfh/read-cache-file cfh))))))))

(deftest ^:unit if-cache-file-configured-it-is-defined
  (u/with-started [started (ts/test-system {:cache-file "hdfs:/somePath"})]
                  (let [cf-handler (:cachefile-handler started)]
                    (is (= true (cfh/cache-file-defined cf-handler))))))
;
(deftest ^:unit if-cache-file-not-configured-it-is-not-defined
  (u/with-started [started (ts/test-system {})]
                  (let [cf-handler (:cachefile-handler started)]
                    (is (= false (cfh/cache-file-defined cf-handler))))))

(def parse-hostname #'cfh/parse-hostname)
(deftest ^:unit parse-zk-response
  (testing "should extract the hostname from a valid zk-response"
    (let [a-zk-response (-> (HAZKInfoProtos$ActiveNodeInfo/newBuilder)
                            (.setHostname "some.host.de")
                            (.setNameserviceId "nameserviceid")
                            (.setNamenodeId "namenodeid")
                            (.setPort 123)
                            (.setZkfcPort 123)
                            .build
                            .toByteArray)]
      (is (= "some.host.de"
             (parse-hostname a-zk-response))))))

(deftest ^:unit test-namenode-injection
  (let [file-path "hdfs://{ZK_NAMENODE}/foo/bar"
        namenode (atom "first_namenode")]
    (with-redefs [cfh/namenode-from-zookeeper (fn [_] @namenode)]
      (u/with-started [started (ts/test-system {:cache-file file-path})]
                      (let [cfh (:cachefile-handler started)]
                        (testing "check if zookeeper-namenode gets injected"
                          (is (= "hdfs://first_namenode/foo/bar" (cfh/current-cache-file cfh :read)))
                          (is (= "hdfs://first_namenode/foo/bar" (cfh/current-cache-file cfh :write))))
                        (reset! namenode "second_namenode")
                        (testing "check if zookeeper-namenode gets injected with fresh value"
                          (is (= "hdfs://second_namenode/foo/bar" (cfh/current-cache-file cfh :read)))
                          (is (= "hdfs://second_namenode/foo/bar" (cfh/current-cache-file cfh :write)))))))))
