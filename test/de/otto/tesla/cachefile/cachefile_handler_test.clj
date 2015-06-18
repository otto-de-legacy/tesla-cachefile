(ns de.otto.tesla.cachefile.cachefile-handler-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.cachefile-handler :as cfh]
            [clojure.java.io :as io]
            [de.otto.tesla.system :as system]
            [com.stuartsierra.component :as c]
            [de.otto.tesla.util.test-utils :as u]
            [de.otto.tesla.zk.zk-observer :as zk])
  (:import (org.apache.hadoop.hdfs.server.namenode.ha.proto HAZKInfoProtos$ActiveNodeInfo)))

(defn- test-system [runtime-conf]
  (-> (system/base-system runtime-conf)
      (assoc :zookeeper (c/using (zk/new-zkobserver) [:config]))
      (assoc :cachefile-handler (c/using (cfh/new-cachefile-handler) [:config :zookeeper]))))

(deftest ^:unit check-for-hdfs-file-path
  (testing "should detect HDFS file path"
    (is (cfh/is-hdfs-file-path "hdfs://somedir/somefile.txt")))
  (testing "should detect local file path"
    (is (not (cfh/is-hdfs-file-path "/localdir/somefile.txt")))))

(deftest ^:unit check-the-existence-of-files
  (let [file-path "/tmp/testlocalfile.txt"]
    (u/with-started [started (test-system {:cache-file file-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "check if local file exists"
                        (spit file-path "")
                        (is (cfh/cache-file-exists cfh)))
                      (testing "check if local file does not exist"
                        (.delete (io/file file-path))
                        (is (not (cfh/cache-file-exists cfh))))))))

(deftest ^:unit check-the-existence-of-hdfs-files
  (let [file-path "hdfs://tmp/testlocalfile.txt"]
    (with-redefs [cfh/hdfs-file-exist (fn [_ _] true)]
      (u/with-started [started (test-system {:cache-file file-path})]
                      (testing "check if hdfs-file exists"
                        (is (cfh/cache-file-exists (:cachefile-handler started))))))))

(deftest ^:unit check-the-existence-of-hdfs-files2
  (let [file-path "hdfs://tmp/testlocalfile.txt"]
    (with-redefs [cfh/hdfs-file-exist (fn [_ _] false)]
      (u/with-started [started (test-system {:cache-file file-path})]
                      (testing "check if hdfs-file does not exist"
                        (is (not (cfh/cache-file-exists (:cachefile-handler started)))))))))

(deftest ^:unit check-reading-the-content-of-files
  (let [file-path "/tmp/testlocalfile.txt"]
    (u/with-started [started (test-system {:cache-file file-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "reading content from a local file"
                        (spit file-path "somevalue=foo")
                        (is (= "somevalue=foo"
                               (cfh/read-cache-file cfh))))))))

(deftest ^:unit check-reading-the-content-of-hdfs-files
  (let [file-path "hdfs://tmp/testlocalfile.txt"]
    (with-redefs [cfh/hdfs-file-exist (fn [_ _] true)
                  cfh/read-hdfs-file (fn [_ _] "some_hdfs_content")]
      (u/with-started [started (test-system {:cache-file file-path})]
                      (let [cfh (:cachefile-handler started)]
                        (testing "reading content from a hdfs file"
                          (is (= "some_hdfs_content"
                                 (cfh/read-cache-file cfh)))))))))

(deftest ^:unit check-writing-files
  (let [file-path "/tmp/testlocalfile.txt"]
    (u/with-started [started (test-system {:cache-file file-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "writing a local file"
                        (.delete (io/file file-path))
                        (cfh/write-cache-file cfh "some-content")
                        (is (= "some-content"
                               (cfh/read-cache-file cfh))))))))

(deftest ^:unit writing-a-hdfs-file
  (let [file-path "/tmp/testlocalfile.txt"]
    (with-redefs [cfh/write-hdfs-file (fn [_ _ content] (spit file-path content))
                  cfh/read-hdfs-file (fn [_ _] (slurp file-path))
                  cfh/hdfs-file-exist (fn [_ _] true)]
      (u/with-started [started (test-system {:cache-file "hdfs://notUsedForWritingBecauseOfMocks"})]
                      (let [cfh (:cachefile-handler started)]
                        (cfh/write-cache-file cfh "some-content")
                        (is (= "some-content"
                               (cfh/read-cache-file cfh))))))))

(deftest ^:unit build-file-system
  (testing "should use hdfs namenode if property is set"
    (let [c (cfh/get-hdfs-conf "hdfs://someHost")]
      (is (= "hdfs://someHost"
             (.get c "fs.defaultFS"))))))

(deftest ^:unit should-check-if-hdfs-file-exists
  (with-redefs [cfh/hdfs-file-exist (fn [_ _] true)]
    (u/with-started [started (test-system {:cache-file    "hdfs://somePath"
                                           :hdfs-namenode "hdfs://someHost"})]
                    (let [cf-handler (:cachefile-handler started)]
                      (is (= (cfh/cache-file-exists cf-handler)
                             true))))))

(deftest ^:unit should-check-if-local-file-exists
  (spit "/tmp/somePath" "")
  (u/with-started [started (test-system {:cache-file "/tmp/somePath"})]
                  (let [cf-handler (:cachefile-handler started)]
                    (is (= (cfh/cache-file-exists cf-handler)
                           true)))))

(deftest ^:unit if-cache-file-isnot-defined-it-should-not-exist
  (u/with-started [started (test-system {})]
                  (let [cf-handler (:cachefile-handler started)]
                    (is (= (cfh/cache-file-exists cf-handler)
                           false)))))

(deftest ^:unit should-return-nil-for-undefined-path
  (is (= (nil? (cfh/is-hdfs-file-path nil))
         false))
  (is (= (nil? (cfh/without-hdfs-prefix nil))
         true)))

(deftest ^:unit if-cache-file-configured-it-is-defined
  (u/with-started [started (test-system {:cache-file "hdfs:/somePath"})]
                  (let [cf-handler (:cachefile-handler started)]
                    (is (= true (cfh/cache-file-defined cf-handler))))))

(deftest ^:unit if-cache-file-not-configured-it-is-not-defined
  (u/with-started [started (test-system {})]
                  (let [cf-handler (:cachefile-handler started)]
                    (is (= false (cfh/cache-file-defined cf-handler))))))

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
             (cfh/parse-hostname a-zk-response))))))
