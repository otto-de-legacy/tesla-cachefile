(ns de.otto.tesla.cachefile.cachefile-handler-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.cachefile-handler :as cfh]
            [clojure.java.io :as io]
            [de.otto.tesla.cachefile.test-system :as ts]
            [de.otto.tesla.util.test-utils :as u])
  (:import (org.apache.hadoop.hdfs.server.namenode.ha.proto HAZKInfoProtos$ActiveNodeInfo)
           (org.apache.hadoop.fs FileUtil)
           (java.io File)))

(def configured-toplevel-path #'cfh/configured-toplevel-path)
(deftest ^:unit check-for-toplevel-path
  (testing "should return keyword without postfix if file type is missing"
    (is (= "foo-bar" (configured-toplevel-path {:config {:test-data-toplevel-path "foo-bar"}} "test-data"))))
  (testing "should return keyword with file type as postfix"
    (is (= "foo-bar" (configured-toplevel-path {:config {:-toplevel-path "foo-bar"}} "")))))

(deftest ^:unit check-the-existence-of-files
  (let [toplevel-path "/tmp/subfolder"
        test-file "/tmp/subfolder/foo.bar"]
    (u/with-started [started (ts/test-system {:test-data-toplevel-path toplevel-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "check if local file exists"
                        (io/make-parents test-file)
                        (spit test-file "a")
                        (is (= true (cfh/cache-file-exists cfh "foo.bar"))))
                      (testing "check if local file does not exist"
                        (.delete (io/file test-file))
                        (is (= false (cfh/cache-file-exists cfh "foo.bar"))))))))

(deftest ^:unit if-cache-file-isnot-defined-it-should-not-exist
  (u/with-started [started (ts/test-system {})]
                  (let [cf-handler (:cachefile-handler started)]
                    (is (= false (cfh/cache-file-exists cf-handler "foo.bar"))))))

(deftest ^:unit check-reading-the-content-of-files
  (let [toplevel-path "/tmp/subfolder"
        test-file "/tmp/subfolder/foo.bar"
        crc-file "/tmp/subfolder/.foo.bar.crc"]
    (u/with-started [started (ts/test-system {:test-data-toplevel-path toplevel-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "reading content from a local file"
                        (.delete (io/file test-file))
                        (.delete (io/file crc-file))
                        (io/make-parents test-file)
                        (spit test-file "somevalue=foo")
                        (is (= "somevalue=foo" (cfh/slurp-cache-file cfh "foo.bar"))))))))

(deftest ^:unit check-writing-files
  (let [toplevel-path "/tmp/subfolder"
        file-path "/tmp/subfolder/foo.bar"
        crc-file "/tmp/subfolder/.foo.bar.crc"]
    (u/with-started [started (ts/test-system {:test-data-toplevel-path toplevel-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "writing a local file"
                        (.delete (io/file file-path))
                        (.delete (io/file crc-file))
                        (cfh/write-cache-file cfh "foo.bar" ["some-content"])
                        (is (= "some-content" (cfh/slurp-cache-file cfh "foo.bar"))))))))

(deftest ^:unit test-success-files
  (let [toplevel-path "/tmp/subfolder"
        file-path "/tmp/subfolder/_SUCCESS"
        crc-file "/tmp/subfolder/._SUCCESS.crc"]
    (u/with-started [started (ts/test-system {:test-data-toplevel-path toplevel-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "reading content from a local file"
                        (.delete (io/file file-path))
                        (.delete (io/file crc-file))
                        (is (= false (.exists (io/file file-path))))
                        (cfh/write-success-file cfh)
                        (is (= true (.exists (io/file file-path)))))))))

(deftest ^:unit check-writing-files-with-latest-generation
  (let [toplevel-path "/tmp/foo/{GENERATION}/subfolder"]
    (u/with-started [started (ts/test-system {:test-data-toplevel-path toplevel-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "should write a local file to a generation path"
                        (FileUtil/fullyDelete (File. "/tmp/foo"))
                        (is (= "/tmp/foo/000000/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :read)))
                        (is (= "/tmp/foo/000000/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :write)))
                        (cfh/write-cache-file cfh "foo.bar" ["some-content" "more-content"])
                        (cfh/write-success-file cfh)
                        (is (= "/tmp/foo/000000/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :read)))
                        (is (= "/tmp/foo/000001/subfolder/foo.bar" (cfh/build-file-path cfh "foo.bar" :write)))
                        (is (= ["some-content" "more-content"] (cfh/read-cache-file cfh "foo.bar" #(into [] (line-seq %))))))))))

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
  (let [toplevel-path "hdfs://{ZK_NAMENODE}/foo/bar"
        namenode (atom "first_namenode")]
    (with-redefs [cfh/namenode-from-zookeeper (fn [_] @namenode)]
      (u/with-started [started (ts/test-system {:test-data-toplevel-path toplevel-path})]
                      (let [cfh (:cachefile-handler started)]
                        (testing "check if zookeeper-namenode gets injected"
                          (is (= "hdfs://first_namenode/foo/bar/foo.bar" (cfh/build-file-path cfh "foo.bar" :read)))
                          (is (= "hdfs://first_namenode/foo/bar/foo.bar" (cfh/build-file-path cfh "foo.bar" :write))))
                        (reset! namenode "second_namenode")
                        (testing "check if zookeeper-namenode gets injected with fresh value"
                          (is (= "hdfs://second_namenode/foo/bar/foo.bar" (cfh/build-file-path cfh "foo.bar" :read)))
                          (is (= "hdfs://second_namenode/foo/bar/foo.bar" (cfh/build-file-path cfh "foo.bar" :write)))))))))
