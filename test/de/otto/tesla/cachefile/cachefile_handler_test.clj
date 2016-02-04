(ns de.otto.tesla.cachefile.cachefile-handler-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.cachefile-handler :as cfh]
            [clojure.java.io :as io]
            [de.otto.tesla.cachefile.utils.zk-namenode :as zknn]
            [de.otto.tesla.cachefile.utils.test-utils :as u]
            [de.otto.tesla.cachefile.utils.hdfs-helpers :as hlps]
            [de.otto.tesla.system :as system]
            [com.stuartsierra.component :as c]
            [de.otto.tesla.zk.zk-observer :as zk])
  (:import (org.apache.hadoop.hdfs.server.namenode.ha.proto HAZKInfoProtos$ActiveNodeInfo)
           (org.apache.hadoop.fs FileUtil)
           (java.io File)))

(defn test-system [runtime-conf]
  (-> (system/base-system runtime-conf)
      (assoc :zookeeper (c/using (zk/new-zkobserver) [:config]))
      (assoc :cachefile-handler (c/using (cfh/new-cachefile-handler "test-data") [:config :zookeeper]))))

(deftest ^:unit test-success-files
  (let [toplevel-path "/tmp/subfolder"
        success-file "/tmp/subfolder/_SUCCESS"
        crc-file "/tmp/subfolder/._SUCCESS.crc"]
    (u/with-started [started (test-system {:test-data-toplevel-path toplevel-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "reading content from a local file"
                        (.delete (io/file success-file))
                        (.delete (io/file crc-file))
                        (is (= false (.exists (io/file success-file))))
                        (cfh/write-success-file cfh toplevel-path)
                        (is (= true (.exists (io/file success-file)))))))))

(deftest ^:unit check-writing-files-with-latest-generation
  (let [toplevel-path "/tmp/foo/{GENERATION}/subfolder"]
    (u/with-started [started (test-system {:test-data-toplevel-path toplevel-path})]
                    (let [cfh (:cachefile-handler started)]
                      (testing "should write a local file to a generation path"
                        (FileUtil/fullyDelete (File. "/tmp/foo"))
                        (is (= nil (cfh/folder-to-read-from cfh)))
                        (let [generation-to-write-to (cfh/folder-to-write-to cfh)
                              a-file-path (str generation-to-write-to "foo.bar")]
                          (is (= "/tmp/foo/000000/subfolder" generation-to-write-to))
                          (hlps/write-file a-file-path ["some-content" "more-content"])
                          (is (= ["some-content" "more-content"]
                                 (hlps/read-file a-file-path #(into [] (line-seq %)))))
                          (cfh/write-success-file cfh generation-to-write-to))
                        (is (= "/tmp/foo/000001/subfolder" (cfh/folder-to-write-to cfh)))
                        (is (= "/tmp/foo/000000/subfolder" (cfh/folder-to-read-from cfh))))))))

(def parse-hostname #'zknn/parse-hostname)
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
    (with-redefs [zknn/namenode-from-zookeeper (fn [_] @namenode)]
      (u/with-started [started (test-system {:test-data-toplevel-path toplevel-path})]
                      (let [cfh (:cachefile-handler started)]
                        (testing "check if zookeeper-namenode gets injected"
                          (is (= "hdfs://first_namenode/foo/bar" (cfh/folder-to-read-from cfh)))
                          (is (= "hdfs://first_namenode/foo/bar" (cfh/folder-to-write-to cfh))))
                        (reset! namenode "second_namenode")
                        (testing "check if zookeeper-namenode gets injected with fresh value"
                          (is (= "hdfs://second_namenode/foo/bar" (cfh/folder-to-read-from cfh)))
                          (is (= "hdfs://second_namenode/foo/bar" (cfh/folder-to-write-to cfh)))))))))
