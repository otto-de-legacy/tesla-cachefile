(ns de.otto.tesla.cachefile.hdfs-helpers-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.hdfs-helpers :as hlps]
            [de.otto.tesla.util.test-utils :as u]
            [de.otto.tesla.cachefile.test-system :as ts]
            [clojure.java.io :as io]))

(deftest ^:unit check-the-existence-of-files
  (let [test-file "/tmp/subfolder/foo.bar"]
    (testing "check if local file exists"
      (io/make-parents test-file)
      (spit test-file "a")
      (is (= true (hlps/file-exists test-file))))
    (testing "check if local file does not exist"
      (.delete (io/file test-file))
      (is (= false (hlps/file-exists test-file))))))

(deftest ^:unit check-slurping-files
  (let [test-file "/tmp/subfolder/foo.bar"
        crc-file "/tmp/subfolder/.foo.bar.crc"]
    (testing "reading content from a local file"
      (.delete (io/file test-file))
      (.delete (io/file crc-file))
      (io/make-parents test-file)
      (spit test-file "somevalue=foo")
      (is (= "somevalue=foo" (hlps/slurp-file test-file))))
    (testing "writing a local file"
      (.delete (io/file test-file))
      (.delete (io/file crc-file))
      (hlps/write-file test-file ["some-content"])
      (is (= "some-content" (hlps/slurp-file test-file))))))


