(ns de.otto.tesla.cachefile.utils.reading-properties-test
  (:require [clojure.test :refer :all]
            [de.otto.tesla.cachefile.utils.reading-properties :as rpr]))

(def configured-toplevel-path #'rpr/configured-toplevel-path)
(def configured-max-writer-age #'rpr/configured-max-writer-age)
(def configured-schedule-closing-time #'rpr/configured-schedule-closing-time)

(deftest ^:unit check-reading-properties
  (testing "should return keyword without postfix if file type is missing"
    (is (= "foo-bar" (configured-toplevel-path {:config {:test-data-toplevel-path "foo-bar"}} "test-data"))))
  (testing "should return keyword with file type as postfix"
    (is (= "foo-bar" (configured-toplevel-path {:config {:-toplevel-path "foo-bar"}} ""))))
  (testing "should return the configured max writer age"
    (is (= 5000 (configured-max-writer-age {:config {:foo-max-writer-age "5000"}} "foo"))))
  (testing "should return the default max writer age"
    (is (= (* 1000 60 5) (configured-max-writer-age {:config {}} "foo"))))
  (testing "should return the configured schedule closing time"
    (is (= 1000 (configured-schedule-closing-time {:config {:foo-schedule-closing-time "1000"}} "foo"))))
  (testing "should return the default schedule closing time"
    (is (= (* 1000 60 2) (configured-schedule-closing-time {:config {}} "foo")))))
