(ns de.otto.tesla.cachefile.utils.hdfs-helpers
  (:require [hdfs.core :as hdfs]))

(defn write-file [path lines]
            (hdfs/make-parents path)
            (hdfs/write-lines path lines))

(defn read-file [path read-fn]
           (with-open [rdr (hdfs/buffered-reader path)]
             (read-fn rdr)))

(defn slurp-file [path]
            (read-file path #(clojure.string/join \newline (line-seq %))))

(defn file-exists [path]
             (hdfs/exists? path))