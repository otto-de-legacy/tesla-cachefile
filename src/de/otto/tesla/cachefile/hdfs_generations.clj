(ns de.otto.tesla.cachefile.hdfs-generations
  (:require [hdfs.core :as hdfs]))

(def LATEST_GENERATION "{LATEST_GENERATION}")

(defn- parent-of-latest-generation [file-path]
  (let [occurence (.indexOf file-path LATEST_GENERATION)]
    (if (>= occurence 0)
      (.substring file-path 0 occurence))))

(defn- all-generations [parent-path]
  (if (hdfs/exists? parent-path)
    (let [status (hdfs/list-file-status parent-path)]
      (sort (map #(.getName (.getPath %)) status)))
    []))

(defn- latest-generation [all-generations]
  (or
    (last all-generations)
    "000000"))

(defn- replace-latest-generation-placholder
  ([file-path latest-generation]
   (clojure.string/replace file-path LATEST_GENERATION latest-generation))
  ([file-path]
   (or
     (some->> file-path
              (parent-of-latest-generation)
              (all-generations)
              (latest-generation)
              (replace-latest-generation-placholder file-path))
     file-path)))

(defn inject-latest-hdfs-generation [cache-file]
  (if (.contains cache-file LATEST_GENERATION)
    (replace-latest-generation-placholder cache-file)
    cache-file))

