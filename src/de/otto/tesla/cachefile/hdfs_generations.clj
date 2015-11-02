(ns de.otto.tesla.cachefile.hdfs-generations
  (:require [hdfs.core :as hdfs]))

(defn- generation-val-in-range [generation-as-int]
  (if (and
        (<= generation-as-int 999999)
        (>= generation-as-int 0))
    generation-as-int
    0))

(defn- as-generation-string [int-val]
  (let [int-val-in-range (generation-val-in-range int-val)
        nr-of-leading-zeros (- 6 (count (str int-val-in-range)))
        leading-zeros (apply str (repeat nr-of-leading-zeros "0"))]
    (format "%s%d" leading-zeros int-val-in-range)))

(def GENERATION "{GENERATION}")
(def DEFAULT_GENERATION (as-generation-string 0))

(defn- parent-of-latest-generation [file-path]
  (let [occurence (.indexOf file-path GENERATION)]
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
    DEFAULT_GENERATION))

(defn- inject-generation [file-path generation]
  (clojure.string/replace file-path GENERATION generation))

(defn- file-present-for-generation [file-path current-generation]
  (hdfs/exists? (inject-generation file-path current-generation)))

(defn- latest-with-file-present [file-path all-generations]
  (loop [generations (reverse all-generations)]
    (if-let [current-generation (first generations)]
      (if (file-present-for-generation file-path current-generation)
        current-generation
        (recur (rest generations)))
      DEFAULT_GENERATION)))

(defn- increase-generation [latest-generation]
  (let [int-val (Integer/parseInt latest-generation)]
    (as-generation-string (inc int-val))))

(defn- latest-if-file-absent-or-new [file-path all-generations]
  (let [latest-generation (latest-generation all-generations)]
    (if-not (file-present-for-generation file-path latest-generation)
      latest-generation
      (increase-generation latest-generation))))

(defn- generation-for [file-path read-or-write all-generations]
  (case read-or-write
    :read (latest-with-file-present file-path all-generations)
    :write (latest-if-file-absent-or-new file-path all-generations)))

(defn- replace-generation-placholder [file-path read-or-write]
  (or
    (some->> file-path
             (parent-of-latest-generation)
             (all-generations)
             (generation-for file-path read-or-write)
             (inject-generation file-path))
    file-path))

(defn inject-latest-hdfs-generation [cache-file read-or-write]
  (if (.contains cache-file GENERATION)
    (replace-generation-placholder cache-file read-or-write)
    cache-file))

