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

(defn- parentpath-of-generation-placeholder [file-path]
  (let [occurence (.indexOf file-path GENERATION)]
    (if (>= occurence 0)
      (.substring file-path 0 occurence))))

(defn- is-generation? [file-name]
  (not (nil? (re-matches #"\d+" file-name))))

(defn- all-generations [parent-path]
  (if (hdfs/exists? parent-path)
    (->> (hdfs/list-file-status parent-path)
         (map #(.getName (.getPath %)))
         (sort)
         (filter is-generation?))
    []))

(defn- latest-generation [all-generations]
  (or
    (last all-generations)
    DEFAULT_GENERATION))

(defn- inject-generation [file-path generation]
  (clojure.string/replace file-path GENERATION generation))

(defn- success-file-present-for-generation [file-path current-generation]
  (hdfs/exists? (str (inject-generation file-path current-generation) "/_SUCCESS")))

(defn- latest-with-success-file [file-path all-generations]
  (loop [generations (reverse all-generations)]
    (if-let [current-generation (first generations)]
      (if (success-file-present-for-generation file-path current-generation)
        current-generation
        (recur (rest generations)))
      DEFAULT_GENERATION)))

(defn- increase-generation [latest-generation]
  (let [int-val (Integer/parseInt latest-generation)]
    (as-generation-string (inc int-val))))

(defn- latest-if-success-file-absent-or-new [file-path all-generations]
  (let [latest (latest-generation all-generations)]
    (if-not (success-file-present-for-generation file-path latest)
      latest
      (increase-generation latest))))

(defn- generation-for [file-path read-or-write all-generations]
  (case read-or-write
    :read (latest-with-success-file file-path all-generations)
    :write (latest-if-success-file-absent-or-new file-path all-generations)))

(defn- replace-generation-placholder [path read-or-write]
  (or
    (some->> path
             (parentpath-of-generation-placeholder)
             (all-generations)
             (generation-for path read-or-write)
             (inject-generation path))
    path))

(defn inject-hdfs-generation [path read-or-write]
  (if (.contains path GENERATION)
    (replace-generation-placholder path read-or-write)
    path))

