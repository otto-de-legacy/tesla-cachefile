(ns de.otto.tesla.cachefile.strategy.generations
  (:require [hdfs.core :as hdfs]
            [clojure.tools.logging :as log]))

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

(defn- inject-generation [file-path generation]
  (clojure.string/replace file-path GENERATION generation))

(defn- success-file-present-for-path [file-path]
  (hdfs/exists? (str file-path "/_SUCCESS")))

(defn- success-file-present-for-generation [file-path current-generation]
  (success-file-present-for-path (inject-generation file-path current-generation)))

(defn- latest-with-success-file [file-path all-generations]
  (loop [generations (reverse all-generations)]
    (if-let [current-generation (first generations)]
      (if (success-file-present-for-generation file-path current-generation)
        current-generation
        (recur (rest generations))))))

(defn- increase-generation [latest-generation]
  (let [int-val (Integer/parseInt latest-generation)]
    (as-generation-string (inc int-val))))

(defn- new-generation [all-generations]
  (if-let [latest (last all-generations)]
    (increase-generation latest)
    DEFAULT_GENERATION))

(defn- generation-for [file-path read-or-write all-generations]
  (case read-or-write
    :read (latest-with-success-file file-path all-generations)
    :write (new-generation all-generations)))

(defn- replace-generation-placholder [path read-or-write]
  (some->> path
           (parentpath-of-generation-placeholder)
           (all-generations)
           (generation-for path read-or-write)
           (inject-generation path)))

(defn- still-keeping-things [kept nr-to-keep]
  (not (= kept nr-to-keep)))

(defn- delete [path]
  (log/info "deleting path:" path)
  (hdfs/delete path))

(defn- all-generation-paths-sorted [toplevel-path]
  (let [all-gens (all-generations (parentpath-of-generation-placeholder toplevel-path))]
    (map #(inject-generation toplevel-path %) (reverse (sort all-gens)))))

(defn cleanup-generations! [toplevel-path nr-to-keep]
  (loop [all-paths (all-generation-paths-sorted toplevel-path)
         kept 0]
    (when-let [current-path (first all-paths)]
      (if (still-keeping-things kept nr-to-keep)
        (if (success-file-present-for-path current-path)
          (recur (rest all-paths) (inc kept))
          (recur (rest all-paths) kept))
        (do
          (delete current-path)
          (recur (rest all-paths) kept))))))

(defn should-cleanup-generations? [nr-gens-to-keep toplevel-path]
  (and
    (not (nil? nr-gens-to-keep))
    (.contains toplevel-path GENERATION)))

(defn with-hdfs-generation [path read-or-write]
  (if (.contains path GENERATION)
    (replace-generation-placholder path read-or-write)
    path))
