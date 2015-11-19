(ns de.otto.tesla.cachefile.strategy.historization
  (:require [hdfs.core :as hdfs])
  (:import (java.io PrintWriter)
           (org.joda.time DateTimeZone DateTime)
           (java.util UUID)
           (java.util.zip GZIPOutputStream)))

(defn- ts->time-map [millis]
  (when millis
    (let [date-time (DateTime. millis (DateTimeZone/forID "Europe/Berlin"))]
      {:month (.getMonthOfYear date-time)
       :day   (.getDayOfMonth date-time)
       :year  (.getYear date-time)
       :hour  (.getHourOfDay date-time)})))

(defn- find-all-writers
  ([writers]
   (find-all-writers [] writers))
  ([path writers]
   (if (:writer writers)
     (assoc writers :path path)
     (flatten (map (fn [[parent-path sub-writers]]
                     (find-all-writers (conj path parent-path) sub-writers)) writers)))))

(defn- current-time []
  (System/currentTimeMillis))

(defn- writer-too-old? [max-age {:keys [last-access] :as w}]
  (let [writer-age (- (current-time) last-access)]
    (>= writer-age max-age)))

(defn- unique-id []
  (str (UUID/randomUUID)))

(defn- output-file-path [output-path {:keys [year month day hour]}]
  (str output-path "/" year "/" month "/" day "/" hour "/" (unique-id) ".hist.gz"))

(defn- new-print-writer ^PrintWriter [file-path]
  (PrintWriter. (GZIPOutputStream. (hdfs/output-stream file-path))))

(defn- create-new-writer [output-path the-time]
  (let [file-path (output-file-path output-path the-time)
        writer-map {:writer      (new-print-writer file-path)
                    :file-path   file-path
                    :last-access (current-time)}]
    writer-map))

(defn- store-writer [writers {:keys [year month day hour]} writer]
  (swap! writers assoc-in [year month day hour] writer)
  writer)

(defn- load-and-update-existing-writer [writers {:keys [year month day hour] :as the-time}]
  (when-let [writer-map (get-in @writers [year month day hour])]
    (store-writer writers the-time
                  (assoc writer-map :last-access (current-time)))))

(defn- create-and-store-new-writer [output-path writers the-time]
  (store-writer writers the-time
                (create-new-writer output-path the-time)))

(defn close-writers!
  ([writers]
   (close-writers! writers (constantly true)))
  ([writers close-writer?]
   (let [all-writers (find-all-writers @writers)]
     (doseq [{:keys [path writer]} (filter close-writer? all-writers)]
       (doto writer (.flush) (.close))
       (swap! writers assoc-in path nil)))))

(defn close-old-writers! [writers max-writer-age]
  (close-writers! writers (partial writer-too-old? max-writer-age)))

(defn lookup-writer-or-create! [output-path writers millis]
  (if-let [the-time (ts->time-map millis)]
    (or
      (load-and-update-existing-writer writers the-time)
      (create-and-store-new-writer output-path writers the-time))))