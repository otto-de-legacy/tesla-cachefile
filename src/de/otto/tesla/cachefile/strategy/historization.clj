(ns de.otto.tesla.cachefile.strategy.historization
  (:require [hdfs.core :as hdfs]
            [de.otto.status :as s]
            [clojure.tools.logging :as log])
  (:import (java.io BufferedWriter OutputStreamWriter)
           (org.joda.time DateTimeZone DateTime)
           (java.util UUID)))

(defn- ts->time-map [millis]
  (when millis
    (let [date-time (DateTime. millis (DateTimeZone/getDefault))]
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

(defn- new-print-writer ^BufferedWriter [file-path]
  (BufferedWriter. (OutputStreamWriter. (hdfs/output-stream file-path))))

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

(defn- close-single-writer! [writer]
  (doto writer (.flush) (.close)))

(defn close-writers!
  ([writers]
   (close-writers! writers (constantly true)))
  ([writers close-writer?]
   (let [all-writers (find-all-writers @writers)]
     (doseq [{:keys [path writer]} (filter close-writer? all-writers)]
       (try
         (close-single-writer! writer)
         (swap! writers assoc-in path nil)
         (catch Exception e
           (log/error e "Error occured when closing and flushing writer in: " path)))))))

(defn close-old-writers! [writers max-writer-age]
  (close-writers! writers (partial writer-too-old? max-writer-age)))

(defn lookup-writer-or-create! [output-path writers millis]
  (when-let [the-time (ts->time-map millis)]
    (or
      (load-and-update-existing-writer writers the-time)
      (create-and-store-new-writer output-path writers the-time))))

(defn- is-a-writer-entry? [c]
  (and (map? c) (= (into #{} (keys c)) #{:file-path :writer :last-access})))

(defn- without-writer-object [c]
  (if (is-a-writer-entry? c)
    (dissoc c :writer)
    c))

(defn historization-status-fn [writers which-historizer]
  (s/status-detail
    (keyword which-historizer) :ok "all ok"
    {:writers (clojure.walk/prewalk without-writer-object @writers)}))
