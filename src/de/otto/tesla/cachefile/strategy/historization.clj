(ns de.otto.tesla.cachefile.strategy.historization
  (:require [hdfs.core :as hdfs]
            [de.otto.status :as s]
            [clojure.tools.logging :as log])
  (:import (java.io BufferedWriter OutputStreamWriter)
           (org.joda.time DateTimeZone DateTime)
           (java.util UUID)))

(defn- writer-entry? [c]
  (when (map? c)
    (if-let [writer-values (vals (select-keys c #{:file-path :writer :last-access}))]
      (every? #(not (nil? %)) writer-values))))

(defn- ts->time-map [millis]
  (when millis
    (let [date-time (DateTime. millis (DateTimeZone/getDefault))]
      {:month (.getMonthOfYear date-time)
       :day   (.getDayOfMonth date-time)
       :year  (.getYear date-time)
       :hour  (.getHourOfDay date-time)})))

(defn time->path [{:keys [year month day hour]}]
  [year month day hour])

(defn- find-all-writers
  ([writers]
   (find-all-writers [] writers))
  ([_ node]
   (if (writer-entry? node)
     node
     (flatten (map (fn [[_ sub-writers]]
                     (find-all-writers sub-writers)) node)))))

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
  (let [file-path (output-file-path output-path the-time)]
    {:writer      (new-print-writer file-path)
     :path        (time->path the-time)
     :write-count 0
     :file-path   file-path
     :last-access (current-time)}))

(defn touch-writer [writer]
  (-> writer
      (assoc :last-access (current-time))
      (update :write-count inc)))

(defn write-line! [{:keys [writer] :as writer-map} msg]
  (doto writer
    (.write msg)
    (.newLine))
  writer-map)

(defn store-writer [{:keys [path] :as writer} writers]
  (swap! writers assoc-in path writer)
  writer)

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

(defn lookup-writer-or-create [output-path writers millis]
  (when-let [the-time (ts->time-map millis)]
    (or
      (get-in @writers (time->path the-time))
      (create-new-writer output-path the-time))))

(defn- without-writer-object [c]
  (if (writer-entry? c)
    (dissoc c :writer :path)
    c))

(defn historization-status-fn [writers which-historizer]
  (s/status-detail
    (keyword which-historizer) :ok "all ok"
    {:writers (clojure.walk/prewalk without-writer-object @writers)}))
