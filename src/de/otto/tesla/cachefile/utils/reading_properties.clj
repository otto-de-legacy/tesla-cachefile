(ns de.otto.tesla.cachefile.utils.reading-properties)

(defn configured-nr-generations-to-keep [config which-data]
  (if-let [gens-to-keep (get-in config [:config (keyword (str which-data "-nr-gens-to-keep"))])]
    (Integer/parseInt gens-to-keep)))

(defn configured-toplevel-path [config which-data]
  (get-in config [:config (keyword (str which-data "-toplevel-path"))]))

(defn configured-schedule-closing-time [config which-historizer]
  (if-let [schedule-closing-time (get-in config [:config (keyword (str which-historizer "-schedule-closing-time"))])]
    (Integer/parseInt schedule-closing-time)
    (* 1000 60 2)))

(defn configured-max-writer-age [config which-historizer]
  (if-let [max-writer-age (get-in config [:config (keyword (str which-historizer "-max-writer-age"))])]
    (Integer/parseInt max-writer-age)
    (* 1000 60 5)))
