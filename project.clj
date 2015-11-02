(defproject de.otto/tesla-cachefile "0.1.0"
            :description "Addon to https://github.com/otto-de/tesla-microservice to use a cachefile locally or on hdfs."
            :url "https://github.com/otto-de/tesla-cachefile"
            :license {:name "Apache License 2.0"
                      :url  "http://www.apache.org/license/LICENSE-2.0.html"}
            :scm {:name "git"
                  :url  "https://github.com/otto-de/tesla-cachefile"}
            :dependencies [[org.clojure/clojure "1.7.0"]
                           [com.stuartsierra/component "0.3.0"]
                           [de.otto/tesla-zookeeper-observer "0.1.5"]

                           [org.slf4j/slf4j-api "1.7.12"]
                           [org.slf4j/jcl-over-slf4j "1.7.12"]
                           [org.apache.hadoop/hadoop-client "2.7.1"]
                           [hdfs-clj "0.1.15"]
                           ]
            :target-path "target/%s"
            :profiles {:dev     {:dependencies [[de.otto/tesla-microservice "0.1.18"]]
                                 :plugins      [[lein-ancient "0.5.4"]]}
                       :uberjar {:aot :all}})
