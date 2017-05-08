(defproject de.otto/tesla-cachefile "0.4.5-SNAPSHOT"
  :description "Addon to https://github.com/otto-de/tesla-microservice to use a cachefile locally or on hdfs."
  :url "https://github.com/otto-de/tesla-cachefile"
  :license {:name "Apache License 2.0"
            :url  "http://www.apache.org/license/LICENSE-2.0.html"}
  :scm {:name "git"
        :url  "https://github.com/otto-de/tesla-cachefile"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.stuartsierra/component "0.3.2"]
                 [de.otto/tesla-zookeeper-observer "0.1.5"
                  :exclusions [log4j/log4j]]
                 [overtone/at-at "1.2.0"]
                 [metrics-clojure "2.7.0"]
                 [org.slf4j/slf4j-api "1.7.21"]
                 [org.clojure/core.async "0.2.395"]
                 [org.slf4j/jcl-over-slf4j "1.7.21"]
                 [org.apache.hadoop/hadoop-common "2.6.0"
                  :exclusions [commons-logging/commons-logging log4j/log4j org.slf4j/slf4j-log4j12]]
                 [org.apache.hadoop/hadoop-hdfs "2.6.0"
                  :exclusions [commons-logging/commons-logging log4j/log4j org.slf4j/slf4j-log4j12]]
                 [hdfs-clj "0.1.15"]]
  :target-path "target/%s"
  :lein-release {:deploy-via :clojars}
  :test-paths ["test" "test-resources"]
  :profiles {:dev     {:dependencies [[de.otto/tesla-microservice "0.6.0"]
                                      [ch.qos.logback/logback-core "1.1.8"]
                                      [ch.qos.logback/logback-classic "1.1.8"]]

                       :plugins      [[lein-ancient "0.5.4"] [lein-release/lein-release "1.0.9"]]}
             :uberjar {:aot :all}})
