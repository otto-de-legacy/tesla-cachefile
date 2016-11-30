(defproject de.otto/tesla-cachefile "0.4.3"
  :description "Addon to https://github.com/otto-de/tesla-microservice to use a cachefile locally or on hdfs."
  :url "https://github.com/otto-de/tesla-cachefile"
  :license {:name "Apache License 2.0"
            :url  "http://www.apache.org/license/LICENSE-2.0.html"}
  :scm {:name "git"
        :url  "https://github.com/otto-de/tesla-cachefile"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.stuartsierra/component "0.3.0"]
                 [de.otto/tesla-zookeeper-observer "0.1.5"]
                 [overtone/at-at "1.2.0"]
                 [metrics-clojure "2.6.1"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.clojure/core.async "0.2.374"]
                 [org.slf4j/jcl-over-slf4j "1.7.12"]
                 [org.apache.hadoop/hadoop-client "2.7.1"]
                 [hdfs-clj "0.1.15"]]
  :target-path "target/%s"
  :lein-release {:deploy-via :clojars}
  :test-paths ["test" "test-resources"]
  :profiles {:dev     {:dependencies [[de.otto/tesla-microservice "0.5.0"]
                                      [org.slf4j/slf4j-api "1.7.16"]
                                      [ch.qos.logback/logback-core "1.1.5"]
                                      [ch.qos.logback/logback-classic "1.1.5"]]

                       :plugins      [[lein-ancient "0.5.4"] [lein-release/lein-release "1.0.9"]]}
             :uberjar {:aot :all}})
