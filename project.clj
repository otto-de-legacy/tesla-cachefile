(defproject de.otto/tesla-cachefile "0.0.5"
  :description "Addon to https://github.com/otto-de/tesla-microservice to use a cachefile locally or on hdfs."
  :url "https://github.com/otto-de/tesla-cachefile"
  :license {:name "Apache License 2.0"
            :url  "http://www.apache.org/license/LICENSE-2.0.html"}
  :scm {:name "git"
        :url  "https://github.com/otto-de/tesla-cachefile"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.stuartsierra/component "0.2.3"]
                 [de.otto/tesla-zookeeper-observer "0.1.5"]

                 [commons-codec "1.6"]
                 
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/jcl-over-slf4j "1.7.12"]
                 [org.apache.hadoop/hadoop-hdfs "2.2.0"
                  :exclusions [[javax.servlet.jsp/jsp-api]
                               [javax.servlet/servlet-api]
                               [tomcat/jasper-runtime]
                               [commons-logging/commons-logging]]]

                 [org.apache.hadoop/hadoop-common "2.2.0"
                  :exclusions [[org.slf4j/slf4j-log4j12]
                               [javax.servlet/servlet-api]]]]
  :target-path "target/%s"
  :profiles {:dev {:dependencies [ [de.otto/tesla-microservice "0.1.17"]]
                   :plugins [[lein-ancient "0.5.4"]]}
             :uberjar {:aot :all}})
