(defproject de.otto/tesla-cachefile "0.5.2-SNAPSHOT"
  :description "Addon to https://github.com/otto-de/tesla-microservice to use a cachefile locally or on hdfs."
  :url "https://github.com/otto-de/tesla-cachefile"
  :license {:name "Apache License 2.0"
            :url  "http://www.apache.org/license/LICENSE-2.0.html"}
  :scm {:name "git"
        :url  "https://github.com/otto-de/tesla-cachefile"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.stuartsierra/component "0.3.2"]
                 [de.otto/tesla-zookeeper-observer "0.2.1"
                  :exclusions [log4j/log4j]]
                 [overtone/at-at "1.2.0"]
                 [metrics-clojure "2.10.0"]
                 [org.slf4j/slf4j-api "1.7.25"]
                 [org.clojure/core.async "0.4.474"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]
                 [org.apache.hadoop/hadoop-common "2.9.0"
                  :exclusions [commons-logging/commons-logging log4j/log4j org.slf4j/slf4j-log4j12 org.mortbay.jetty/jetty org.mortbay.jetty/jetty-util org.mortbay.jetty/jetty-sslengine]]
                 [org.apache.hadoop/hadoop-hdfs "2.9.0"
                  :exclusions [commons-logging/commons-logging log4j/log4j org.slf4j/slf4j-log4j12 org.mortbay.jetty/jetty org.mortbay.jetty/jetty-util org.mortbay.jetty/jetty-sslengine]]
                 [hdfs-clj "0.1.16"]
                 ;override stuff because of security vulnerabilities
                 [com.fasterxml.jackson.core/jackson-databind "2.9.4"]
                 [commons-beanutils/commons-beanutils "1.9.3"]
                 [com.nimbusds/nimbus-jose-jwt "4.39.2"]
                 [com.squareup.okhttp/okhttp "2.7.5"]
                 [io.netty/netty-all "4.0.54.Final"]
                 ]
  :target-path "target/%s"
  :lein-release {:deploy-via :clojars}
  :test-paths ["test" "test-resources"]
  :profiles {:dev     {:dependencies [[de.otto/tesla-microservice "0.11.25"]
                                      [ch.qos.logback/logback-core "1.2.3"]
                                      [ch.qos.logback/logback-classic "1.2.3"]]

                       :plugins      [[lein-release/lein-release "1.0.9"]]}
             :uberjar {:aot :all}})
