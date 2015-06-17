# tesla-cachefile

An addon to [tesla-microservice](https://github.com/otto-de/tesla-microservice)
to use a cachefile locally or on hdfs.
In case of hdfs, the namenode can be manually configured or automatically determined by querying a zookeeper.

[![Build Status](https://travis-ci.org/otto-de/tesla-cachefile.svg)](https://travis-ci.org/otto-de/tesla-cachefile)

## Usage

Add this to your project's dependencies:

`[de.otto/tesla-cachefile "0.0.5"]`

the module, if used within a system, can be accessed using this protocol:

```
(defprotocol CfAccess
 (read-cache-file [self])
  (write-cache-file [self content])
  (cache-file-exists [self])
  (cache-file-defined [self]))
```

### Local cachefile
Add `cache.file` to your properties pointing to e.g. `/tmp/local.cachefile`

### HDFS cachefile
Add `cache.file` to your properties pointing to e.g. `hdfs://some/hdfs.cachefile`

#### Configuring a namenode manually
Add `hdfs.namenode` to your properties pointing to e.g. `hdfs://some.namenode:port`

#### Configuring a namenode via zookeeper
Add `hdfs.namenode` to your properties pointing to `zookeeper`
Add `zookeeper.connect` to your properties containing a valid zookeeper connection string.
The module is currently looking for a namenode-string at a zk-node called `/hadoop-ha/hadoop-ha/ActiveBreadCrumb`.

## Initial Contributors

Christian Stamm, Kai Brandes, Daley Chetwynd, Carl Düvel, Florian Weyandt

## License

Apache License
