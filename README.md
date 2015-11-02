# tesla-cachefile

An addon to [tesla-microservice](https://github.com/otto-de/tesla-microservice)
to use a cachefile locally or on hdfs.
In case of hdfs, the namenode can be automatically determined by querying a zookeeper every time the cache-file is read or written.

[![Build Status](https://travis-ci.org/otto-de/tesla-cachefile.svg)](https://travis-ci.org/otto-de/tesla-cachefile)

## Usage

Add this to your project's dependencies:

`[de.otto/tesla-cachefile "0.0.9"]`

From version `0.0.5` tesla-cachefile needs version `0.1.4` or later of tesla-zookeeper-observer
Version `0.0.9` has some major changes: 

   * the property `hdfs.namenode` has been removed. The namenode is now configured directly in the cache-file-path
   * you can use `{ZK_NAMENODE}` in your cache-file-path to determine the namenode from zookeeper
   * you can use `{GENERATION}` in your cache-file-path to read from the latest generation with the cache-file present and
     write to the latest generation if cache-file absent or otherwise to a new generation

The module, if used within a system, can be accessed using this protocol:

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
Add `cache.file` to your properties pointing to e.g. `hdfs://namenode:port/some/hdfs.cachefile`

### cachefile with generations
Add `cache.file` to your properties pointing to e.g. `hdfs://namenode:port/some/{GENERATION}/your.cachefile`

#### Configuring a namenode via zookeeper
Add `cache.file` to your properties pointing to e.g. `hdfs://{ZK_NAMENODE}/some/hdfs.cachefile`
Add `zookeeper.connect` to your properties containing a valid zookeeper connection string.
The module is currently looking for a namenode-string at a zk-node called `/hadoop-ha/hadoop-ha/ActiveBreadCrumb`.

## Initial Contributors

Christian Stamm, Kai Brandes, Daley Chetwynd, Carl Düvel, Florian Weyandt

## License

Apache License
