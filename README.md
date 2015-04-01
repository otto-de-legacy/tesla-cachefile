# tesla-cachefile

An addon to [tesla-microservice](https://github.com/otto-de/tesla-microservice)
to use a cachefile locally or on hdfs.
In case of hdfs, the namenode can be manually configured or automatically determined by querying a zookeeper.

## Usage

Add this to your project's dependencies:

`[de.otto/tesla-cachefile "0.1.1"]`

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

Christian Stamm, Kai Brandes, Daley Chetwynd, Felix Bechstein, Ralf Sigmund, Florian Weyandt

## License

Apache License
