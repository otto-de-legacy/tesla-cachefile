# tesla-cachefile

An addon to [tesla-microservice](https://github.com/otto-de/tesla-microservice)
to use a cachefile locally or on hdfs.
In case of hdfs, the namenode can be automatically determined by querying a zookeeper every time the cache-file is read or written.

[![Build Status](https://travis-ci.org/otto-de/tesla-cachefile.svg)](https://travis-ci.org/otto-de/tesla-cachefile)
[![Dependencies Status](http://jarkeeper.com/otto-de/tesla-vault/status.svg)](http://jarkeeper.com/otto-de/tesla-cachefile)

## Usage

Add this to your project's dependencies:

[![Clojars Project](http://clojars.org/de.otto/tesla-cachefile/latest-version.svg)](http://clojars.org/de.otto/tesla-cachefile)

## Changelog

Version `0.3.3` changes:
   * bugfix: history files were compressed twice


Version `0.3.2` changes:
   * historization-strategy creates gzipped files


Version `0.3.0` changes:
   * Refactoring of repo-structure
   * Added a historization-strategy, which is used by the new `file-historizer` component

Version `0.2.0` changes:
   * Complete redesign of the API. The Filesystem is now treated as an immutable resource.

Version `0.1.2` changes: 
   
   * added the possibility to cleanup hdfs-generations: `(cleanup-generations [self])`.   
     To make it work, you have to define a property called `your.name.nr.gens.to.keep` which 
     specifies the number of generations with success-files to keep.   
     The delete is executed for the path you have specified by `your.name.toplevel.path` with the corresponding generations injected
     e.g. `hdfs://namenode:1234/foo/bar/000001/subfolder`
   
Version `0.1.0` has some major changes: 
   
   * a folder is now configured by the property `your.name.toplevel.path` (`{ZK_NAMENODE}` and `{GENERATION}` can be used)
   * many files can now been written to the folder configured
   * generation-logic now works based on `_SUCCESS`-files: Read from latest generation with `_SUCCESS`-file + 
     write to latest generation if `_SUCCESS`-file is not present or otherwise create and write to new generation

Version `0.0.10` has some api-changes: 

   * write-cache-file now takes a line-seq as input
   * read-cache-file now takes an additional argument (read-fn), which is a function to accept a BufferedReader
   * slurp-cache-file has the old behaviour of getting the file's content as one big string. 

Version `0.0.9` has some major changes: 

   * the property `hdfs.namenode` has been removed. The namenode is now configured directly in the cache-file-path
   * you can use `{ZK_NAMENODE}` in your cache-file-path to determine the namenode from zookeeper
   * you can use `{GENERATION}` in your cache-file-path to read from the latest generation with the cache-file present and
     write to the latest generation if cache-file absent or otherwise to a new generation
   * uses [hdfs-clj "0.1.15"]


## Cachefile-Handler component
The component, if used within a system, can be accessed using this protocol:

            (defprotocol GenerationHandling
              (folder-to-write-to [self] "Creates new generation directory and returns the path.")
              (folder-to-read-from [self] "Finds newest generation wit a success file and returns the path.")
              (write-success-file [self path] "Creates a file named _SUCCESS in the given , which is a marker for the other functions of this protocol")
              (cleanup-generations [self] "Determines n last successful generations and deletes any older generation."))

### Local cachefile
Add `your.name.toplevel.path` to your properties pointing to e.g. `/tmp/yourfolder`  
`your.name` is defined when adding the CacheFileHandler to your system:

    (assoc :cachefile-handler (c/using (cfh/new-cachefile-handler "your-name") [:config :zookeeper]))

### HDFS cachefile
Add `your.name.toplevel.path` to your properties pointing to e.g. `hdfs://namenode:port/some/folder`

### cachefile with generations
Add `your.name.toplevel.path` to your properties pointing to e.g. `hdfs://namenode:port/some/{GENERATION}/folder`

#### Configuring a namenode via zookeeper
Add `your.name.toplevel.path` to your properties pointing to e.g. `hdfs://{ZK_NAMENODE}/some/folder`
Add `zookeeper.connect` to your properties containing a valid zookeeper connection string.
The module is currently looking for a namenode-string at a zk-node called `/hadoop-ha/hadoop-ha/ActiveBreadCrumb`.


## File-Historizer component
The component, if used within a system, can be accessed using this protocol:

        (defprotocol HistorizationHandling
          (writer-for-timestamp [self timestamp] "Returns a PrintWriter-instance for the given timestamp (see Historization)"))

### Historization            
A new PrintWriter is returned for every new hour.
This leads to the following fs-structure:

        └── output
            └── 2015
                └── 11
                    └── 13
                        └── 13
                        |   └── a2586bd5-2636-4130-1fef-cd35af8e433k.hist.gz
                        └── 14
                            └── c7586bd8-2636-4130-9fef-cd35af8e433f.hist.gz


## Initial Contributors

Christian Stamm, Kai Brandes, Torsten Mangner, Daley Chetwynd, Carl Düvel, Florian Weyandt

## License

Apache License
