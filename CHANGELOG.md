##1.3.0

* Upgrade to TinkerPop 3.2.2
* Added H2 support.
* Added support for getting the data source from JNDI.
* Optimize `SqlgGraph.bulkAddEdges(...) to use the correct types for the in and out properties. This has a marginal performance increase.

##1.2.0

* Optimize lazy iteration. Remove unnecessary list creation for managing state.
* Add thread local `PreparedStatement` cache, to close all statements on commit or rollback.
* Refactor the vertex transaction cache to use a `WeakHashMap`.
* Refactor Sqlg to lazily iterate the sql `ResultSet`.
* Add all optimizations to `LocalStep`.
* Refactor `RepeatStep` optimization to follow the same sql pattern as the `OptionalStep` optimization.
* Optimized the `OptionalStep`.
* Optimize `hasId(...)`
* Sqlg stores the schema in the graph. It is accessible via the `TopologyStrategy`. eg. `TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES)`
* Remove `SqlgTransaction.batchCommit()` as is no longer useful as the embedded topology change forced sqlg to auto flush the batch before any query.
* Add support for `java.time.ZonedDateTime`, `java.time.Duration` and `java.time.Period`
* Add support for array types in batch mode. `String[], int[], double[], long[], float[], short[], byte[]`

##1.1.0.RC2

* Use special characters to separate label + schema + table + column as opposed to a period. Periods may now be present in property names.
* Change postgresql copy command to use the csv format. This allows for specifying a quote character which prevents nasty bugs where backslash characters in the value escapes the delimiter.
* Added `SortedTree`. A utility class that wraps TinkerPop's Tree using a TreeMap instead.