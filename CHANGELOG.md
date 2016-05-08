##1.2.0

* Add all optimizations to LocalStep.
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