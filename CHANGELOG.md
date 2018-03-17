##1.5.1
* Fix bug [#272](https://github.com/pietermartin/sqlg/issues/272)
* Make gremlin console work. Tinkerpop made some minor changes to the console that made it stop working.

##1.5.0

* Optimize `DropStep` i.e. `drop()`.
`TRUNCATE` is used for the most simplest cases. Else `DELETE` statements are generated.
If the `traversal` itself can not be optimized then barrier strategy is used to cache the starts before deleted them all in one statement.
* Upgrade dependencies to latest in sonatype. [#247](https://github.com/pietermartin/sqlg/issues/247)
* Fix bug [#246](https://github.com/pietermartin/sqlg/issues/246)
* Optimize `OrStep` and `AndStep` to push the predicates down to the db if they are trivial.
* Optimize `NotStep` to barrier the starts.
* Optimize `AndStep` to barrier the starts.
* Optimize `OrStep` to barrier the starts.
* Optimize `WhereTraversalStep` to barrier the starts.
* Optimize `RepeatStep`s until traversal to barrier the starts.
* Replace `TraversetSet` in `ExpandableStep` with an `ArrayList` in `SqlgExpandableStep`. `TravererSet` has a backing `Map`
of `Traverser` which for Sqlg is always a `SqlgTraverser`. As `SqlgTraverser` always holds the full `Path` adding the barriered (cached)
starts to the map is to heavy. Seeing as Sqlg does not use bulking the `TraverserSet`s logic is not needed.
* Optimize the `TraversalFilterStep` to barrier the starts.
* Upgrade to Tinkerpop 3.3.0

##1.4.1

* Added new predicate to compare 2 properties on the same label.
* Added support for `has("property")` and `hasNot("property")`

##1.4.0

* Added support for MariaDb
* Added support for MSSqlServer
* Added barrier optimization for unoptimized steps. This optimization barriers (cache) the incoming traversers and 
performs the next step for all the traversers at once. It uses the sql `VALUES` expression for this.

Currently the `optional`, `choose`, `repeat` and `local` steps have this optimization.
* Added batch mode support for all dialects.

##1.3.3

* `TopologyInf` support for `remove(boolean preserveData)` added.
        Thanks to [JPMoresmau](https://github.com/JPMoresmau)
* Replace `ResultSet.getObject(index)` with `ResultSet.getString/Int...` as its faster.
* Removed Hazelcast. The topology is now distributed using Postgresql's `lock` to hold a global lock across multiple Graphs and JVMs
and a `V_log` table which hold the changes made. The changes are in turn sent to other Graphs and JVMs using Postgresql's `notify` mechanism.
* Added `TopologyListener` as a mechanism to observe changes to the topology.
* Added support for global  unique indexes. This means that a unique index can be placed on multiple properties from any Vertex or Edge.
* Rewrite of the topology/schema management. `SchemaManager` is replaced with `Topology`.
There are now classes representing the topology. `Topology`, `Schema`, `VertexLabel`, `EdgeLabel`, `PropertyColumn`, `Index` and `GlobalUniqueIndex`
* Strengthened the reloading of the topology from the information_schema tables.
This highlighted some limitations. It is not possible to tell a primitive array from a object array. 
As such all arrays are  loaded as object arrays. i.e. `int[]{1,2,3}` will become `Integer[]{1,2,3}`

    Example of how to fix a property' type
    
    update sqlg_schema."V_property" set type = 'byte_ARRAY' where name = 'password' and type = 'BYTE_ARRAY'
    update sqlg_schema."V_property" set type = 'byte_ARRAY' where name = 'password_salt' and type = 'BYTE_ARRAY'
* Fix bug [#116](https://github.com/pietermartin/sqlg/issues/116)

    If a `RepeapStep` could not be optimized then the incoming emit `Element` did not get a label so it was not being returned from the sql.

##1.3.2

* Ensure SqlgGraphStepStrategy and SqlgVertexStepStrategy fires before InlineFilterStrategy.
* Fix a bug where hasId uses the P.neq predicate.
* Use BIGSERIAL for auto increment columns in Postgresql [#91](https://github.com/pietermartin/sqlg/issues/91)
* Fix bug [#92](https://github.com/pietermartin/sqlg/issues/92)
* SqlgGraph.bulkAddEdges to take a Collection of ids as opposed to a List.
    Fix bug [#102](https://github.com/pietermartin/sqlg/issues/102)
* Fix bug [#73](https://github.com/pietermartin/sqlg/issues/73)
        Thanks to [JPMoresmau](https://github.com/JPMoresmau)

##1.3.1

* 1.3.0 uploaded strange byte code, this fixes that.

##1.3.0

* Upgrade to TinkerPop 3.2.2
* Added H2 support.
* Added support for getting the data source from JNDI.
* Optimize `SqlgGraph.bulkAddEdges(...) to use the correct types for the in and out properties. This has a marginal performance increase.
* Refactored pom to separate out `gremlin-groovy` to be an optional dependency.

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