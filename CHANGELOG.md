##2.1.6

* Added tests for inserting via fdw on postgres for user identified elements
* Added validation for inserting data via fdw on postgres.
* Fixed [#451](https://github.com/pietermartin/sqlg/issues/451). Postgresql copy command did not take special characters into account.
* Fixed [#450](https://github.com/pietermartin/sqlg/issues/450). Fixed a bunch of copy past errors in Sqlg's ddl statements.
* Fixed [#446](https://github.com/pietermartin/sqlg/issues/446). Upgraded H2 to 2.1.210
* Implemented [#334](https://github.com/pietermartin/sqlg/issues/332). Added `Topology.lock` and `Topology.unlock`. 
* Upgrade postgresql jdbc driver to 42.3.1
* Implemented [#361](https://github.com/pietermartin/sqlg/issues/361). Implements UUID support on postgresql, hsqldb and h2.
* Implemented [#431](https://github.com/pietermartin/sqlg/issues/431). Optimizes `id()`.
* Implemented [#139](https://github.com/pietermartin/sqlg/issues/139).
Added `rename` to `VertexLabel`, `EdgeLabel` and `PropertyColumn`
* BREAKING CHANGE: Changed `TopologyListener`'s interface, `oldValue` is now a `TopologyInf`
* Implemented [#428](https://github.com/pietermartin/sqlg/issues/428). 
Added ability to import foreign schemas and to import tables into an existing schema.
Both import must be consistent, i.e. can not reference elements that are not being imported.

* Removed deprecated `RecordId.getId()`
 
##2.1.5

* Upgrade to TinkerPop 3.1.5
* Remove support for globally unique indexes
* Update support for citus to work with citus 10.x

##2.1.4

* Upgrade HSQLDB to 2.6.1
* Support partitioned primary keys.
* Added postgresql hash partition support.

##2.1.3

* Added a ui to visualize and delete schema elements.

##2.1.1

*Fix bug [#417](https://github.com/pietermartin/sqlg/issues/417). Fix issue with dangling Index meta data on property deletions

*Fix bug [#416](https://github.com/pietermartin/sqlg/issues/416). Fixed bug with first iteration state

*Fix bug [#415](https://github.com/pietermartin/sqlg/issues/415). Optimize InjectStep and create `SqlTraverser` in `SqlgStartStepBarrier`

*Deprecate global unique identifiers

##2.1.0

* Removed all attempts at preventing database dead locks from happening. Sqlg no longer takes any kind of locks
when do schema creation scripts.
  This means that preventing database dead locks is now the responsibility of the client.
  Sqlg's attempt at managing this was a bad idea from the start. The lock Sqlg took was too coarse and ended
  up causing lock timeouts. The suggested pattern to use now is to simply retry the transaction x number of times.

* Removed `time zone` from Sqlg's timestamp data types. Sqlg guarantees that all clients regardless of their time zone will see the timestamp as stored in the db. This meant that Sqlg had to undo any manipulation of the timestamp the db might have done. This is because Sqlg used the `timestamp with time zone` data type.
`2.1.0` removes the time zone part. The sql below will generate the `ALTER TABLE...` statements to change the type of all columns with `time zone`.

ALTER `timestamp with time zone` to `timestamp without time zone` and
  `time with time zone` to `time without time zone`, 

```
select 
	'ALTER TABLE "' || e.name || '"."V_' || c.name || '" ALTER COLUMN "' || a.name || '" TYPE timestamp' || ';'
from 
	sqlg_schema."V_property" a join 
	sqlg_schema."E_vertex_property" b on a."ID" = b."sqlg_schema.property__I" join
	sqlg_schema."V_vertex" c on b."sqlg_schema.vertex__O" = c."ID" join
	sqlg_schema."E_schema_vertex" d on c."ID" = d."sqlg_schema.vertex__I" join
	sqlg_schema."V_schema" e on d."sqlg_schema.schema__O" = e."ID"
where 
	a.type = 'LOCALDATETIME'
	
UNION ALL	
	
select 
	'ALTER TABLE "' || e.name || '"."V_' || c.name || '" ALTER COLUMN "' || a.name || '" TYPE time' || ';'
from 
	sqlg_schema."V_property" a join 
	sqlg_schema."E_vertex_property" b on a."ID" = b."sqlg_schema.property__I" join
	sqlg_schema."V_vertex" c on b."sqlg_schema.vertex__O" = c."ID" join
	sqlg_schema."E_schema_vertex" d on c."ID" = d."sqlg_schema.vertex__I" join
	sqlg_schema."V_schema" e on d."sqlg_schema.schema__O" = e."ID"
where 
	a.type = 'LOCALTIME'

UNION ALL
	
select 
	'ALTER TABLE "' || e.name || '"."V_' || c.name || '" ALTER COLUMN "' || a.name || '" TYPE timestamp' || ';'
from 
	sqlg_schema."V_property" a join 
	sqlg_schema."E_vertex_property" b on a."ID" = b."sqlg_schema.property__I" join
	sqlg_schema."V_vertex" c on b."sqlg_schema.vertex__O" = c."ID" join
	sqlg_schema."E_schema_vertex" d on c."ID" = d."sqlg_schema.vertex__I" join
	sqlg_schema."V_schema" e on d."sqlg_schema.schema__O" = e."ID"
where 
	a.type = 'ZONEDDATETIME'	
	
UNION ALL

select 
	'ALTER TABLE "' || g.name || '"."E_' || c.name || '" ALTER COLUMN "' || a.name || '" TYPE timestamp' || ';'
from 
	sqlg_schema."V_property" a join 
	sqlg_schema."E_edge_property" b on a."ID" = b."sqlg_schema.property__I" join
	sqlg_schema."V_edge" c on b."sqlg_schema.edge__O" = c."ID" join
	sqlg_schema."E_out_edges" d on c."ID" = d."sqlg_schema.edge__I" join
	sqlg_schema."V_vertex" e on d."sqlg_schema.vertex__O" = e."ID" join
	sqlg_schema."E_schema_vertex" f on e."ID" = f."sqlg_schema.vertex__I" join
	sqlg_schema."V_schema" g on f."sqlg_schema.schema__O" = g."ID"
where 
	a.type = 'LOCALDATETIME'	
	
UNION ALL

select 
	'ALTER TABLE "' || g.name || '"."E_' || c.name || '" ALTER COLUMN "' || a.name || '" TYPE time' || ';'
from 
	sqlg_schema."V_property" a join 
	sqlg_schema."E_edge_property" b on a."ID" = b."sqlg_schema.property__I" join
	sqlg_schema."V_edge" c on b."sqlg_schema.edge__O" = c."ID" join
	sqlg_schema."E_out_edges" d on c."ID" = d."sqlg_schema.edge__I" join
	sqlg_schema."V_vertex" e on d."sqlg_schema.vertex__O" = e."ID" join
	sqlg_schema."E_schema_vertex" f on e."ID" = f."sqlg_schema.vertex__I" join
	sqlg_schema."V_schema" g on f."sqlg_schema.schema__O" = g."ID"
where 
	a.type = 'LOCALTIME'	
	
UNION ALL

select 
	'ALTER TABLE "' || g.name || '"."E_' || c.name || '" ALTER COLUMN "' || a.name || '" TYPE timestamp' || ';'
from 
	sqlg_schema."V_property" a join 
	sqlg_schema."E_edge_property" b on a."ID" = b."sqlg_schema.property__I" join
	sqlg_schema."V_edge" c on b."sqlg_schema.edge__O" = c."ID" join
	sqlg_schema."E_out_edges" d on c."ID" = d."sqlg_schema.edge__I" join
	sqlg_schema."V_vertex" e on d."sqlg_schema.vertex__O" = e."ID" join
	sqlg_schema."E_schema_vertex" f on e."ID" = f."sqlg_schema.vertex__I" join
	sqlg_schema."V_schema" g on f."sqlg_schema.schema__O" = g."ID"
where 
	a.type = 'ZONEDDATETIME'	
	
```

##2.0.3

* Merge the reducing branch in. Sqlg now optimizes reducing steps.
  `MaxGlobalStep`, `MinGlobalStep`, `SumGlobalStep`, `MeanGlobalStep` and `GroupStep` are supported.

*Fix bug [#398](https://github.com/pietermartin/sqlg/issues/398). Fixed a bug in the sql generation.

##2.0.2

*Implement enhancement[#296](https://github.com/pietermartin/sqlg/issues/396) Replace `TIME WITH TIME ZONE` with `TIME`.
This is a breaking change, the databases will have to run the command, `ALTER TABLENAME ALTER COLUMN column_name TYPE TIME;`.

*Fix bug [#395](https://github.com/pietermartin/sqlg/issues/395). Added a toggle in the failing tests to truncate
the time to MILLIS.

* Upgrade HSQLDB to 2.5.1

* Upgrade H2 to 1.4.200

*Fix bug [#359](https://github.com/pietermartin/sqlg/issues/359). The labels were not being handled properly. 
`UnionStep` is now optimized by `SqlgUnionStepBarrier` and `Startstep` with `SqlgStartStepBarrier`.

* Added support for `postgresql` array operators via [PR](https://github.com/pietermartin/sqlg/pull/360)

*Upgrade to TinkerPop 3.4.1, support added for docker/travis [#358](https://github.com/pietermartin/sqlg/pull/358)

*Fix bug [#344](https://github.com/pietermartin/sqlg/issues/344). Fix generation of `WHERE` clause. 

*Fix bug [#339](https://github.com/pietermartin/sqlg/issues/339). Fix the in out vertices being incorrectly set on 
updating of an edge.

*Fix bug [#336](https://github.com/pietermartin/sqlg/issues/336). Added a check for the presence of partitions.

*Fix bug [#335](https://github.com/pietermartin/sqlg/issues/335). Remove `ONLY` from `TRUNCATE ONLY` statement on 
postgresql as its not supported by partitioned tables.

* Added support to specify a custom datasource.

*Fix bug [#332](https://github.com/pietermartin/sqlg/issues/332). Added quotes to the `partition` sql expression.

* Fix bug [#329](https://github.com/pietermartin/sqlg/issues/329). Sqlg now only allows topology changes to be made when 
no write threads are active. It will detect a dead lock and throw an exception if a topology thread is waiting on a 
write thread that is waiting on the topology thread.

* Fix bug [#317](https://github.com/pietermartin/sqlg/issues/317). Made the timestamp precondition drop the 3 zero's of 
the nano seconds. ZULU JDK has more nano's than Oracle JDK and they are dropped by the database.

##2.0.1
* TestMultiThread.shouldExecuteWithCompetingThreads was failing on `RDBMS` that do not support transaction schema 
manipulation. Creating the schema upfront now.
* Fix bug [#322](https://github.com/pietermartin/sqlg/issues/322) Leaked database connections if `SqlgGraph` failed to 
start up.
* Fix bug [#321](https://github.com/pietermartin/sqlg/issues/321) Fix a bug with `P.without`.
* Fix bug [#320](https://github.com/pietermartin/sqlg/issues/320) Fix regressions with respect to Sqlg executing properly 
in the gremlin console.

##2.0.0
* Add user supplied primary key support.
* Add partitioning support on Postgresql 10.
* Add sharding support on Postgresql 10 using the [Citus](https://www.citusdata.com/) extension.
* Optimize the `PropertiesStep` and `PropertyMapStep` to only query the database for the 
requested properties and not for all properties as is the default.
* Remove auto sorting of columns on table creation. It should be done at the application level.

##1.5.2
* Upgrade to TinkerPop 3.3.3
* Add docker image for Postgresql
* Support additional properties on BulkAddEdge [#300](https://github.com/pietermartin/sqlg/issues/300)
* Fix concurrency bug on MsSqlServer batch mode. Take table lock on BulkCopy.
* Fix concurrency bug on Postgresql batch mode. The id sequence is incremented before the copy insert happens.
* Added MySql support. Uses MariaDb dialect but the MySql driver.
* Improve memory consumption by removing closed prepared statements from the cache.

##1.5.1
* Add the ability to set the `fetchSize` on the jdbc `java.sql.Statement`.
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
* Rewrite of the topology/schema management. `TopologyManager` is replaced with `Topology`.
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
* Sqlg stores the schema in the graph. It is accessible via the `TopologyStrategy`. eg. `TopologyStrategy.build().selectFrom(TopologyManager.SQLG_SCHEMA_SCHEMA_TABLES)`
* Remove `SqlgTransaction.batchCommit()` as is no longer useful as the embedded topology change forced sqlg to auto flush the batch before any query.
* Add support for `java.time.ZonedDateTime`, `java.time.Duration` and `java.time.Period`
* Add support for array types in batch mode. `String[], int[], double[], long[], float[], short[], byte[]`

##1.1.0.RC2

* Use special characters to separate label + schema + table + column as opposed to a period. Periods may now be present in property names.
* Change postgresql copy command to use the csv format. This allows for specifying a quote character which prevents nasty bugs where backslash characters in the value escapes the delimiter.
* Added `SortedTree`. A utility class that wraps TinkerPop's Tree using a TreeMap instead.