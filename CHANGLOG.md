##1.2.0

* Sqlg stores the schema in the graph. It is accessible via the `TopologyStrategy`. eg. `TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES)`
* Remove SqlgTransaction.batchCommit() as is no longer useful as the embedded topology change forced sqlg to auto flush the batch before any query.
* Add support for java.time.ZonedDateTime, java.time.Duration and java.time.Period
