# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Sqlg is a TinkerPop 3 (Gremlin) `Graph` implementation on top of relational databases. It compiles Gremlin
traversals into SQL, so a single logical graph query typically becomes one or a handful of (sometimes deeply
nested) SQL statements instead of many small lookups. Dialects supported: PostgreSQL (the primary/most complete
one — topology, partitioning, sharding, pgRouting, pgvector, ltree, JSON, arrays, foreign data wrappers), H2,
HSQLDB, MariaDB, MySQL.

Java 17, Maven multi-module reactor build, TinkerPop `${tinkerpop.version}` (see root `pom.xml`) as the Gremlin
API version.

## Module layout

- `sqlg-core` — the engine: Gremlin traversal strategies, SQL generation/parsing, the topology model
  (schemas/vertex labels/edge labels/properties/partitions as first-class metadata), `SqlgGraph`/`SqlgVertex`/
  `SqlgEdge`/`SqlgTransaction`. Dialect-agnostic; depends only on the `SqlDialect`/`SqlgPlugin` SPI, not on any
  concrete dialect.
- `sqlg-<db>-parent/sqlg-<db>-dialect` (postgres, h2, hsqldb, mariadb, mysql) — implements `SqlDialect` /
  `SqlBulkDialect` / `SqlSchemaChangeDialect` from `sqlg-core` for that database, and registers itself via
  `META-INF/services/org.umlg.sqlg.SqlgPlugin` (Java `ServiceLoader` SPI — see `SqlgPlugin.java`). Dialect
  selection at runtime is by inspecting the JDBC URL or connection metadata, not config.
- `sqlg-<db>-parent/sqlg-<db>` — the test module for that dialect. Contains dialect-specific tests plus a
  `<Db>AllTest` suite that pulls in the shared test suite from `sqlg-test`, and `Sqlg<Db>StructureStandardTest`
  / `Sqlg<Db>ProcessStandardTest`, which run the upstream TinkerPop `gremlin-test` structure/process conformance
  suites against that dialect (feature opt-in/opt-out is declared via `@OptIn`/`@OptOut` annotations on
  `SqlgGraph`).
- `sqlg-test` — NOT a standalone test module; it's a shared library of test cases (`org.umlg.sqlg.test.*`,
  hundreds of `Test*` classes organized by feature area) and test infrastructure (`BaseTest`) reused across all
  dialect test modules. A dialect module runs these by subclassing/importing them into its own `AllTest` suite.
- `sqlg-jdbc-pool/sqlg-c3p0`, `sqlg-jdbc-pool/sqlg-hikari` — pluggable connection pool backends.
- `sqlg-jmh` — JMH microbenchmarks.
- `sqlg-doc` — published documentation site sources/archives (versioned docs under `sqlg-doc/docs/<version>`).
- `sqlg-testdb-postgres` — Docker setup (`docker-compose.yml`, `start.sh`) to run a disposable Postgres instance
  for the Postgres test suite.

## Build & test

This is a Maven reactor; run commands from the repo root unless testing a single module.

```bash
# Full reactor build (compiles everything, no tests)
mvn -T4 install -DskipTests

# Run the test suite as CI does — H2 and HSQLDB only (fast, no external DB required);
# Postgres/MariaDB/MySQL suites are commented out of root pom.xml surefire <includes> by default
mvn -T4 test
# or
./run-tests.sh

# Run a single test class in one dialect module
mvn -pl sqlg-h2-parent/sqlg-h2 -am test -Dtest=TestVertex

# Run a single test method
mvn -pl sqlg-h2-parent/sqlg-h2 -am test -Dtest=TestVertex#testSomeMethod
```

To run the **Postgres** suite (the most complete/authoritative one), a running Postgres instance is required:

```bash
cd sqlg-testdb-postgres && ./start.sh   # starts Postgres in Docker via docker-compose
mvn -pl sqlg-postgres-parent/sqlg-postgres -am test -Dtest=PostgresAllTest
```

Connection settings for tests live in each dialect test module's `src/test/resources/sqlg.properties`
(`jdbc.url`, `jdbc.username`, `jdbc.password`, plus Sqlg-specific flags like `implement.foreign.keys`,
`cache.vertices`, `bulk.within.count`). Several Postgres tests use alternate property files
(`sqlg.readonly.properties`, `sqlg.pool1/2/3.properties`, `sqlg.fdw.properties`, `sqlg.coordinator.properties`)
for scenarios like read-only roles, connection-pool variants, foreign data wrappers, and multi-graph
coordination — check the specific test class to see which one it loads.

The root `pom.xml` enforces JDK 17+ and Maven 3.6+ (`maven-enforcer-plugin`) and dependency convergence across
the reactor — a version bump in one module's dependency can break the build elsewhere via
`DependencyConvergence`.

## Architecture notes

- **Traversal compilation, not per-step execution.** `sqlg-core/strategy/*` contains TinkerPop
  `TraversalStrategy` implementations (`SqlgGraphStepStrategy`, `SqlgVertexStepStrategy`, `SqlgHasStepStrategy`,
  `SqlgWhereStrategy`, etc.) that rewrite a Gremlin traversal at planning time so that as much of it as possible
  gets folded into SQL. `sql/parse/ReplacedStepTree` and `SchemaTableTree` model which parts of the traversal
  chain have been "replaced" by SQL and how the resulting joins/schemas/tables nest. `GremlinParser` and
  `sql/parse/*` turn the (partially rewritten) traversal into actual SQL text. Understanding a bug in query
  behavior usually means tracing: Gremlin steps → strategy rewrite → `ReplacedStepTree`/`SchemaTableTree` →
  generated SQL, across `strategy/`, `sql/parse/`, and `step/`.
- **Topology is a first-class runtime model, not just JDBC metadata.** `structure/topology/*`
  (`Topology`, `Schema`, `VertexLabel`, `EdgeLabel`, `PropertyColumn`, `Index`, `Partition`, `ForeignKey`, ...)
  tracks the graph's schema in memory and persists it into dedicated `*_schema`/`*_vertex`/`*_edge`/`*_property`
  tables (`TopologyManager`, `SqlgStartupManager`) rather than only relying on the underlying DB's information
  schema. Adding vertex/edge labels, properties, indexes, or partitions all go through this topology layer so
  it stays in sync with the physical DDL.
- **Dialect SPI (`sql/dialect/SqlDialect`, `SqlBulkDialect`, `SqlSchemaChangeDialect`)** is the seam between
  `sqlg-core` and each database. It covers type mapping (`PropertyType` ⇄ SQL types), DDL generation/schema
  changes, bulk-loading strategy (e.g. Postgres `COPY`), and SQL dialect quirks (quoting, `LIMIT`/`TOP` syntax,
  array/JSON support, etc.). New database support or new SQL features generally start by extending this SPI and
  its per-dialect implementations, not `sqlg-core`.
- **Transactions.** `SqlgTransaction` wraps one JDBC `Connection` per Gremlin transaction and coordinates it
  with `BatchManager` (for batch-mode inserts/updates) and per-thread `TransactionCache`. Most structural
  mutation paths (`SqlgVertex`, `SqlgEdge`, `SqlgElement`) go through the transaction's batch manager rather
  than issuing SQL immediately, especially under batch mode.
- **TinkerPop conformance vs. Sqlg-specific behavior.** `SqlgGraph` declares `@OptIn`/`@OptOut` for the upstream
  `gremlin-test` structure/process suites — this is the definitive list of which TinkerPop behaviors Sqlg
  intentionally diverges from (and why, in the `reason` attribute) versus which are run as regression tests.
  Check there before assuming a TinkerPop conformance test failure is a real bug.
- **Postgres is the reference dialect.** Features like schema/table partitioning, sharding, pgRouting,
  pgvector, ltree, and foreign data wrappers only exist for Postgres; H2/HSQLDB/MariaDB/MySQL implement the
  common subset. When adding a feature, check whether it's meant to be dialect-agnostic (goes through the
  `SqlDialect` SPI with a fallback/default) or Postgres-only.
