package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.topology.EdgeLabel;
import org.umlg.sqlg.topology.Topology;
import org.umlg.sqlg.topology.VertexLabel;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.sun.xml.internal.fastinfoset.alphabet.BuiltInRestrictedAlphabets.table;

/**
 * Date: 2014/07/12
 * Time: 11:02 AM
 */
public class SchemaManager {

    private Topology topology;
    public static final String JDBC_URL = "jdbc.url";
    public static final String GIVEN_TABLES_MUST_NOT_BE_NULL = "Given tables must not be null";
    public static final String GIVEN_TABLE_MUST_NOT_BE_NULL = "Given table must not be null";
    public static final String CREATED_ON = "createdOn";
    public static final String SCHEMA_VERTEX_DISPLAY = "schemaVertex";
    public static final String SHOULD_NOT_HAPPEN = "Should not happen!";
    private Logger logger = LoggerFactory.getLogger(SchemaManager.class.getName());

    /**
     * Rdbms schema that holds sqlg topology.
     */
    public static final String SQLG_SCHEMA = "sqlg_schema";
    /**
     * Table storing the graph's schemas.
     */
    public static final String SQLG_SCHEMA_SCHEMA = "schema";
    /**
     * Schema's name.
     */
    public static final String SQLG_SCHEMA_SCHEMA_NAME = "name";
    /**
     * Table storing the graphs vertex labels.
     */
    public static final String SQLG_SCHEMA_VERTEX_LABEL = "vertex";
    /**
     * VertexLabel's name property.
     */
    public static final String SQLG_SCHEMA_VERTEX_LABEL_NAME = "name";
    /**
     * Table storing the graphs edge labels.
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL = "edge";
    /**
     * EdgeLabel's name property.
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL_NAME = "name";
    /**
     * Table storing the graphs element properties.
     */
    public static final String SQLG_SCHEMA_PROPERTY = "property";
    /**
     * Edge table for the schema to vertex edge.
     */
    public static final String SQLG_SCHEMA_SCHEMA_VERTEX_EDGE = "schema_vertex";
    /**
     * Edge table for the vertices in edges.
     */
    public static final String SQLG_SCHEMA_IN_EDGES_EDGE = "in_edges";
    /**
     * Edge table for the vertices out edges.
     */
    public static final String SQLG_SCHEMA_OUT_EDGES_EDGE = "out_edges";
    /**
     * Edge table for the vertex's properties.
     */
    public static final String SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE = "vertex_property";
    /**
     * Edge table for the edge's properties.
     */
    public static final String SQLG_SCHEMA_EDGE_PROPERTIES_EDGE = "edge_property";
    /**
     * Property table's name property
     */
    public static final String SQLG_SCHEMA_PROPERTY_NAME = "name";
    /**
     * Property table's type property
     */
    public static final String SQLG_SCHEMA_PROPERTY_TYPE = "type";

    public static final String VERTEX_PREFIX = "V_";
    public static final String EDGE_PREFIX = "E_";
    public static final String VERTICES = "VERTICES";
    public static final String ID = "ID";
    public static final String VERTEX_SCHEMA = "VERTEX_SCHEMA";
    public static final String VERTEX_TABLE = "VERTEX_TABLE";
    public static final String LABEL_SEPARATOR = ":::";
    public static final String IN_VERTEX_COLUMN_END = "__I";
    public static final String OUT_VERTEX_COLUMN_END = "__O";
    public static final String ZONEID = "~~~ZONEID";
    public static final String MONTHS = "~~~MONTHS";
    public static final String DAYS = "~~~DAYS";
    public static final String DURATION_NANOS = "~~~NANOS";
    public static final String BULK_TEMP_EDGE = "BULK_TEMP_EDGE";

    public static final List<String> SQLG_SCHEMA_SCHEMA_TABLES = Arrays.asList(
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_SCHEMA,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_VERTEX_LABEL,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_EDGE_LABEL,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_PROPERTY,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE
    );

    //    private Map<String, String> schemas = new HashMap<>();
//    private Set<String> uncommittedSchemas = new HashSet<>();
//
//    private Map<String, Set<String>> labelSchemas = new HashMap<>();
//    private Map<String, Set<String>> uncommittedLabelSchemas = new ConcurrentHashMap<>();
//
//    private Map<String, Map<String, PropertyType>> tables = new ConcurrentHashMap<>();
//    private Map<String, Map<String, PropertyType>> uncommittedTables = new ConcurrentHashMap<>();
//
//    private Map<String, Set<String>> edgeForeignKeys = new ConcurrentHashMap<>();
//    private Map<String, Set<String>> uncommittedEdgeForeignKeys = new ConcurrentHashMap<>();
//
//    //tableLabels is a map of every vertex's schemaTable together with its in and out edge schemaTables
//    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> tableLabels = new HashMap<>();
//    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedTableLabels = new HashMap<>();
//
    //temporary tables
    private Map<String, Map<String, PropertyType>> temporaryTables = new ConcurrentHashMap<>();

    private Lock schemaLock;
    private SqlgGraph sqlgGraph;
    private SqlDialect sqlDialect;
    private SqlSchemaChangeDialect sqlSchemaChangeDialect;
    private boolean distributed;

    private static final int LOCK_TIMEOUT = 10;

    SchemaManager(SqlgGraph sqlgGraph, SqlDialect sqlDialect, Configuration configuration) {
        this.sqlgGraph = sqlgGraph;
        this.topology = new Topology(this.sqlgGraph);

        this.sqlDialect = sqlDialect;
        this.distributed = configuration.getBoolean(SqlgGraph.DISTRIBUTED, false);
        if (this.distributed) {
            this.sqlSchemaChangeDialect = (SqlSchemaChangeDialect) sqlDialect;
            this.sqlSchemaChangeDialect.registerListener(sqlgGraph);
        }
        this.schemaLock = new ReentrantLock();

        this.sqlgGraph.tx().afterCommit(() -> {
            this.topology.afterCommit();
            this.temporaryTables.clear();
        });
        this.sqlgGraph.tx().afterRollback(() -> {
            if (this.sqlDialect.supportsTransactionalSchema()) {
                this.topology.afterRollback();
            } else {
                this.topology.afterCommit();
            }
            this.temporaryTables.clear();
        });
    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    void ensureVertexTableExist(final String schema, final String label, final Object... keyValues) {
        Objects.requireNonNull(schema, GIVEN_TABLES_MUST_NOT_BE_NULL);
        Objects.requireNonNull(label, GIVEN_TABLE_MUST_NOT_BE_NULL);
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), String.format("label may not be prefixed with %s", VERTEX_PREFIX));

        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.topology.existVertexLabel(schema, label)) {
            this.topology.lock();
            //double check strategy in-case the table was created between the check and the lock.
            if (!this.topology.existVertexLabel(schema, label)) {
                this.topology.createVertexLabel(schema, label, columns);
            }
        }
        //ensure columns exist
        ensureVertexColumnsExist(schema, label, columns);
    }

    /**
     * @param edgeLabelName     The table for this edge
     * @param foreignKeyIn  The tables table pair of foreign key to the in vertex
     * @param foreignKeyOut The tables table pair of foreign key to the out vertex
     * @param keyValues
     * @return The edge's {@link SchemaTable}
     */
    public SchemaTable ensureEdgeTableExist(final String edgeLabelName, final SchemaTable foreignKeyOut, final SchemaTable foreignKeyIn, Object... keyValues) {
        Objects.requireNonNull(table, GIVEN_TABLE_MUST_NOT_BE_NULL);
        Objects.requireNonNull(foreignKeyIn.getSchema(), "Given inTable must not be null");
        Objects.requireNonNull(foreignKeyOut.getTable(), "Given outTable must not be null");
        final Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);

        EdgeLabel edgeLabel;
        Optional<EdgeLabel> edgeLabelOptional = this.topology.getEdgeLabel(foreignKeyOut.getSchema(), edgeLabelName);
        if (!edgeLabelOptional.isPresent()) {
            this.topology.lock();
            edgeLabelOptional = this.topology.getEdgeLabel(foreignKeyOut.getSchema(), edgeLabelName);
            if (!edgeLabelOptional.isPresent()) {
                edgeLabel = this.topology.createEdgeLabel(edgeLabelName, foreignKeyOut, foreignKeyIn, columns);
            } else {
                edgeLabel = edgeLabelOptional.get();
            }
        } else {
            edgeLabel = edgeLabelOptional.get();
        }
        Preconditions.checkState(edgeLabel != null, "BUG: EdgeLabel %s must be present!", edgeLabelName);

        if (!edgeLabel.getSchema().isSqlgSchema()) {
            Optional<VertexLabel> outVertexLabelOptional = this.topology.getVertexLabel(foreignKeyOut.getSchema(), foreignKeyOut.getTable());
            Optional<VertexLabel> inVertexLabelOptional = this.topology.getVertexLabel(foreignKeyIn.getSchema(), foreignKeyIn.getTable());
            Preconditions.checkState(outVertexLabelOptional.isPresent(), "Out vertex label not found for %s.%s", foreignKeyOut.getSchema(), foreignKeyOut.getTable());
            Preconditions.checkState(inVertexLabelOptional.isPresent(), "In vertex label not found for %s.%s", foreignKeyIn.getSchema(), foreignKeyIn.getTable());
            //
            //noinspection OptionalGetWithoutIsPresent
            edgeLabel.ensureEdgeForeignKeysExist(this.sqlgGraph, false, outVertexLabelOptional.get(), foreignKeyOut);
            //noinspection OptionalGetWithoutIsPresent
            edgeLabel.ensureEdgeForeignKeysExist(this.sqlgGraph, true, inVertexLabelOptional.get(), foreignKeyIn);
            edgeLabel.ensureColumnsExist(this.sqlgGraph, columns);
        }
        return SchemaTable.of(edgeLabel.getSchema().getName(), edgeLabel.getLabel());

    }

    void ensureVertexTemporaryTableExist(final String schema, final String table, final Object... keyValues) {
        Objects.requireNonNull(schema, GIVEN_TABLES_MUST_NOT_BE_NULL);
        Objects.requireNonNull(table, GIVEN_TABLE_MUST_NOT_BE_NULL);
        final String prefixedTable = VERTEX_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.temporaryTables.containsKey(prefixedTable)) {
            this.temporaryTables.put(prefixedTable, columns);
            createTempTable(prefixedTable, columns);
        }
    }

    boolean schemaExist(String schema) {
        return this.topology.existSchema(schema);
    }

    public void createSchema(String schema) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE SCHEMA ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

//    /**
//     * This is only called from createEdgeIndex
//     *
//     * @param schema
//     * @param table
//     * @param keyValues
//     */
//    public void ensureEdgeTableExist(final String schema, final String table, Object... keyValues) {
//        Objects.requireNonNull(schema, GIVEN_TABLES_MUST_NOT_BE_NULL);
//        Objects.requireNonNull(table, GIVEN_TABLE_MUST_NOT_BE_NULL);
//        final String prefixedTable = EDGE_PREFIX + table;
//        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
//        if (!this.tables.containsKey(schema + "." + prefixedTable)) {
//            //Make sure the current thread/transaction owns the lock
//            lock(schema, table);
//            if (!this.sqlDialect.getPublicSchema().equals(schema) && !this.schemas.containsKey(schema)) {
//                if (!this.uncommittedSchemas.contains(schema)) {
//                    this.uncommittedSchemas.add(schema);
//                    createSchema(schema);
//                }
//            }
//            if (!this.tables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
//                Set<String> schemas = this.uncommittedLabelSchemas.get(prefixedTable);
//                if (schemas == null) {
//                    this.uncommittedLabelSchemas.put(prefixedTable, new HashSet<>(Collections.singletonList(schema)));
//                } else {
//                    schemas.add(schema);
//                    this.uncommittedLabelSchemas.put(prefixedTable, schemas);
//                }
//                this.uncommittedTables.put(schema + "." + prefixedTable, columns);
//
//                //TODO need to createVertexLabel the topology here
//                createEdgeTable(schema, prefixedTable, columns);
//            }
//        }
//        //ensure columns exist
//        ensureVertexColumnsExist(schema, prefixedTable, columns);
//    }

    private void lock(String schema, String table) {
        if (!this.isLockedByCurrentThread()) {
            try {
                if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                    throw new RuntimeException("timeout lapsed for to acquire lock for schema creation for " + schema + "." + table);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //    private Map<String, PropertyType> internalGetColumn(String schema, String table) {
//        final Map<String, PropertyType> cachedColumns = this.tables.get(schema + "." + table);
//        Map<String, PropertyType> uncommitedColumns;
//        if (cachedColumns == null) {
//            uncommitedColumns = this.uncommittedTables.get(schema + "." + table);
//        } else {
//            uncommitedColumns = this.uncommittedTables.get(schema + "." + table);
//            if (uncommitedColumns != null) {
//                uncommitedColumns.putAll(cachedColumns);
//            } else {
//                uncommitedColumns = new HashMap<>(cachedColumns);
//            }
//        }
//        Objects.requireNonNull(uncommitedColumns, "Table must already be present in the cache!");
//        return uncommitedColumns;
//    }
//

    public void ensureVertexColumnExist(String schema, String label, ImmutablePair<String, PropertyType> keyValue) {
        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(keyValue.getLeft(), keyValue.getRight());
        ensureVertexColumnsExist(schema, label, columns);
    }

    public void ensureEdgeColumnExist(String schema, String label, ImmutablePair<String, PropertyType> keyValue) {
        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(keyValue.getLeft(), keyValue.getRight());
        ensureEdgeColumnsExist(schema, label, columns);
    }

    private void ensureVertexColumnsExist(String schema, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        this.topology.ensureVertexColumnsExist(schema, label, columns);
    }

    private void ensureEdgeColumnsExist(String schema, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        this.topology.ensureEdgeColumnsExist(schema, label, columns);
    }

//    private void ensureEdgeForeignKeysExist(String schema, String prefixedTable, boolean in, SchemaTable vertexSchemaTable) {
//        if (!prefixedTable.startsWith(VERTEX_PREFIX) && !prefixedTable.startsWith(EDGE_PREFIX)) {
//            throw new IllegalStateException("prefixedTable must start with " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
//        }
//        Set<String> foreignKeys = this.edgeForeignKeys.get(schema + "." + prefixedTable);
//        Set<String> uncommittedForeignKeys;
//        if (foreignKeys == null) {
//            uncommittedForeignKeys = this.uncommittedEdgeForeignKeys.get(schema + "." + prefixedTable);
//        } else {
//            uncommittedForeignKeys = this.uncommittedEdgeForeignKeys.get(schema + "." + prefixedTable);
//            if (uncommittedForeignKeys != null) {
//                uncommittedForeignKeys.addAll(foreignKeys);
//            } else {
//                uncommittedForeignKeys = new HashSet<>(foreignKeys);
//            }
//        }
//        if (uncommittedForeignKeys == null) {
//            //This happens on edge indexes which creates the prefixedTable with no foreign keys to vertices.
//            uncommittedForeignKeys = new HashSet<>();
//        }
//        SchemaTable foreignKey = SchemaTable.of(vertexSchemaTable.getSchema(), vertexSchemaTable.getTable() + (in ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END));
//        if (!uncommittedForeignKeys.contains(foreignKey.getSchema() + "." + foreignKey.getTable())) {
//            //Make sure the current thread/transaction owns the lock
//            lock(schema, prefixedTable);
//            if (!uncommittedForeignKeys.contains(foreignKey)) {
//
//                if (!SQLG_SCHEMA.equals(schema)) {
//                    TopologyManager.addLabelToEdge(this.sqlgGraph, schema, prefixedTable, in, foreignKey);
//                }
//
//                addEdgeForeignKey(schema, prefixedTable, foreignKey, vertexSchemaTable);
//                uncommittedForeignKeys.add(vertexSchemaTable.getSchema() + "." + foreignKey.getTable());
//                this.uncommittedEdgeForeignKeys.put(schema + "." + prefixedTable, uncommittedForeignKeys);
//
//                //For each prefixedTable capture its in and out labels
//                //This schema information is needed for compiling gremlin
//                //first do the in labels
//                SchemaTable foreignKeyInWithPrefix = SchemaTable.of(vertexSchemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + vertexSchemaTable.getTable());
//                Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.uncommittedTableLabels.get(foreignKeyInWithPrefix);
//                if (labels == null) {
//                    labels = Pair.of(new HashSet<>(), new HashSet<>());
//                    this.uncommittedTableLabels.put(foreignKeyInWithPrefix, labels);
//                }
//                if (in) {
//                    labels.getLeft().add(SchemaTable.of(schema, prefixedTable));
//                } else {
//                    labels.getRight().add(SchemaTable.of(schema, prefixedTable));
//                }
//            }
//        }
//    }

    public void close() {
        if (this.sqlSchemaChangeDialect != null)
            this.sqlSchemaChangeDialect.unregisterListener();
    }

//    private void createVertexTable(String schema, String tableName, Map<String, PropertyType> columns) {
//        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
//        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
//        sql.append(".");
//        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
//        sql.append(" (");
//        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
//        sql.append(" ");
//        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
//        if (columns.size() > 0) {
//            sql.append(", ");
//        }
//        buildColumns(columns, sql);
//        sql.append(")");
//        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
//            sql.append(";");
//        }
//        if (logger.isDebugEnabled()) {
//            logger.debug(sql.toString());
//        }
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement stmt = conn.createStatement()) {
//            stmt.execute(sql.toString());
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    private void buildColumns(Map<String, PropertyType> columns, StringBuilder sql) {
        int i = 1;
        //This is to make the columns sorted
        List<String> keys = new ArrayList<>(columns.keySet());
        Collections.sort(keys);
        for (String column : keys) {
            PropertyType propertyType = columns.get(column);
            int count = 1;
            String[] propertyTypeToSqlDefinition = sqlDialect.propertyTypeToSqlDefinition(propertyType);
            for (String sqlDefinition : propertyTypeToSqlDefinition) {
                if (count > 1) {
                    sql.append(sqlDialect.maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2])).append(" ").append(sqlDefinition);
                } else {
                    //The first column existVertexLabel no postfix
                    sql.append(sqlDialect.maybeWrapInQoutes(column)).append(" ").append(sqlDefinition);
                }
                if (count++ < propertyTypeToSqlDefinition.length) {
                    sql.append(", ");
                }
            }
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
    }

//    private void createEdgeTable(String schema, String tableName, SchemaTable foreignKeyIn, SchemaTable foreignKeyOut, Map<String, PropertyType> columns) {
//        this.sqlDialect.assertTableName(tableName);
//        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
//        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
//        sql.append(".");
//        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
//        sql.append("(");
//        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
//        sql.append(" ");
//        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
//        if (columns.size() > 0) {
//            sql.append(", ");
//        }
//        buildColumns(columns, sql);
//        sql.append(", ");
//        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
//        sql.append(" ");
//        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
//        sql.append(", ");
//        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
//        sql.append(" ");
//        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
//
//        //foreign key definition start
//        if (this.sqlgGraph.isImplementForeignKeys()) {
//            sql.append(", ");
//            sql.append("FOREIGN KEY (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
//            sql.append(") REFERENCES ");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema()));
//            sql.append(".");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyIn.getTable()));
//            sql.append(" (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
//            sql.append("), ");
//            sql.append(" FOREIGN KEY (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
//            sql.append(") REFERENCES ");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema()));
//            sql.append(".");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyOut.getTable()));
//            sql.append(" (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
//            sql.append(")");
//        }
//        //foreign key definition end
//
//        sql.append(")");
//        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
//            sql.append(";");
//        }
//
//        if (this.sqlgGraph.getSqlDialect().needForeignKeyIndex()) {
//            sql.append("\nCREATE INDEX ON ");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
//            sql.append(".");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
//            sql.append(" (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
//            sql.append(");");
//
//            sql.append("\nCREATE INDEX ON ");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
//            sql.append(".");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
//            sql.append(" (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
//            sql.append(");");
//        }
//
//        if (logger.isDebugEnabled()) {
//            logger.debug(sql.toString());
//        }
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement stmt = conn.createStatement()) {
//            stmt.execute(sql.toString());
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public void createTempTable(String tableName, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTemporaryTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(columns, sql);
        sql.append(") ");
        sql.append(this.sqlDialect.afterCreateTemporaryTableStatement());
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

//    //This is called from creating edge indexes
//    private void createEdgeTable(String schema, String tableName, Map<String, PropertyType> columns) {
//        this.sqlDialect.assertTableName(tableName);
//        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
//        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
//        sql.append(".");
//        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
//        sql.append("(");
//        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
//        sql.append(" ");
//        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
//        if (columns.size() > 0) {
//            sql.append(", ");
//        }
//        buildColumns(columns, sql);
//        sql.append(")");
//        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
//            sql.append(";");
//        }
//        if (logger.isDebugEnabled()) {
//            logger.debug(sql.toString());
//        }
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement stmt = conn.createStatement()) {
//            stmt.execute(sql.toString());
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }


//    private void addEdgeForeignKey(String schema, String table, SchemaTable foreignKey, SchemaTable otherVertex) {
//        StringBuilder sql = new StringBuilder();
//        sql.append("ALTER TABLE ");
//        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
//        sql.append(".");
//        sql.append(this.sqlDialect.maybeWrapInQoutes(table));
//        sql.append(" ADD COLUMN ");
//        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
//        sql.append(" ");
//        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
//        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
//            sql.append(";");
//        }
//        if (logger.isDebugEnabled()) {
//            logger.debug(sql.toString());
//        }
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//            preparedStatement.executeUpdate();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        sql.setLength(0);
//        //foreign key definition start
//        if (this.sqlgGraph.isImplementForeignKeys()) {
//            sql.append(" ALTER TABLE ");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
//            sql.append(".");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(table));
//            sql.append(" ADD CONSTRAINT ");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(table + "_" + foreignKey.getSchema() + "." + foreignKey.getTable() + "_fkey"));
//            sql.append(" FOREIGN KEY (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
//            sql.append(") REFERENCES ");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(otherVertex.getSchema()));
//            sql.append(".");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + otherVertex.getTable()));
//            sql.append(" (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
//            sql.append(")");
//            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
//                sql.append(";");
//            }
//            if (logger.isDebugEnabled()) {
//                logger.debug(sql.toString());
//            }
//            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                preparedStatement.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        sql.setLength(0);
//        if (this.sqlgGraph.getSqlDialect().needForeignKeyIndex()) {
//            sql.append("\nCREATE INDEX ON ");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
//            sql.append(".");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(table));
//            sql.append(" (");
//            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
//            sql.append(")");
//            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
//                sql.append(";");
//            }
//            if (logger.isDebugEnabled()) {
//                logger.debug(sql.toString());
//            }
//            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                preparedStatement.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

//    public void createVertexIndex(SchemaTable schemaTable, Object... dummykeyValues) {
//        this.ensureVertexTableExist(schemaTable.getSchema(), schemaTable.getTable(), dummykeyValues);
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().readWrite();
//        internalCreateIndex(schemaTable, VERTEX_PREFIX, dummykeyValues);
//    }
//
//    public void createEdgeIndex(SchemaTable schemaTable, Object... dummykeyValues) {
//        this.ensureEdgeTableExist(schemaTable.getSchema(), schemaTable.getTable(), dummykeyValues);
//        this.sqlgGraph.tx().commit();
//        this.sqlgGraph.tx().readWrite();
//        internalCreateIndex(schemaTable, EDGE_PREFIX, dummykeyValues);
//    }

//    private void internalCreateIndex(SchemaTable schemaTable, String prefix, Object[] dummykeyValues) {
//        int i = 1;
//        for (Object dummyKeyValue : dummykeyValues) {
//            if (i++ % 2 != 0) {
//                if (!existIndex(schemaTable, prefix, this.sqlDialect.indexName(schemaTable, prefix, (String) dummyKeyValue))) {
//                    StringBuilder sql = new StringBuilder("CREATE INDEX ");
//                    sql.append(this.sqlDialect.maybeWrapInQoutes(this.sqlDialect.indexName(schemaTable, prefix, (String) dummyKeyValue)));
//                    sql.append(" ON ");
//                    sql.append(this.sqlDialect.maybeWrapInQoutes(schemaTable.getSchema()));
//                    sql.append(".");
//                    sql.append(this.sqlDialect.maybeWrapInQoutes(prefix + schemaTable.getTable()));
//                    sql.append(" (");
//                    sql.append(this.sqlDialect.maybeWrapInQoutes((String) dummyKeyValue));
//                    sql.append(")");
//                    if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
//                        sql.append(";");
//                    }
//                    if (logger.isDebugEnabled()) {
//                        logger.debug(sql.toString());
//                    }
//                    Connection conn = this.sqlgGraph.tx().getConnection();
//                    try (Statement stmt = conn.createStatement()) {
//                        stmt.execute(sql.toString());
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
//        }
//    }

//    private boolean existIndex(SchemaTable schemaTable, String prefix, String indexName) {
//        Connection conn = this.sqlgGraph.tx().getConnection();
//        try (Statement stmt = conn.createStatement()) {
//            String sql = this.sqlDialect.existIndexQuery(schemaTable, prefix, indexName);
//            ResultSet rs = stmt.executeQuery(sql);
//            return rs.next();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

//    public boolean tableExist(String schema, String table) {
//        this.sqlgGraph.tx().readWrite();
//        boolean exists = this.tables.containsKey(schema + "." + table) || this.uncommittedTables.containsKey(schema + "." + table);
//        return exists;
//    }

    void loadSchema() {
        if (logger.isDebugEnabled()) {
            logger.debug("SchemaManager.loadSchema()...");
        }
        //check if the topology schema exists, if not createVertexLabel it
        boolean existSqlgSchema = existSqlgSchema();
        if (!existSqlgSchema) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            createSqlgSchema();
            //committing here will ensure that sqlg creates the tables.
            this.sqlgGraph.tx().commit();
            stopWatch.stop();
            logger.debug("Time to createVertexLabel sqlg topology: " + stopWatch.toString());
        }
        if (!existSqlgSchema) {
            addPublicSchema();
            this.sqlgGraph.tx().commit();
        }
        if (!existSqlgSchema) {
            //old versions of sqlg needs the topology populated from the information_schema table.
            logger.debug("Upgrading sqlg from pre sqlg_schema version to sqlg_schema version");
            upgradeSqlgToTopologySchema();
            logger.debug("Done upgrading sqlg from pre sqlg_schema version to sqlg_schema version");
        }
        loadUserSchema();
        this.sqlgGraph.tx().commit();
        if (distributed) {
        }

    }

    private void upgradeSqlgToTopologySchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String[] types = new String[]{"TABLE"};
            //load the schemas
            ResultSet schemaRs = metadata.getSchemas();
            while (schemaRs.next()) {
                String schema = schemaRs.getString(1);
                if (schema.equals(SQLG_SCHEMA) ||
                        this.sqlDialect.getDefaultSchemas().contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                TopologyManager.addSchema(this.sqlgGraph, schema);
            }
            //load the vertices
            ResultSet vertexRs = metadata.getTables(catalog, schemaPattern, "V_%", types);
            while (vertexRs.next()) {
                String schema = vertexRs.getString(2);
                String table = vertexRs.getString(3);

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getDefaultSchemas());
                schemasToIgnore.remove(this.sqlDialect.getPublicSchema());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }

                Map<SchemaTable, MutablePair<SchemaTable, SchemaTable>> inOutSchemaTable = new HashMap<>();
                Map<String, PropertyType> columns = new ConcurrentHashMap<>();
                //get the columns
                ResultSet columnsRs = metadata.getColumns(catalog, schema, table, null);
                while (columnsRs.next()) {
                    String column = columnsRs.getString(4);
                    if (!column.equals(SchemaManager.ID)) {
                        int columnType = columnsRs.getInt(5);
                        String typeName = columnsRs.getString("TYPE_NAME");
                        PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(this.sqlgGraph, schema, table, column, columnType, typeName);
                        columns.put(column, propertyType);
                    }

                }
                TopologyManager.addVertexLabel(this.sqlgGraph, schema, table, columns);
            }
            //load the edges without their properties
            ResultSet edgeRs = metadata.getTables(catalog, schemaPattern, "E_%", types);
            while (edgeRs.next()) {
                String schema = edgeRs.getString(2);
                String table = edgeRs.getString(3);

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getDefaultSchemas());
                schemasToIgnore.remove(this.sqlDialect.getPublicSchema());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }

                Map<SchemaTable, MutablePair<SchemaTable, SchemaTable>> inOutSchemaTableMap = new HashMap<>();
                Map<String, PropertyType> columns = Collections.emptyMap();
                //get the columns
                ResultSet columnsRs = metadata.getColumns(catalog, schema, table, null);
                SchemaTable edgeSchemaTable = SchemaTable.of(schema, table);
                boolean edgeAdded = false;
                while (columnsRs.next()) {
                    String column = columnsRs.getString(4);
                    if (table.startsWith(EDGE_PREFIX) && (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) || column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END))) {
                        String[] split = column.split("\\.");
                        SchemaTable foreignKey = SchemaTable.of(split[0], split[1]);
                        if (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                            SchemaTable schemaTable = SchemaTable.of(
                                    split[0],
                                    split[1].substring(0, split[1].length() - SchemaManager.IN_VERTEX_COLUMN_END.length())
                            );
                            if (inOutSchemaTableMap.containsKey(edgeSchemaTable)) {
                                MutablePair<SchemaTable, SchemaTable> inSchemaTable = inOutSchemaTableMap.get(edgeSchemaTable);
                                if (inSchemaTable.getLeft() == null) {
                                    inSchemaTable.setLeft(schemaTable);
                                } else {
                                    TopologyManager.addLabelToEdge(this.sqlgGraph, schema, table, true, foreignKey);
                                }
                            } else {
                                inOutSchemaTableMap.put(edgeSchemaTable, MutablePair.of(schemaTable, null));
                            }
                        } else if (column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                            SchemaTable schemaTable = SchemaTable.of(
                                    split[0],
                                    split[1].substring(0, split[1].length() - SchemaManager.OUT_VERTEX_COLUMN_END.length())
                            );
                            if (inOutSchemaTableMap.containsKey(edgeSchemaTable)) {
                                MutablePair<SchemaTable, SchemaTable> outSchemaTable = inOutSchemaTableMap.get(edgeSchemaTable);
                                if (outSchemaTable.getRight() == null) {
                                    outSchemaTable.setRight(schemaTable);
                                } else {
                                    TopologyManager.addLabelToEdge(this.sqlgGraph, schema, table, false, foreignKey);
                                }
                            } else {
                                inOutSchemaTableMap.put(edgeSchemaTable, MutablePair.of(null, schemaTable));
                            }
                        }
                        MutablePair<SchemaTable, SchemaTable> inOutLabels = inOutSchemaTableMap.get(edgeSchemaTable);
                        if (!edgeAdded && inOutLabels.getLeft() != null && inOutLabels.getRight() != null) {
                            TopologyManager.addEdgeLabel(this.sqlgGraph, schema, table, inOutLabels.getLeft(), inOutLabels.getRight(), columns);
                            edgeAdded = true;
                        }
                    }
                }
            }
            //load the edges without their in and out vertices
            edgeRs = metadata.getTables(catalog, schemaPattern, "E_%", types);
            while (edgeRs.next()) {
                String schema = edgeRs.getString(2);
                String table = edgeRs.getString(3);

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getDefaultSchemas());
                schemasToIgnore.remove(this.sqlDialect.getPublicSchema());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }

                Map<String, PropertyType> columns = new HashMap<>();
                //get the columns
                ResultSet columnsRs = metadata.getColumns(catalog, schema, table, null);
                while (columnsRs.next()) {
                    String column = columnsRs.getString(4);
                    if (!column.equals(SchemaManager.ID) && !column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) && !column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                        int columnType = columnsRs.getInt(5);
                        String typeName = columnsRs.getString("TYPE_NAME");
                        PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(this.sqlgGraph, schema, table, column, columnType, typeName);
                        columns.put(column, propertyType);
                    }
                }
                TopologyManager.addEdgeColumn(this.sqlgGraph, schema, table, columns);
            }
            if (distributed) {
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load Sqlg's actual topology.
     * This is needed because with the {@link TopologyStrategy} it is possible to query the topology itself.
     */

    /**
     * Load the schema from the topology.
     */
    private void loadUserSchema() {
        this.topology.loadUserSchema();
        System.out.println(this.topology.toString());
    }

    private void addPublicSchema() {
        this.sqlgGraph.addVertex(
                T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", this.sqlDialect.getPublicSchema(),
                CREATED_ON, LocalDateTime.now()
        );
    }

    private void createSqlgSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            Statement statement = conn.createStatement();
            List<String> creationScripts = this.sqlDialect.sqlgTopologyCreationScripts();
            //Hsqldb can not do this in one go
            for (String creationScript : creationScripts) {
                statement.execute(creationScript);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean existSqlgSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            if (this.sqlDialect.supportSchemas()) {
                DatabaseMetaData metadata = conn.getMetaData();
                String catalog = null;
                ResultSet schemaRs = metadata.getSchemas(catalog, SQLG_SCHEMA);
                return schemaRs.next();
            } else {
                throw new IllegalStateException("schemas not supported not supported, i.e. probably MariaDB not supported.");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Deletes all tables.
     */
    public void clear() {
        try {
            Connection conn = this.sqlgGraph.getSqlgDataSource().get(this.sqlgGraph.getJdbcUrl()).getConnection();
            DatabaseMetaData metadata;
            metadata = conn.getMetaData();
            if (sqlDialect.supportsCascade()) {
                String catalog = "sqlgraphdb";
                String schemaPattern = null;
                String tableNamePattern = "%";
                String[] types = {"TABLE"};
                ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
                while (result.next()) {
                    StringBuilder sql = new StringBuilder("DROP TABLE ");
                    sql.append(sqlDialect.maybeWrapInQoutes(result.getString(3)));
                    sql.append(" CASCADE");
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
            } else {
                throw new RuntimeException("Not yet implemented!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    Set<String> getEdgeForeignKeys(String schemaTable) {
        return getAllEdgeForeignKeys().get(schemaTable);
    }

    public Map<String, Set<String>> getEdgeForeignKeys() {
        return getAllEdgeForeignKeys();
    }

    public Map<String, Set<String>> getAllEdgeForeignKeys() {
        return this.topology.getAllEdgeForeignKeys();
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        return this.topology.getTableLabels();
    }

    /**
     * <code>Schema.vertexLabels</code> contains committed tables with its labels.
     * <code>Schema.uncommittedVertexLabels</code> contains uncommitted tables with its labels.
     *
     * @param schemaTable
     * @return
     */
    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels(SchemaTable schemaTable) {
        return this.topology.getTableLabels(schemaTable);
    }

    Map<String, Map<String, PropertyType>> getTables() {
        return this.topology.getAllTables();
    }

    public Map<String, Map<String, PropertyType>> getAllTables() {
        return getAllTablesWithout(SQLG_SCHEMA_SCHEMA_TABLES);
    }

    public Map<String, Map<String, PropertyType>> getAllTablesWithout(List<String> filter) {
        return this.topology.getAllTablesWithout(filter);
    }

    public Map<String, Map<String, PropertyType>> getAllTablesFrom(List<String> selectFrom) {
        return this.topology.getAllTablesFrom(selectFrom);
    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Preconditions.checkArgument(schemaTable.getTable().startsWith(VERTEX_PREFIX) || schemaTable.getTable().startsWith(EDGE_PREFIX), "BUG: SchemaTable.table must start with edge or vertex prefix.");
        return this.topology.getTableFor(schemaTable);
    }

    private boolean isLockedByCurrentThread() {
        return ((ReentrantLock) this.schemaLock).isHeldByCurrentThread();
    }

}
