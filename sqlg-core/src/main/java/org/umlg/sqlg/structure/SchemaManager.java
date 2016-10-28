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
import org.umlg.sqlg.topology.Topology;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Date: 2014/07/12
 * Time: 11:02 AM
 */
public class SchemaManager {

    private Topology topology;
    public static final String GIVEN_TABLES_MUST_NOT_BE_NULL = "Given tables must not be null";
    public static final String GIVEN_TABLE_MUST_NOT_BE_NULL = "Given table must not be null";
    public static final String CREATED_ON = "createdOn";
    public static final String SCHEMA_VERTEX_DISPLAY = "schemaVertex";
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
     * Table storing the logs.
     */
    public static final String SQLG_SCHEMA_LOG = "log";
    public static final String SQLG_SCHEMA_LOG_TIMESTAMP = "timestamp";
    public static final String SQLG_SCHEMA_LOG_LOG = "log";
    public static final String SQLG_SCHEMA_LOG_PID = "pid";

    /**
     * Property table's type property
     */
    public static final String SQLG_SCHEMA_PROPERTY_TYPE = "type";
    public static final String SQLG_SCHEMA_PROPERTY_INDEX_TYPE = "index_type";

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
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_LOG,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE
    );

    private SqlgGraph sqlgGraph;
    private SqlDialect sqlDialect;
    private SqlSchemaChangeDialect sqlSchemaChangeDialect;
    private boolean distributed;

    SchemaManager(SqlgGraph sqlgGraph, SqlDialect sqlDialect, Configuration configuration) {
        this.sqlgGraph = sqlgGraph;
        this.topology = new Topology(this.sqlgGraph);

        this.sqlDialect = sqlDialect;
        this.distributed = configuration.getBoolean(SqlgGraph.DISTRIBUTED, false);
        if (this.distributed) {
            this.sqlSchemaChangeDialect = (SqlSchemaChangeDialect) sqlDialect;
            this.sqlSchemaChangeDialect.registerListener(sqlgGraph);
        }

        this.sqlgGraph.tx().beforeCommit(() -> this.topology.beforeCommit());
        this.sqlgGraph.tx().afterCommit(() -> this.topology.afterCommit());

        this.sqlgGraph.tx().afterRollback(() -> {
            if (this.sqlDialect.supportsTransactionalSchema()) {
                this.topology.afterRollback();
            } else {
                this.topology.afterCommit();
            }
        });
    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    public Topology getTopology() {
        return topology;
    }

    void ensureVertexTableExist(final String schema, final String label, final Map<String, PropertyType> columns) {
        this.topology.ensureVertexTableExist(schema, label, columns);
    }

    void ensureVertexTableExist(final String schema, final String label, final Object... keyValues) {
        final Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        ensureVertexTableExist(schema, label, columns);
    }

    public SchemaTable ensureEdgeTableExist(final String edgeLabelName, final SchemaTable foreignKeyOut, final SchemaTable foreignKeyIn, final Map<String, PropertyType> columns) {
        return this.topology.ensureEdgeTableExist(edgeLabelName, foreignKeyOut, foreignKeyIn, columns);
    }

    public SchemaTable ensureEdgeTableExist(final String edgeLabelName, final SchemaTable foreignKeyOut, final SchemaTable foreignKeyIn, Object... keyValues) {
        final Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        return ensureEdgeTableExist(edgeLabelName, foreignKeyOut, foreignKeyIn, columns);
    }

    void ensureVertexTemporaryTableExist(final String schema, final String table, final Map<String, PropertyType> columns) {
        this.topology.ensureVertexTemporaryTableExist(schema, table, columns);
    }

    void ensureVertexTemporaryTableExist(final String schema, final String table, final Object... keyValues) {
        final Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        ensureVertexTemporaryTableExist(schema, table, columns);
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

    public void close() {
        if (this.sqlSchemaChangeDialect != null)
            this.sqlSchemaChangeDialect.unregisterListener();
    }

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
                TopologyManager.addVertexLabel(this.sqlgGraph, schema, table.substring(SchemaManager.VERTEX_PREFIX.length()), columns);
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
                            TopologyManager.addEdgeLabel(this.sqlgGraph, schema, table, inOutLabels.getRight(), inOutLabels.getLeft(), columns);
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
     * Load the schema from the topology.
     */
    private void loadUserSchema() {
        this.topology.loadUserSchema();
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
        try (Connection conn = this.sqlgGraph.getSqlgDataSource().get(this.sqlgGraph.getJdbcUrl()).getConnection()) {
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

    public boolean tableExist(String schema, String table) {
        SchemaTable schemaTable = SchemaTable.of(schema, table);
        return !getTableFor(schemaTable).isEmpty();
    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Preconditions.checkArgument(schemaTable.getTable().startsWith(VERTEX_PREFIX) || schemaTable.getTable().startsWith(EDGE_PREFIX), "BUG: SchemaTable.table must start with edge or vertex prefix.");
        return this.topology.getTableFor(schemaTable);
    }

    public void createTempTable(String tmpTableIdentified, Map<String, PropertyType> columns) {
        this.topology.createTempTable(tmpTableIdentified, columns);
    }

    public void merge(int pid, LocalDateTime timestamp) {
        this.topology.fromNotifyJson(pid, timestamp);
    }
}
