package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Date: 2014/07/12
 * Time: 11:02 AM
 */
public class SchemaManager {

    private Topology topology;

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


    private SqlgGraph sqlgGraph;
    private SqlDialect sqlDialect;

    SchemaManager(SqlgGraph sqlgGraph, Topology topology) {
        this.sqlgGraph = sqlgGraph;
        this.sqlDialect = sqlgGraph.getSqlDialect();
        this.topology = topology;
    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    public Topology getTopology() {
        return topology;
    }

    /**
     * Deletes all tables.
     */
    public void clear() {
        try (Connection conn = this.sqlgGraph.getConnection()) {
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

    @Deprecated
    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels(SchemaTable schemaTable) {
        return this.topology.getTableLabels(schemaTable);
    }

    Map<String, Map<String, PropertyType>> getTables() {
        return this.topology.getAllTables();
    }

    public Map<String, Map<String, PropertyType>> getAllTables() {
//        return getAllTablesWithout(SQLG_SCHEMA_SCHEMA_TABLES);
        return getAllTablesWithout(this.getTopology().getSqlgSchemaAbstractLabels());
    }

    /**
     * Use the {@link Topology} class directly instead.
     * @param filter The objects not to include in the result.
     * @return A map without the filter elements present.
     */
    @Deprecated
    public Map<String, Map<String, PropertyType>> getAllTablesWithout(Set<TopologyInf> filter) {
        return this.topology.getAllTablesWithout(filter);
    }

    public Map<String, Map<String, PropertyType>> getAllTablesFrom(Set<TopologyInf> selectFrom) {
        return this.topology.getAllTablesFrom(selectFrom);
    }

    public boolean tableExist(String schema, String table) {
        SchemaTable schemaTable = SchemaTable.of(schema, table);
        Optional<Schema> schemaOptional = this.topology.getSchema(schemaTable.getSchema());
        return schemaOptional.isPresent() && schemaOptional.get().getVertexLabel(schemaTable.withOutPrefix().getTable()).isPresent();
    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Preconditions.checkArgument(schemaTable.getTable().startsWith(VERTEX_PREFIX) || schemaTable.getTable().startsWith(EDGE_PREFIX), "BUG: SchemaTable.table must start with edge or vertex prefix.");
        return this.topology.getTableFor(schemaTable);
    }

    public void createTempTable(String tmpTableIdentified, Map<String, PropertyType> columns) {
        this.topology.createTempTable(tmpTableIdentified, columns);
    }

    /**
     * @deprecated
     */
    public void ensureVertexColumnExist(String schema, String label, ImmutablePair<String, PropertyType> keyValue) {
        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(keyValue.getLeft(), keyValue.getRight());
        ensureVertexColumnsExist(schema, label, columns);
    }

    /**
     * @deprecated
     */
    public void ensureEdgeColumnExist(String schema, String label, ImmutablePair<String, PropertyType> keyValue) {
        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(keyValue.getLeft(), keyValue.getRight());
        ensureEdgeColumnsExist(schema, label, columns);
    }

    private void ensureVertexColumnsExist(String schema, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        this.topology.ensureVertexLabelPropertiesExist(schema, label, columns);
    }

    private void ensureEdgeColumnsExist(String schema, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        this.topology.ensureEdgePropertiesExist(schema, label, columns);
    }

    void ensureVertexTableExist(final String schema, final String label, final Map<String, PropertyType> columns) {
        this.topology.ensureVertexLabelExist(schema, label, columns);
    }

    void ensureVertexTableExist(final String schema, final String label, final Object... keyValues) {
        final Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        ensureVertexTableExist(schema, label, columns);
    }

    /**
     * @deprecated
     */
    public SchemaTable ensureEdgeTableExist(final String edgeLabelName, final SchemaTable foreignKeyOut, final SchemaTable foreignKeyIn, final Map<String, PropertyType> columns) {
        return this.topology.ensureEdgeLabelExist(edgeLabelName, foreignKeyOut, foreignKeyIn, columns);
    }

    /**
     * @deprecated
     */
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
}
