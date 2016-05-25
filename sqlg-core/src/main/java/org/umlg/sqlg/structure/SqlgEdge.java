package org.umlg.sqlg.structure;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.base.Splitter;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.BaseSqlgStrategy;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 5:41 AM
 */
public class SqlgEdge extends SqlgElement implements Edge {

    public static final String IN_OR_OUT_VERTEX_ID_NOT_SET = "in or out vertex id not set!!!!";
    private Logger logger = LoggerFactory.getLogger(SqlgEdge.class.getName());
    private SqlgVertex inVertex;
    private SqlgVertex outVertex;

    public SqlgEdge(SqlgGraph sqlgGraph, boolean complete, String schema, String table, SqlgVertex inVertex, SqlgVertex outVertex, Object... keyValues) {
        super(sqlgGraph, schema, table);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
        try {
            insertEdge(complete, keyValues);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public SqlgEdge(SqlgGraph sqlgGraph, Long id, String schema, String table, SqlgVertex inVertex, SqlgVertex outVertex, Object... keyValues) {
        super(sqlgGraph, id, schema, table);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    public SqlgEdge(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        super(sqlgGraph, id, schema, table);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id());
        this.sqlgGraph.tx().readWrite();
        return super.property(key, value);
    }

    private Iterator<Vertex> internalGetVertices(Direction direction) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(getOutVertex());
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(getInVertex());
        return vertices.iterator();
    }

    @Override
    public void remove() {
        this.sqlgGraph.tx().readWrite();
        if (this.removed)
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            this.sqlgGraph.tx().getBatchManager().removeEdge(this.schema, this.table, this);
        } else {
            super.remove();
        }

    }

    public SqlgVertex getInVertex() {
        if (this.inVertex == null) {
            load();
        }
        return this.inVertex;
    }

    public SqlgVertex getOutVertex() {
        if (this.outVertex == null) {
            load();
        }
        return this.outVertex;
    }

    @Override
    public String toString() {
        if (this.inVertex == null) {
            load();
        }
        return StringFactory.edgeString(this);
    }

    protected void insertEdge(boolean complete, Object... keyValues) throws SQLException {
        Map<String, Object> keyValueMap = SqlgUtil.transformToInsertValues(keyValues);
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            internalBatchAddEdge(complete, keyValueMap);
        } else {
            internalAddEdge(keyValueMap);
        }
        //Cache the properties
        this.properties.putAll(keyValueMap);
    }

    private void internalBatchAddEdge(boolean complete, Map<String, Object> keyValueMap) {
        this.sqlgGraph.tx().getBatchManager().addEdge(complete, this, this.outVertex, this.inVertex, keyValueMap);
    }

    private void internalAddEdge(Map<String, Object> keyValueMap) throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + this.table));
        sql.append(" (");
        int i = 1;
        Map<String, PropertyType> columnPropertyTypeMap = this.sqlgGraph.getSchemaManager().getAllTables().get(getSchemaTablePrefixed().toString());
        for (String column : keyValueMap.keySet()) {
            PropertyType propertyType = columnPropertyTypeMap.get(column);
            String[] sqlDefinitions = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
            int count = 1;
            for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                if (count > 1) {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2]));
                } else {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
                }
                if (count++ < sqlDefinitions.length) {
                    sql.append(",");
                }
            }
            if (i++ < keyValueMap.size()) {
                sql.append(", ");
            }
        }
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        }
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.inVertex.schema + "." + this.inVertex.table + SchemaManager.IN_VERTEX_COLUMN_END));
        sql.append(", ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.outVertex.schema + "." + this.outVertex.table + SchemaManager.OUT_VERTEX_COLUMN_END));
        sql.append(") VALUES (");
        i = 1;
        for (String column : keyValueMap.keySet()) {
            PropertyType propertyType = columnPropertyTypeMap.get(column);
            String[] sqlDefinitions = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
            int count = 1;
            for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                if (count > 1) {
                    sql.append("?");
                } else {
                    sql.append("?");
                }
                if (count++ < sqlDefinitions.length) {
                    sql.append(",");
                }
            }
            if (i++ < keyValueMap.size()) {
                sql.append(", ");
            }
        }
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        }
        sql.append("?, ?");
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        i = 1;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            i = SqlgUtil.setKeyValuesAsParameter(this.sqlgGraph, i, conn, preparedStatement, keyValueMap);
            preparedStatement.setLong(i++, this.inVertex.recordId.getId());
            preparedStatement.setLong(i, this.outVertex.recordId.getId());
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), generatedKeys.getLong(1));
            } else {
                throw new RuntimeException("Could not retrieve the id after an insert into " + SchemaManager.VERTICES);
            }
        }

    }

    //TODO this needs optimizing, an edge created in the transaction need not go to the db to load itself again
    @Override
    protected void load() {
        //recordId can be null when in batchMode
        if (recordId != null && this.properties.isEmpty()) {
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + this.table));
            sql.append(" WHERE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" = ?");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.setLong(1, this.recordId.getId());
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    loadResultSet(resultSet);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void loadResultSet(ResultSet resultSet, SchemaTableTree schemaTableTree) throws SQLException {
        SchemaTable inVertexColumnName;
        SchemaTable outVertexColumnName;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            String properName = schemaTableTree.getThreadLocalAliasColumnNameMap().get(columnName);
            if (properName == null) {
                properName = columnName;
            }
            String[] splittedColumn = properName.split(SchemaTableTree.ALIAS_SEPARATOR);
            if (!properName.contains(BaseSqlgStrategy.PATH_LABEL_SUFFIX) && (splittedColumn.length < 4 || (splittedColumn[3].endsWith(SchemaManager.IN_VERTEX_COLUMN_END) || splittedColumn[3].endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)))) {

                Object o = resultSet.getObject(columnName);
                String name = schemaTableTree.propertyNameFromAlias(properName);

                //Optimized!!, using String.replace is slow
                Iterator<String> split = Splitter.on(SchemaTableTree.ALIAS_SEPARATOR).split(name).iterator();
                name = split.next();
                if (split.hasNext()) {
                    name += "." + split.next() + "." + split.next();
                }

                if (!name.equals("ID") &&
                        !Objects.isNull(o) &&
                        !name.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END) &&
                        !name.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {

                    loadProperty(resultSet, name, o, schemaTableTree.getThreadLocalColumnNameAliasMap());

                }
                if (!Objects.isNull(o)) {
                    if (name.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                        inVertexColumnName = SchemaTable.from(this.sqlgGraph, name, this.sqlgGraph.getSqlDialect().getPublicSchema());
                        Long inId = resultSet.getLong(columnName);
                        this.inVertex = SqlgVertex.of(this.sqlgGraph, inId, inVertexColumnName.getSchema(), SqlgUtil.removeTrailingInId(inVertexColumnName.getTable()));
                    } else if (name.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                        outVertexColumnName = SchemaTable.from(this.sqlgGraph, name, this.sqlgGraph.getSqlDialect().getPublicSchema());
                        Long outId = resultSet.getLong(columnName);
                        this.outVertex = SqlgVertex.of(this.sqlgGraph, outId, outVertexColumnName.getSchema(), SqlgUtil.removeTrailingOutId(outVertexColumnName.getTable()));
                    }
                }
            }
        }
        if (this.inVertex == null || this.outVertex == null) {
            throw new IllegalStateException(IN_OR_OUT_VERTEX_ID_NOT_SET);
        }
    }

    @Override
    public void loadLabeledResultSet(ResultSet resultSet, Multimap<String, Integer> columnMap, SchemaTableTree schemaTableTree) throws SQLException {
        SchemaTable inVertexColumnName;
        SchemaTable outVertexColumnName;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            String properName = schemaTableTree.getThreadLocalAliasColumnNameMap().get(columnName);
            if (properName == null) {
                properName = columnName;
            }
            Object o = resultSet.getObject(columnName);
            if (schemaTableTree.containsLabelledColumn(properName)) {
                String name = schemaTableTree.propertyNameFromLabeledAlias(properName);

                if (!name.equals("ID") &&
                        !Objects.isNull(o) &&
                        !name.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END) &&
                        !name.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {

                    loadProperty(resultSet, name, o, schemaTableTree.getThreadLocalColumnNameAliasMap());
                } else if (!Objects.isNull(o)) {
                    if (name.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                        inVertexColumnName = SchemaTable.from(this.sqlgGraph, name, this.sqlgGraph.getSqlDialect().getPublicSchema());
                        Long inId = resultSet.getLong(columnName);
                        this.inVertex = SqlgVertex.of(this.sqlgGraph, inId, inVertexColumnName.getSchema(), SqlgUtil.removeTrailingInId(inVertexColumnName.getTable()));
                    } else if (name.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                        outVertexColumnName = SchemaTable.from(this.sqlgGraph, name, this.sqlgGraph.getSqlDialect().getPublicSchema());
                        Long outId = resultSet.getLong(columnName);
                        this.outVertex = SqlgVertex.of(this.sqlgGraph, outId, outVertexColumnName.getSchema(), SqlgUtil.removeTrailingOutId(outVertexColumnName.getTable()));
                    }
                }
            }
        }
        if (this.inVertex == null || this.outVertex == null) {
            throw new IllegalStateException(IN_OR_OUT_VERTEX_ID_NOT_SET);
        }
    }

    @Override
    public void loadResultSet(ResultSet resultSet) throws SQLException {
        SchemaTable inVertexColumnName = null;
        SchemaTable outVertexColumnName = null;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            Object o = resultSet.getObject(columnName);
            if (!columnName.equals("ID") &&
                    !Objects.isNull(o) &&
                    !columnName.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END) &&
                    !columnName.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {

                loadProperty(resultSet, columnName, o, ArrayListMultimap.create());

            }
            if (!Objects.isNull(o)) {
                if (columnName.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                    inVertexColumnName = SchemaTable.from(this.sqlgGraph, columnName, this.sqlgGraph.getSqlDialect().getPublicSchema());
                } else if (columnName.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                    outVertexColumnName = SchemaTable.from(this.sqlgGraph, columnName, this.sqlgGraph.getSqlDialect().getPublicSchema());
                }
            }
        }
        if (inVertexColumnName == null || outVertexColumnName == null) {
            throw new IllegalStateException(IN_OR_OUT_VERTEX_ID_NOT_SET);
        }
        Long inId = resultSet.getLong(inVertexColumnName.getSchema() + "." + inVertexColumnName.getTable());
        Long outId = resultSet.getLong(outVertexColumnName.getSchema() + "." + outVertexColumnName.getTable());

        this.inVertex = SqlgVertex.of(this.sqlgGraph, inId, inVertexColumnName.getSchema(), SqlgUtil.removeTrailingInId(inVertexColumnName.getTable()));
        this.outVertex = SqlgVertex.of(this.sqlgGraph, outId, outVertexColumnName.getSchema(), SqlgUtil.removeTrailingOutId(outVertexColumnName.getTable()));
    }


    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return (Iterator) super.properties(propertyKeys);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        SqlgEdge.this.sqlgGraph.tx().readWrite();
        return internalGetVertices(direction);
    }

    @Override
    SchemaTable getSchemaTablePrefixed() {
        return SchemaTable.of(this.getSchema(), SchemaManager.EDGE_PREFIX + this.getTable());
    }
}
