package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_PROPERTY_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_PROPERTY_TYPE;

/**
 * Date: 2016/09/14
 * Time: 11:19 AM
 */
public abstract class AbstractLabel implements TopologyInf {

    private Logger logger = LoggerFactory.getLogger(AbstractLabel.class.getName());
    protected boolean committed = true;
    protected String label;
    protected SqlgGraph sqlgGraph;
    protected Map<String, PropertyColumn> properties = new HashMap<>();
    Map<String, PropertyColumn> uncommittedProperties = new HashMap<>();
    protected Map<String, PropertyColumn> globalUniqueIndexProperties = new HashMap<>();
    Map<String, PropertyColumn> uncommittedGlobalUniqueIndexProperties = new HashMap<>();
    private Map<String, Index> indexes = new HashMap<>();
    private Map<String, Index> uncommittedIndexes = new HashMap<>();

    /**
     * Only called for a new vertex/edge label being added.
     *
     * @param label      The vertex or edge's label.
     * @param properties The vertex's properties.
     */
    AbstractLabel(SqlgGraph sqlgGraph, String label, Map<String, PropertyType> properties) {
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        for (Map.Entry<String, PropertyType> propertyEntry : properties.entrySet()) {
            PropertyColumn property = new PropertyColumn(this, propertyEntry.getKey(), propertyEntry.getValue());
            property.setCommitted(false);
            this.uncommittedProperties.put(propertyEntry.getKey(), property);
        }
    }

    AbstractLabel(SqlgGraph sqlgGraph, String label) {
        this.sqlgGraph = sqlgGraph;
        this.label = label;
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    public Index ensureIndexExists(final IndexType indexType, final List<PropertyColumn> properties) {
        String prefix = this instanceof VertexLabel ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX;
        SchemaTable schemaTable = SchemaTable.of(this.getSchema().getName(), this.getLabel());
        String indexName = this.sqlgGraph.getSqlDialect().indexName(schemaTable, prefix, properties.stream().map(PropertyColumn::getName).collect(Collectors.toList()));

        Optional<Index> indexOptional = this.getIndex(indexName);
        if (!indexOptional.isPresent()) {
            this.getSchema().getTopology().lock();
            indexOptional = this.getIndex(indexName);
            if (!indexOptional.isPresent()) {
                return this.createIndex(indexName, indexType, properties);
            } else {
                return indexOptional.get();
            }
        } else {
            return indexOptional.get();
        }
    }

    private Index createIndex(String indexName, IndexType indexType, List<PropertyColumn> properties) {
        Index index = Index.createIndex(this.sqlgGraph, this, indexName, indexType, properties);
        this.uncommittedIndexes.put(indexName, index);
        this.getSchema().getTopology().fire(index, "", TopologyChangeAction.CREATE);
        return index;
    }

    void addIndex(Index i) {
        this.indexes.put(i.getName(), i);
    }

    protected abstract Schema getSchema();

    public String getLabel() {
        return this.label;
    }

    @Override
    public String getName() {
        return this.label;
    }

    public Map<String, PropertyColumn> getProperties() {
        Map<String, PropertyColumn> result = new HashMap<>();
        result.putAll(this.properties);
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedProperties);
        }
        return result;
    }

    public Map<String, PropertyColumn> getGlobalUniqueIndexProperties() {
        Map<String, PropertyColumn> result = new HashMap<>();
        result.putAll(this.globalUniqueIndexProperties);
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedGlobalUniqueIndexProperties);
        }
        return result;
    }

    public Optional<PropertyColumn> getProperty(String key) {
        PropertyColumn propertyColumn = getProperties().get(key);
        if (propertyColumn != null) {
            return Optional.of(propertyColumn);
        } else {
            return Optional.empty();
        }
    }

    public Map<String, Index> getIndexes() {
        Map<String, Index> result = new HashMap<>();
        result.putAll(this.indexes);
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedIndexes);
        }
        return result;
    }

    public Optional<Index> getIndex(String key) {
        Index index = getIndexes().get(key);
        if (index != null) {
            return Optional.of(index);
        } else {
            return Optional.empty();
        }
    }

    Map<String, PropertyType> getPropertyTypeMap() {
        Map<String, PropertyType> result = new HashMap<>();
        this.properties.forEach((k, v) -> result.put(k, v.getPropertyType()));
        if (getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            this.uncommittedProperties.forEach((k, v) -> result.put(k, v.getPropertyType()));
        }
        return result;
    }

    Map<String, PropertyColumn> getUncommittedPropertyTypeMap() {
        if (getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            return this.uncommittedProperties;
        } else {
            return Collections.emptyMap();
        }
    }

    static void buildColumns(SqlgGraph sqlgGraph, Map<String, PropertyType> columns, StringBuilder sql) {
        int i = 1;
        //This is to make the columns sorted
        List<String> keys = new ArrayList<>(columns.keySet());
        Collections.sort(keys);
        for (String column : keys) {
            PropertyType propertyType = columns.get(column);
            int count = 1;
            String[] propertyTypeToSqlDefinition = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
            for (String sqlDefinition : propertyTypeToSqlDefinition) {
                if (count > 1) {
                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2])).append(" ").append(sqlDefinition);
                } else {
                    //The first column existVertexLabel no postfix
                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column)).append(" ").append(sqlDefinition);
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

    protected void addColumn(String schema, String table, ImmutablePair<String, PropertyType> keyValue) {
        int count = 1;
        String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(keyValue.getRight());
        for (String sqlDefinition : propertyTypeToSqlDefinition) {
            StringBuilder sql = new StringBuilder("ALTER TABLE ");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sql.append(" ADD ");
            if (count > 1) {
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(keyValue.getLeft() + keyValue.getRight().getPostFixes()[count - 2]));
            } else {
                //The first column existVertexLabel no postfix
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(keyValue.getLeft()));
            }
            count++;
            sql.append(" ");
            sql.append(sqlDefinition);

            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void addProperty(Vertex propertyVertex) {
        PropertyColumn property = new PropertyColumn(this, propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME), PropertyType.valueOf(propertyVertex.value(SQLG_SCHEMA_PROPERTY_TYPE)));
        this.properties.put(propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME), property);
    }

    void afterCommit() {
        Preconditions.checkState(this.getSchema().getTopology().isWriteLockHeldByCurrentThread(), "Abstract.afterCommit must hold the write lock");
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            this.properties.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedGlobalUniqueIndexProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            this.globalUniqueIndexProperties.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }
        for (Iterator<Map.Entry<String, Index>> it = this.uncommittedIndexes.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Index> entry = it.next();
            this.indexes.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.properties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            entry.getValue().afterCommit();
        }
        this.committed = true;
    }

    void afterRollback() {
        Preconditions.checkState(this.getSchema().getTopology().isWriteLockHeldByCurrentThread(), "Abstract.afterRollback must hold the write lock");
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        this.uncommittedGlobalUniqueIndexProperties.clear();
        for (Iterator<Map.Entry<String, Index>> it = this.uncommittedIndexes.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Index> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.properties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            entry.getValue().afterRollback();
        }
    }

    protected JsonNode toJson() {
        ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (PropertyColumn property : this.properties.values()) {
            propertyArrayNode.add(property.toNotifyJson());
        }
        return propertyArrayNode;
    }

    protected Optional<JsonNode> toNotifyJson() {
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedProperties.isEmpty()) {
            ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
            ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (PropertyColumn property : this.uncommittedProperties.values()) {
                propertyArrayNode.add(property.toNotifyJson());
            }
            ArrayNode indexArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (Index index : this.uncommittedIndexes.values()) {
                //noinspection OptionalGetWithoutIsPresent
                Optional<JsonNode> indexJsonOptional = index.toNotifyJson();
                Preconditions.checkState(indexJsonOptional.isPresent());
                //noinspection OptionalGetWithoutIsPresent
                indexArrayNode.add(indexJsonOptional.get());
            }
            result.set("uncommittedProperties", propertyArrayNode);
            result.set("uncommittedIndexes", indexArrayNode);
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    void fromPropertyNotifyJson(JsonNode vertexLabelJson) {
        ArrayNode propertiesNode = (ArrayNode) vertexLabelJson.get("uncommittedProperties");
        if (propertiesNode != null) {
            for (JsonNode propertyNode : propertiesNode) {
                PropertyColumn property = PropertyColumn.fromNotifyJson(this, propertyNode);
                this.properties.put(property.getName(), property);
            }
        }
        ArrayNode indexNodes = (ArrayNode) vertexLabelJson.get("uncommittedIndexes");
        if (indexNodes != null) {
            for (JsonNode indexNode : indexNodes) {
                Index index = Index.fromNotifyJson(this, indexNode);
                this.indexes.put(index.getName(), index);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof AbstractLabel)) {
            return false;
        }
        AbstractLabel other = (AbstractLabel) o;
        if (!this.label.equals(other.label)) {
            return false;
        }
        return true;
    }

    void addGlobalUniqueIndexToUncommittedProperties(PropertyColumn propertyColumn) {
        this.uncommittedGlobalUniqueIndexProperties.put(propertyColumn.getName(), propertyColumn);
    }

    void addGlobalUniqueIndexToProperties(PropertyColumn propertyColumn) {
        this.globalUniqueIndexProperties.put(propertyColumn.getName(), propertyColumn);
    }

    protected abstract List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException;

    protected abstract String getPrefix();
}
