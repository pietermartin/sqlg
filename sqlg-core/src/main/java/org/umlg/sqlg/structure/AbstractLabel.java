package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_PROPERTY_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_PROPERTY_TYPE;

/**
 * Date: 2016/09/14
 * Time: 11:19 AM
 */
public abstract class AbstractLabel {

    private Logger logger = LoggerFactory.getLogger(AbstractLabel.class.getName());
    protected String label;
    protected Map<String, PropertyColumn> properties = new HashMap<>();
    Map<String, PropertyColumn> uncommittedProperties = new HashMap<>();
    protected Map<String, Index> indexes = new HashMap<>();
    Map<String, Index> uncommittedIndexes = new HashMap<>();

    AbstractLabel(String label, Map<String, PropertyType> columns) {
        this.label = label;
        for (Map.Entry<String, PropertyType> propertyEntry : columns.entrySet()) {
            PropertyColumn property = new PropertyColumn(this, propertyEntry.getKey(), propertyEntry.getValue());
            this.uncommittedProperties.put(propertyEntry.getKey(), property);
        }
    }

    AbstractLabel(String label) {
        this.label = label;
    }

    public Index ensureIndexExists(final SqlgGraph sqlgGraph, final IndexType indexType, final List<PropertyColumn> properties) {

        String prefix = this instanceof VertexLabel ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX;
        SchemaTable schemaTable = SchemaTable.of(this.getSchema().getName(), this.getLabel());
        String indexName = sqlgGraph.getSqlDialect().indexName(schemaTable, prefix, properties.stream().map(PropertyColumn::getName).collect(Collectors.toList()));

        Optional<Index> indexOptional = this.getIndex(indexName);
        if (!indexOptional.isPresent()) {
            this.getSchema().getTopology().lock();
            indexOptional = this.getIndex(indexName);
            if (!indexOptional.isPresent()) {
                return this.createIndex(sqlgGraph, indexName, indexType, properties);
            } else {
                return indexOptional.get();
            }
        } else {
            return indexOptional.get();
        }
    }

    private Index createIndex(SqlgGraph sqlgGraph, String indexName, IndexType indexType, List<PropertyColumn> properties) {
        Index index = Index.createIndex(sqlgGraph, this, indexName, indexType, properties);
        this.uncommittedIndexes.put(indexName, index);
        return index;
    }

    protected abstract Schema getSchema();

    public String getLabel() {
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

    protected void addColumn(SqlgGraph sqlgGraph, String schema, String table, ImmutablePair<String, PropertyType> keyValue) {
        int count = 1;
        String[] propertyTypeToSqlDefinition = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(keyValue.getRight());
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
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, PropertyColumn> entry = it.next();
                this.properties.put(entry.getKey(), entry.getValue());
                entry.getValue().afterCommit();
                it.remove();
            }
            for (Iterator<Map.Entry<String, Index>> it = this.uncommittedIndexes.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Index> entry = it.next();
                this.indexes.put(entry.getKey(), entry.getValue());
                entry.getValue().afterCommit();
                it.remove();
            }
        }
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.properties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            entry.getValue().afterCommit();
        }
    }

    protected void afterRollback() {
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, PropertyColumn> entry = it.next();
                entry.getValue().afterRollback();
                it.remove();
            }
            for (Iterator<Map.Entry<String, Index>> it = this.uncommittedIndexes.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Index> entry = it.next();
                entry.getValue().afterRollback();
                it.remove();
            }
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
            ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (PropertyColumn property : this.uncommittedProperties.values()) {
                propertyArrayNode.add(property.toNotifyJson());
            }
            return Optional.of(propertyArrayNode);
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
        if (!this.properties.equals(other.properties)) {
            return false;
        }
        return true;
    }

}
