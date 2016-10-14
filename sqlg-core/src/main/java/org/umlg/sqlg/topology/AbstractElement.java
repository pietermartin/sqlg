package org.umlg.sqlg.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static org.umlg.sqlg.structure.SchemaManager.SQLG_SCHEMA_PROPERTY_NAME;
import static org.umlg.sqlg.structure.SchemaManager.SQLG_SCHEMA_PROPERTY_TYPE;

/**
 * Date: 2016/09/14
 * Time: 11:19 AM
 */
public abstract class AbstractElement {

    private Logger logger = LoggerFactory.getLogger(AbstractElement.class.getName());
    protected String label;
    protected Map<String, Property> properties = new HashMap<>();
    protected Map<String, Property> uncommittedProperties = new HashMap<>();

    public AbstractElement(String label, Map<String, PropertyType> columns) {
        this.label = label;
        for (Map.Entry<String, PropertyType> propertyEntry : columns.entrySet()) {
            Property property = new Property(propertyEntry.getKey(), propertyEntry.getValue());
            this.uncommittedProperties.put(propertyEntry.getKey(), property);
        }
    }

    AbstractElement(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    public Map<String, PropertyType> getPropertyTypeMap() {
        Map<String, PropertyType> result = new HashMap<>();
        for (Map.Entry<String, Property> propertyEntry : this.properties.entrySet()) {
            result.put(propertyEntry.getValue().getName(), propertyEntry.getValue().getPropertyType());
        }
        for (Map.Entry<String, Property> propertyEntry : this.uncommittedProperties.entrySet()) {
            result.put(propertyEntry.getValue().getName(), propertyEntry.getValue().getPropertyType());
        }
        return result;
    }

    protected static void buildColumns(SqlgGraph sqlgGraph, Map<String, PropertyType> columns, StringBuilder sql) {
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
        Property property = new Property(propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME), PropertyType.valueOf(propertyVertex.value(SQLG_SCHEMA_PROPERTY_TYPE)));
        this.properties.put(propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME), property);
    }

    public JsonNode toJson() {
        ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (Property property : this.properties.values()) {
            propertyArrayNode.add(property.toNotifyJson());
        }
        return propertyArrayNode;
    }

    public Optional<JsonNode> toNotifyJson() {
        if (this.uncommittedProperties.size() > 0) {
            ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (Property property : this.uncommittedProperties.values()) {
                propertyArrayNode.add(property.toNotifyJson());
            }
            return Optional.of(propertyArrayNode);
        } else {
            return Optional.empty();
        }
    }

    public void fromPropertyNotifyJson(JsonNode vertexLabelJson) {
        ArrayNode propertiesNode = (ArrayNode) vertexLabelJson.get("uncommittedProperties");
        if (propertiesNode != null) {
            for (JsonNode propertyNode : propertiesNode) {
                Property property = Property.fromNotifyJson(propertyNode);
                this.properties.put(property.getName(), property);
            }
        }
    }

}
