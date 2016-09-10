package org.umlg.sqlg.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class VertexLabel {

    private Logger logger = LoggerFactory.getLogger(VertexLabel.class.getName());
    private Schema schema;
    private String label;
    private Map<String, EdgeLabel> inEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> outEdgeLabels = new HashMap<>();
    private Map<String, Property> properties = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedInEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedOutEdgeLabels = new HashMap<>();
    private Map<String, Property> uncommittedProperties = new HashMap<>();

    public static VertexLabel createVertexLabel(SqlgGraph sqlgGraph, Schema schema, String label, ConcurrentHashMap<String, PropertyType> columns) {
        VertexLabel vertexLabel = new VertexLabel(schema, label, columns);
        vertexLabel.createVertexLabelOnDb(sqlgGraph, columns);
        TopologyManager.addVertexLabel(sqlgGraph, schema.getName(), label, columns);
        return vertexLabel;
    }

    public static VertexLabel metaAddVertexLabel(Schema schema, String label, ConcurrentHashMap<String, PropertyType> columns) {
        VertexLabel vertexLabel = new VertexLabel(schema, label, columns);
        return vertexLabel;
    }

    private VertexLabel(Schema schema, String label, ConcurrentHashMap<String, PropertyType> columns) {
        this.schema = schema;
        this.label = label;
        for (Map.Entry<String, PropertyType> propertyEntry : columns.entrySet()) {
            Property property = new Property(propertyEntry.getKey(), propertyEntry.getValue());
            this.properties.put(propertyEntry.getKey(), property);
        }
    }

    private void createVertexLabelOnDb(SqlgGraph sqlgGraph, Map<String, PropertyType> columns) {
        StringBuilder sql = new StringBuilder(sqlgGraph.getSqlDialect().createTableStatement());
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema.getName()));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.label));
        sql.append(" (");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(sqlgGraph.getSqlDialect().getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(sqlgGraph, columns, sql);
        sql.append(")");
        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void buildColumns(SqlgGraph sqlgGraph, Map<String, PropertyType> columns, StringBuilder sql) {
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

    public void afterCommit() {

        for (Iterator<Map.Entry<String, Property>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Property> entry = it.next();
            this.properties.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }

    }

    public void afterRollback() {

        for (Iterator<Map.Entry<String, Property>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Property> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }

    }

    public boolean existOutEdgeLabel(String outVertexTable) {
        return false;
    }
}
