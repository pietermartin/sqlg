package org.umlg.sqlg.topology;

import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;

/**
 * Date: 2016/09/14
 * Time: 11:19 AM
 */
public abstract class AbstractElement {

    private String label;
    protected Map<String, Property> properties = new HashMap<>();
    protected Map<String, Property> uncommittedProperties = new HashMap<>();

    public AbstractElement(String label, Map<String, PropertyType> columns) {
        this.label = label;
        for (Map.Entry<String, PropertyType> propertyEntry : columns.entrySet()) {
            Property property = new Property(propertyEntry.getKey(), propertyEntry.getValue());
            this.properties.put(propertyEntry.getKey(), property);
        }
    }

    public String getLabel() {
        return this.label;
    }

    protected void buildColumns(SqlgGraph sqlgGraph, Map<String, PropertyType> columns, StringBuilder sql) {
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

}
