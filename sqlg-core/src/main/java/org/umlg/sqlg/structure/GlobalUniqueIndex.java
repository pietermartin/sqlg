package org.umlg.sqlg.structure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Date: 2016/12/01
 * Time: 10:10 PM
 */
public class GlobalUniqueIndex {

    private Logger logger = LoggerFactory.getLogger(GlobalUniqueIndex.class.getName());
    private String name;
    private Set<PropertyColumn> properties = new HashSet<>();
    private Set<PropertyColumn> uncommittedProperties = new HashSet<>();

    public GlobalUniqueIndex(String name, Set<PropertyColumn> uncommittedProperties) {
        this.name = name;
        this.uncommittedProperties = uncommittedProperties;
    }

    static GlobalUniqueIndex createGlobalUniqueIndex(SqlgGraph sqlgGraph, String globalUniqueIndexName, Set<PropertyColumn> properties) {
        GlobalUniqueIndex globalUniqueIndex = new GlobalUniqueIndex(globalUniqueIndexName, properties);
        globalUniqueIndex.createGlobalUniqueIndexOnDb(sqlgGraph, globalUniqueIndexName, properties);
//        TopologyManager.addGlobalUniqueIndex(sqlgGraph, globalUniqueIndexName, properties);
        return globalUniqueIndex;
    }

    private void createGlobalUniqueIndexOnDb(SqlgGraph sqlgGraph, String globalUniqueIndexName, Set<PropertyColumn> properties) {
        StringBuilder sql = new StringBuilder(sqlgGraph.getSqlDialect().createTableStatement());
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(sqlgGraph.getSqlDialect().getPublicSchema()));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(globalUniqueIndexName));
        sql.append(" (");
        //The properties must all be of the same type
        Map<String, PropertyType> columns = new HashMap<String, PropertyType>() {{
            put(globalUniqueIndexName, properties.iterator().next().getPropertyType());
        }};
        VertexLabel.buildColumns(sqlgGraph, columns, sql);
        sql.append(", PRIMARY KEY (");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(globalUniqueIndexName));
        sql.append("))");
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

    static String globalUniqueIndexName(Set<PropertyColumn> properties) {
        List<PropertyColumn> propertyColumns = new ArrayList<>(properties);
        Collections.sort(propertyColumns, Comparator.comparing(PropertyColumn::getName));
        //noinspection OptionalGetWithoutIsPresent
        return propertyColumns.stream().map(PropertyColumn::getName).reduce((a, b) -> a + "_" + b).get();
    }
}
