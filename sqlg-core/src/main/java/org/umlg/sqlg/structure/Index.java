package org.umlg.sqlg.structure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;

/**
 * Date: 2016/11/26
 * Time: 7:35 PM
 */
public class Index {

    private Logger logger = LoggerFactory.getLogger(Index.class.getName());
    private String name;
    private AbstractLabel abstractLabel;
    private IndexType indexType;
    private List<PropertyColumn> properties = new ArrayList<>();
    private IndexType uncommittedIndexType;
    private List<PropertyColumn> uncommittedProperties = new ArrayList<>();

    Index(String name, IndexType indexType, AbstractLabel abstractLabel, List<PropertyColumn> properties) {
        this.name = name;
        this.uncommittedIndexType = indexType;
        this.abstractLabel = abstractLabel;
        this.uncommittedProperties.addAll(properties);
    }

    public String getName() {
        return name;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    void afterCommit() {
        if (this.abstractLabel.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            this.indexType = this.uncommittedIndexType;
            Iterator<PropertyColumn> propertyColumnIterator = this.uncommittedProperties.iterator();
            while (propertyColumnIterator.hasNext()) {
                PropertyColumn propertyColumn = propertyColumnIterator.next();
                this.properties.add(propertyColumn);
                propertyColumn.afterCommit();
                propertyColumnIterator.remove();
            }
            this.uncommittedIndexType = null;
        }
    }

    void afterRollback() {
        if (this.abstractLabel.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            this.uncommittedIndexType = null;
            this.uncommittedProperties.clear();
        }
    }

    private void addIndex(SqlgGraph sqlgGraph, SchemaTable schemaTable, IndexType indexType, List<PropertyColumn> properties) {
        String prefix = this.abstractLabel instanceof VertexLabel ? VERTEX_PREFIX : EDGE_PREFIX;
        StringBuilder sql = new StringBuilder("CREATE ");
        if (indexType == IndexType.UNIQUE) {
            sql.append("UNIQUE ");
        }
        sql.append("INDEX");
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        sql.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(schemaTable, prefix, properties.stream().map(PropertyColumn::getName).collect(Collectors.toList()))));
        sql.append(" ON ");
        sql.append(sqlDialect.maybeWrapInQoutes(schemaTable.getSchema()));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(prefix + schemaTable.getTable()));
        sql.append(" (");
        int count = 1;
        for (PropertyColumn property : properties) {
            sql.append(sqlDialect.maybeWrapInQoutes(property.getName()));
            if (count++ < properties.size()) {
                sql.append(",");
            }
        }
        sql.append(")");
        if (sqlDialect.needsSemicolon()) {
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

    public static Index createIndex(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String indexName, IndexType indexType, List<PropertyColumn> properties) {
        Index index = new Index(indexName, indexType, abstractLabel, properties);
        SchemaTable schemaTable = SchemaTable.of(abstractLabel.getSchema().getName(), abstractLabel.getLabel());
        index.addIndex(sqlgGraph, schemaTable, indexType, properties);
        TopologyManager.addIndex(sqlgGraph, abstractLabel, index, indexType, properties);
        return index;
    }
}
