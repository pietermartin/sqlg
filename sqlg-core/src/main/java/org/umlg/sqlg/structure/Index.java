package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;

/**
 * Date: 2016/11/26
 * Time: 7:35 PM */
public class Index implements TopologyInf {

    private Logger logger = LoggerFactory.getLogger(Index.class.getName());
    private String name;
    private boolean committed = true;
    private AbstractLabel abstractLabel;
    private IndexType indexType;
    private List<PropertyColumn> properties = new ArrayList<>();
    private IndexType uncommittedIndexType;
    private List<PropertyColumn> uncommittedProperties = new ArrayList<>();

    /**
     * create uncommitted index
     *
     * @param name
     * @param indexType
     * @param abstractLabel
     * @param properties
     */
    Index(String name, IndexType indexType, AbstractLabel abstractLabel, List<PropertyColumn> properties) {
        this.name = name;
        this.uncommittedIndexType = indexType;
        this.abstractLabel = abstractLabel;
        this.uncommittedProperties.addAll(properties);
    }

    /**
     * create a committed index (when loading topology from existing schema)
     *
     * @param name
     * @param indexType
     * @param abstractLabel
     */
    Index(String name, IndexType indexType, AbstractLabel abstractLabel) {
        this.name = name;
        this.indexType = indexType;
        this.abstractLabel = abstractLabel;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * add a committed property (when loading topology from existing schema)
     *
     * @param property
     */
    void addProperty(PropertyColumn property) {
        this.properties.add(property);
    }

    void afterCommit() {
        this.indexType = this.uncommittedIndexType;
        Iterator<PropertyColumn> propertyColumnIterator = this.uncommittedProperties.iterator();
        while (propertyColumnIterator.hasNext()) {
            PropertyColumn propertyColumn = propertyColumnIterator.next();
            this.properties.add(propertyColumn);
            propertyColumn.afterCommit();
            propertyColumnIterator.remove();
        }
        this.uncommittedIndexType = null;
        this.committed = true;
    }

    void afterRollback() {
        this.uncommittedIndexType = null;
        this.uncommittedProperties.clear();
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

    protected Optional<JsonNode> toNotifyJson() {
        Preconditions.checkState(this.abstractLabel.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedProperties.isEmpty());
        ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        result.put("name", this.name);
        result.put("indexType", this.uncommittedIndexType.name());
        ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (PropertyColumn property : this.uncommittedProperties) {
            propertyArrayNode.add(property.toNotifyJson());
        }
        result.set("uncommittedProperties", propertyArrayNode);
        return Optional.of(result);
    }

    public static Index fromNotifyJson(AbstractLabel abstractLabel, JsonNode indexNode) {
        IndexType indexType = IndexType.valueOf(indexNode.get("indexType").asText());
        String name = indexNode.get("name").asText();
        ArrayNode propertiesNode = (ArrayNode) indexNode.get("uncommittedProperties");
        List<PropertyColumn> properties = new ArrayList<>();
        for (JsonNode propertyNode : propertiesNode) {
            String propertyName = propertyNode.get("name").asText();
            PropertyType propertyType = PropertyType.valueOf(propertyNode.get("propertyType").asText());
            Optional<PropertyColumn> propertyColumnOptional = abstractLabel.getProperty(propertyName);
            Preconditions.checkState(propertyColumnOptional.isPresent(), "BUG: property %s for PropertyType %s not found.", propertyName, propertyType.name());
            //noinspection OptionalGetWithoutIsPresent
            properties.add(propertyColumnOptional.get());
        }
        Index index = new Index(name, indexType, abstractLabel, properties);
        return index;
    }

    static Index createIndex(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String indexName, IndexType indexType, List<PropertyColumn> properties) {
        Index index = new Index(indexName, indexType, abstractLabel, properties);
        SchemaTable schemaTable = SchemaTable.of(abstractLabel.getSchema().getName(), abstractLabel.getLabel());
        index.addIndex(sqlgGraph, schemaTable, indexType, properties);
        TopologyManager.addIndex(sqlgGraph, abstractLabel, index, indexType, properties);
        index.committed = false;
        return index;
    }

    List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException {
        List<Topology.TopologyValidationError> validationErrors = new ArrayList<>();
        try (ResultSet propertyRs = metadata.getIndexInfo(null, this.abstractLabel.getSchema().getName(), this.abstractLabel.getPrefix() + this.abstractLabel.getLabel(), false, false)) {
            Map<String, List<String>> indexColumns = new HashMap<>();
            while (propertyRs.next()) {
                String columnName = propertyRs.getString("COLUMN_NAME");
                String indexName = propertyRs.getString("INDEX_NAME");
                List<String> columnNames;
                if (!indexColumns.containsKey(indexName)) {
                    columnNames = new ArrayList<>();
                    indexColumns.put(indexName, columnNames);
                } else {
                    columnNames = indexColumns.get(indexName);
                }
                columnNames.add(columnName);
            }
            if (!indexColumns.containsKey(this.getName())) {
                validationErrors.add(new Topology.TopologyValidationError(this));
            }
        }
        return validationErrors;

    }
    
    public AbstractLabel getParentLabel() {
		return abstractLabel;
	}
}
