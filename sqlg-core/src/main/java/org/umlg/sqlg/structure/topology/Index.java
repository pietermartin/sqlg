package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.text.RandomStringGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.util.ThreadLocalList;

import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * Date: 2016/11/26
 * Time: 7:35 PM
 */
public class Index implements TopologyInf {

    private static final Logger logger = LoggerFactory.getLogger(Index.class);
    private final String name;
    private boolean committed = true;
    private final AbstractLabel abstractLabel;
    private IndexType indexType;
    private final List<PropertyColumn> properties = new ArrayList<>();
    private IndexType uncommittedIndexType;
    private final List<PropertyColumn> uncommittedProperties = new ThreadLocalList<>();

    /**
     * create uncommitted index
     *
     * @param name
     * @param indexType
     * @param abstractLabel
     * @param properties
     */
    private Index(String name, IndexType indexType, AbstractLabel abstractLabel, List<PropertyColumn> properties) {
        this.name = name;
        this.indexType = indexType;
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
    public String toString() {
        return getName();
    }


    @Override
    public int hashCode() {
        return (this.abstractLabel.getName() + this.getName()).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Index)) {
            return false;
        }
        Index otherIndex = (Index) other;
        return this.abstractLabel.equals(otherIndex.abstractLabel) && this.name.equals(otherIndex.name);
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

    void createIndex(SqlgGraph sqlgGraph, SchemaTable schemaTable, String name) {
        StringBuilder sql = new StringBuilder("CREATE ");
        if (IndexType.UNIQUE.equals(getIndexType())) {
            sql.append("UNIQUE ");
        }
        sql.append("INDEX ");
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        sql.append(sqlDialect.maybeWrapInQoutes(name));
        sql.append(" ON ");
        sql.append(sqlDialect.maybeWrapInQoutes(schemaTable.getSchema()));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(schemaTable.getTable()));


        if (this.indexType.isGIN()) {
            sql.append(" USING GIN");
        }

        sql.append(" (");
        List<PropertyColumn> props = getProperties();
        if (IndexType.GIN_FULLTEXT.equals(getIndexType().getName())) {
            sql.append("to_tsvector(");
            String conf = indexType.getProperties().get(IndexType.GIN_CONFIGURATION);
            if (conf != null) {
                sql.append("'").append(conf).append("'"); // need single quotes, no double
                sql.append(",");
            }
            int count = 1;
            for (PropertyColumn property : props) {
                sql.append(sqlDialect.maybeWrapInQoutes(property.getName()));
                if (count++ < props.size()) {
                    sql.append(" || ' ' || ");
                }
            }
            sql.append(")");
        } else {
            int count = 1;
            for (PropertyColumn property : props) {
                sql.append(sqlDialect.maybeWrapInQoutes(property.getName()));
                //This is for mariadb. It needs to know how many characters to index.
                if (property.getPropertyType().isString() && sqlgGraph.getSqlDialect().requiresIndexLengthLimit()) {
                    //This number is for MariaDb TEXT data type.
                    //192 crashes with "Caused by: java.sql.SQLException: Specified key was too long; max key length is 767 bytes"
                    //Some or other Innodb byte count magic I can't be bothered to understand.
                    sql.append("(191)");
                }
                if (count++ < props.size()) {
                    sql.append(", ");
                }
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

    Optional<JsonNode> toNotifyJson() {
        Preconditions.checkState(this.abstractLabel.getSchema().getTopology().isSchemaChanged() && !this.uncommittedProperties.isEmpty());
        ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        result.put("name", this.name);
        result.set("indexType", this.uncommittedIndexType.toNotifyJson());
        ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (PropertyColumn property : this.uncommittedProperties) {
            propertyArrayNode.add(property.toNotifyJson());
        }
        result.set("uncommittedProperties", propertyArrayNode);
        return Optional.of(result);
    }

    public static Index fromNotifyJson(AbstractLabel abstractLabel, JsonNode indexNode) {
        IndexType indexType = IndexType.fromNotifyJson(indexNode.get("indexType"));
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
        return new Index(name, indexType, abstractLabel, properties);
    }

    static Index createIndex(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String indexName, IndexType indexType, List<PropertyColumn> properties) {
        Index index = new Index(indexName, indexType, abstractLabel, properties);
        SchemaTable schemaTable = SchemaTable.of(abstractLabel.getSchema().getName(), abstractLabel.getLabel());
        //For partitioned tables the index is on each partition.
        //It is created when the partition is created.
        if (!abstractLabel.isPartition()) {
            String prefix = abstractLabel instanceof VertexLabel ? VERTEX_PREFIX : EDGE_PREFIX;
            index.createIndex(sqlgGraph, schemaTable.withPrefix(prefix), index.getName());
        } else {
            for (Partition partition : abstractLabel.getPartitions().values()) {
                partition.createIndexOnLeafPartitions(index);
            }
        }
        TopologyManager.addIndex(sqlgGraph, index);
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

    public List<PropertyColumn> getProperties() {
        List<PropertyColumn> props = new ArrayList<>(properties);
        if (this.getParentLabel().getSchema().getTopology().isSchemaChanged()) {
            props.addAll(uncommittedProperties);
        }
        return Collections.unmodifiableList(props);
    }

    /**
     * delete the index from the database
     *
     * @param sqlgGraph
     */
    void delete(SqlgGraph sqlgGraph) {
        String sql = sqlgGraph.getSqlDialect().dropIndex(sqlgGraph, getParentLabel(), getName());
        if (logger.isDebugEnabled()) {
            logger.debug(sql);
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove(boolean preserveData) {
        getParentLabel().removeIndex(this, preserveData);
    }

    public static String generateName(SqlDialect sqlDialect) {
        RandomStringGenerator generator = new RandomStringGenerator.Builder()
                .withinRange('a', 'z').build();
        return generator.generate(sqlDialect.getMaximumIndexNameLength());
    }
}
