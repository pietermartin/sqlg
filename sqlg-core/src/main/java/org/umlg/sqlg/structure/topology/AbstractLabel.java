package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2016/09/14
 * Time: 11:19 AM
 */
public abstract class AbstractLabel implements TopologyInf {

    private static Logger logger = LoggerFactory.getLogger(AbstractLabel.class);
    protected boolean committed = true;
    protected String label;
    protected SqlgGraph sqlgGraph;
    protected Map<String, PropertyColumn> properties = new HashMap<>();
    Map<String, PropertyColumn> uncommittedProperties = new HashMap<>();
    Set<String> uncommittedRemovedProperties = new HashSet<>();

    private Map<String, PropertyColumn> globalUniqueIndexProperties = new HashMap<>();
    private Map<String, PropertyColumn> uncommittedGlobalUniqueIndexProperties = new HashMap<>();

    private Map<String, Index> indexes = new HashMap<>();
    private Map<String, Index> uncommittedIndexes = new HashMap<>();
    private Set<String> uncommittedRemovedIndexes = new HashSet<>();

    //Yet another cache to speed meta data up.
    private Map<String, PropertyType> propertyTypeMap;

    /**
     * Indicates if the table is partitioned or not.
     * {@link PartitionType#NONE} indicates a normal non partitioned table.
     * {@link PartitionType#RANGE} and {@link PartitionType#LIST} indicate the type of partitioning.
     */
    protected PartitionType partitionType = PartitionType.NONE;
    protected String partitionExpression;
    protected Map<String, Partition> partitions = new HashMap<>();
    Map<String, Partition> uncommittedPartitions = new HashMap<>();
    Set<String> uncommittedRemovedPartitions = new HashSet<>();

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

    /**
     * Only called for a new partitioned vertex/edge label being added.
     *
     * @param label               The vertex or edge's label.
     * @param properties          The vertex's properties.
     * @param partitionType       The partition type. i.e. RANGE or LIST.
     * @param partitionExpression The sql fragment to express the partition column or expression.
     */
    AbstractLabel(SqlgGraph sqlgGraph, String label, Map<String, PropertyType> properties, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkArgument(partitionType == PartitionType.RANGE || partitionType == PartitionType.LIST, "Only RANGE and LIST partitions are supported. Found %s", partitionType.name());
        Preconditions.checkArgument(!partitionExpression.isEmpty(), "partitionExpression may not be an empty string.");
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        for (Map.Entry<String, PropertyType> propertyEntry : properties.entrySet()) {
            PropertyColumn property = new PropertyColumn(this, propertyEntry.getKey(), propertyEntry.getValue());
            property.setCommitted(false);
            this.uncommittedProperties.put(propertyEntry.getKey(), property);
        }
        this.partitionType = partitionType;
        this.partitionExpression = partitionExpression;
    }

    AbstractLabel(SqlgGraph sqlgGraph, String label) {
        this.sqlgGraph = sqlgGraph;
        this.label = label;
    }

    public abstract void ensurePropertiesExist(Map<String, PropertyType> columns);

    /**
     * Ensures that a RANGE partition exists.
     * @param name The partition's name
     * @param from The RANGE partition's start clause.
     * @param to THe RANGE partition's end clause.
     * @return The {@link Partition}
     */
    public Partition ensurePartitionExists(String name, String from, String to) {
        Objects.requireNonNull(name, "RANGE Partition's \"name\" must not be null");
        Objects.requireNonNull(from, "RANGE Partition's \"from\" must not be null");
        Objects.requireNonNull(to, "RANGE Partition's \"to\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.RANGE, "ensurePartitionExists(String name, String from, String to) can only be called for a RANGE partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (!partitionOptional.isPresent()) {
            getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            if (!partitionOptional.isPresent()) {
                return this.createPartition(name, from, to);
            } else {
                return partitionOptional.get();
            }
        } else {
            return partitionOptional.get();
        }
    }

    /**
     * Ensures that a LIST partition exists.
     * @param name The partition's name.
     * @param in The LIST partition's 'in' clause.
     * @return The {@link Partition}
     */
    public Partition ensurePartitionExists(String name, String in) {
        Objects.requireNonNull(name, "LIST Partition's \"name\" must not be null");
        Objects.requireNonNull(in, "LIST Partition's \"in\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.LIST, "ensurePartitionExists(String name, String ... in) can only be called for a LIST partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (!partitionOptional.isPresent()) {
            getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            if (!partitionOptional.isPresent()) {
                return this.createPartition(name, in);
            } else {
                return partitionOptional.get();
            }
        } else {
            return partitionOptional.get();
        }
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    private Partition createPartition(String name, String from, String to) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createPartition(this.sqlgGraph, this, name, from, to);
        this.uncommittedPartitions.put(name, partition);
        this.getSchema().getTopology().fire(partition, "", TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createPartition(String name, String in) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createPartition(this.sqlgGraph, this, name, in);
        this.uncommittedPartitions.put(name, partition);
        this.getSchema().getTopology().fire(partition, "", TopologyChangeAction.CREATE);
        return partition;
    }

    public Index ensureIndexExists(final IndexType indexType, final List<PropertyColumn> properties) {
        String prefix = this instanceof VertexLabel ? VERTEX_PREFIX : EDGE_PREFIX;
        SchemaTable schemaTable = SchemaTable.of(this.getSchema().getName(), this.getLabel());

        String indexName = this.sqlgGraph.getSqlDialect().indexName(schemaTable, prefix, properties.stream().map(PropertyColumn::getName).collect(Collectors.toList()));
        if (indexName.length() > this.sqlgGraph.getSqlDialect().getMaximumIndexNameLength()) {
            // name was random, need to check the properties list
            for (Index idx : this.getIndexes().values()) {
                if (idx.getProperties().equals(properties)) {
                    return idx;
                }
            }

            this.getSchema().getTopology().lock();
            for (Index idx : this.getIndexes().values()) {
                if (idx.getProperties().equals(properties)) {
                    return idx;
                }
            }
            RandomStringGenerator generator = new RandomStringGenerator.Builder()
                    .withinRange('a', 'z').build();
            indexName = generator.generate(this.sqlgGraph.getSqlDialect().getMaximumIndexNameLength());

            return this.createIndex(indexName, indexType, properties);

        } else {

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

    public abstract Schema getSchema();

    public String getLabel() {
        return this.label;
    }

    @Override
    public String getName() {
        return this.label;
    }


    public String getFullName() {
        return getSchema().getName() + "." + getName();
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public String getPartitionExpression() {
        return partitionExpression;
    }

    public Optional<Partition> getPartition(String name) {
        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() && this.uncommittedRemovedPartitions.contains(name)) {
            return Optional.empty();
        }
        Partition result = null;
        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result = this.uncommittedPartitions.get(name);
        }
        if (result == null) {
            result = this.partitions.get(name);
        }
        return Optional.ofNullable(result);
    }

    public Map<String, Partition> getPartitions() {
        Map<String, Partition> result = new HashMap<>(this.partitions);
        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedPartitions);
            for (String s : this.uncommittedRemovedPartitions) {
                result.remove(s);
            }
        }
        return result;
    }

    public Map<String, PropertyColumn> getProperties() {
        Map<String, PropertyColumn> result = new HashMap<>(this.properties);
        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedProperties);
            for (String s : this.uncommittedRemovedProperties) {
                result.remove(s);
            }
        }
        return result;
    }

    public Map<String, PropertyColumn> getGlobalUniqueIndexProperties() {
        Map<String, PropertyColumn> result = new HashMap<>();
        result.putAll(this.globalUniqueIndexProperties);
        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
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
        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedIndexes);
            for (String i : this.uncommittedRemovedIndexes) {
                result.remove(i);
            }
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
        for (Map.Entry<String, PropertyColumn> propertyEntry : this.properties.entrySet()) {
            result.put(propertyEntry.getKey(), propertyEntry.getValue().getPropertyType());
        }
        if (getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            for (Map.Entry<String, PropertyColumn> uncommittedPropertyEntry : this.uncommittedProperties.entrySet()) {
                result.put(uncommittedPropertyEntry.getKey(), uncommittedPropertyEntry.getValue().getPropertyType());
            }
            for (String s : this.uncommittedRemovedProperties) {
                result.remove(s);
            }
        }
        return result;
    }

    Map<String, PropertyColumn> getUncommittedPropertyTypeMap() {
        if (getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            return this.uncommittedProperties;
        } else {
            return Collections.emptyMap();
        }
    }

    Set<String> getUncommittedRemovedProperties() {
        if (getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            return this.uncommittedRemovedProperties;
        } else {
            return Collections.emptySet();
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
        Preconditions.checkState(this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread());
        PropertyColumn property = new PropertyColumn(this, propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME), PropertyType.valueOf(propertyVertex.value(SQLG_SCHEMA_PROPERTY_TYPE)));
        this.properties.put(propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME), property);
    }

    void addPartition(Vertex partitionVertex) {
        Preconditions.checkState(this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread());
        Partition partition = new Partition(this.sqlgGraph, this, partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                partitionVertex.value(SQLG_SCHEMA_PARTITION_FROM), partitionVertex.value(SQLG_SCHEMA_PARTITION_TO));
        this.partitions.put(partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME), partition);
    }

    void afterCommit() {
        Preconditions.checkState(this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread(), "Abstract.afterCommit must hold the write lock");
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            this.properties.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
            if (this.propertyTypeMap != null) {
                this.propertyTypeMap.clear();
                this.propertyTypeMap = null;
            }
        }
        for (Iterator<String> it = this.uncommittedRemovedProperties.iterator(); it.hasNext(); ) {
            String prop = it.next();
            this.properties.remove(prop);
            it.remove();
            if (this.propertyTypeMap != null) {
                this.propertyTypeMap.clear();
                this.propertyTypeMap = null;
            }
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
        for (Iterator<String> it = this.uncommittedRemovedIndexes.iterator(); it.hasNext(); ) {
            String prop = it.next();
            this.indexes.remove(prop);
            it.remove();
        }
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.properties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            entry.getValue().afterCommit();
        }
        for (Iterator<Map.Entry<String, Partition>> it = this.uncommittedPartitions.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Partition> entry = it.next();
            this.partitions.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }
        for (Iterator<String> it = this.uncommittedRemovedPartitions.iterator(); it.hasNext(); ) {
            String prop = it.next();
            this.partitions.remove(prop);
            it.remove();
        }
        for (Iterator<Map.Entry<String, Partition>> it = this.partitions.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Partition> entry = it.next();
            entry.getValue().afterCommit();
        }
        this.committed = true;
    }

    void afterRollback() {
        Preconditions.checkState(this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread(), "Abstract.afterRollback must hold the write lock");
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
            if (this.propertyTypeMap != null) {
                this.propertyTypeMap.clear();
                this.propertyTypeMap = null;
            }
        }
        this.uncommittedRemovedProperties.clear();
        this.uncommittedGlobalUniqueIndexProperties.clear();
        for (Iterator<Map.Entry<String, Index>> it = this.uncommittedIndexes.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Index> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        this.uncommittedRemovedIndexes.clear();
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
        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
            ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (PropertyColumn property : this.uncommittedProperties.values()) {
                propertyArrayNode.add(property.toNotifyJson());
            }
            ArrayNode removedPropertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String property : this.uncommittedRemovedProperties) {
                removedPropertyArrayNode.add(property);
            }

            ArrayNode partitionArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (Partition partition : this.uncommittedPartitions.values()) {
                partitionArrayNode.add(partition.toNotifyJson());
            }
            ArrayNode removedPartitionArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String partition : this.uncommittedRemovedPartitions) {
                removedPartitionArrayNode.add(partition);
            }

            ArrayNode indexArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (Index index : this.uncommittedIndexes.values()) {
                //noinspection OptionalGetWithoutIsPresent
                Optional<JsonNode> indexJsonOptional = index.toNotifyJson();
                Preconditions.checkState(indexJsonOptional.isPresent());
                //noinspection OptionalGetWithoutIsPresent
                indexArrayNode.add(indexJsonOptional.get());
            }
            ArrayNode removedIndexArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String property : this.uncommittedRemovedIndexes) {
                removedIndexArrayNode.add(property);
            }
            result.set("uncommittedProperties", propertyArrayNode);
            result.set("uncommittedRemovedProperties", removedPropertyArrayNode);
            result.set("uncommittedPartitions", partitionArrayNode);
            result.set("uncommittedRemovedPartitions", removedPartitionArrayNode);
            result.set("uncommittedIndexes", indexArrayNode);
            result.set("uncommittedRemovedIndexes", removedIndexArrayNode);
            if (propertyArrayNode.size() == 0 && removedPropertyArrayNode.size() == 0 &&
                    partitionArrayNode.size() == 0 && removedPartitionArrayNode.size() == 0 &&
                    indexArrayNode.size() == 0 && removedIndexArrayNode.size() == 0) {
                return Optional.empty();
            }
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    /**
     * @param vertexLabelJson
     * @param fire            should we fire topology events
     */
    void fromPropertyNotifyJson(JsonNode vertexLabelJson, boolean fire) {
        ArrayNode propertiesNode = (ArrayNode) vertexLabelJson.get("uncommittedProperties");
        if (propertiesNode != null) {
            for (JsonNode propertyNode : propertiesNode) {
                PropertyColumn propertyColumn = PropertyColumn.fromNotifyJson(this, propertyNode);
                PropertyColumn old = this.properties.put(propertyColumn.getName(), propertyColumn);
                if (fire && old == null) {
                    this.getSchema().getTopology().fire(propertyColumn, "", TopologyChangeAction.CREATE);
                }
            }
        }
        ArrayNode removedPropertyArrayNode = (ArrayNode) vertexLabelJson.get("uncommittedRemovedProperties");
        if (removedPropertyArrayNode != null) {
            for (JsonNode propertyNode : removedPropertyArrayNode) {
                String pName = propertyNode.asText();
                PropertyColumn old = this.properties.remove(pName);
                if (fire && old != null) {
                    this.getSchema().getTopology().fire(old, "", TopologyChangeAction.DELETE);
                }
            }
        }
        ArrayNode partitionsNode = (ArrayNode) vertexLabelJson.get("uncommittedPartitions");
        if (partitionsNode != null) {
            for (JsonNode partitionNode : partitionsNode) {
                Partition partitionColumn = Partition.fromNotifyJson(this, partitionNode);
                Partition old = this.partitions.put(partitionColumn.getName(), partitionColumn);
                if (fire && old == null) {
                    this.getSchema().getTopology().fire(partitionColumn, "", TopologyChangeAction.CREATE);
                }
            }
        }
        ArrayNode removedPartitionsArrayNode = (ArrayNode) vertexLabelJson.get("uncommittedRemovedPartitions");
        if (removedPartitionsArrayNode != null) {
            for (JsonNode partitionNode : removedPartitionsArrayNode) {
                String pName = partitionNode.asText();
                Partition old = this.partitions.remove(pName);
                if (fire && old != null) {
                    this.getSchema().getTopology().fire(old, "", TopologyChangeAction.DELETE);
                }
            }
        }
        ArrayNode indexNodes = (ArrayNode) vertexLabelJson.get("uncommittedIndexes");
        if (indexNodes != null) {
            for (JsonNode indexNode : indexNodes) {
                Index index = Index.fromNotifyJson(this, indexNode);
                this.indexes.put(index.getName(), index);
                this.getSchema().getTopology().fire(index, "", TopologyChangeAction.CREATE);
            }
        }
        ArrayNode removedIndexArrayNode = (ArrayNode) vertexLabelJson.get("uncommittedRemovedIndexes");
        if (removedIndexArrayNode != null) {
            for (JsonNode indexNode : removedIndexArrayNode) {
                String iName = indexNode.asText();
                Index old = this.indexes.remove(iName);
                if (fire && old != null) {
                    this.getSchema().getTopology().fire(old, "", TopologyChangeAction.DELETE);
                }
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

    /**
     * remove a given property
     *
     * @param propertyColumn the property column
     * @param preserveData   should we preserve the SQL data?
     */
    abstract void removeProperty(PropertyColumn propertyColumn, boolean preserveData);

    /**
     * remove a column from the table
     *
     * @param schema the schema
     * @param table  the table name
     * @param column the column name
     */
    void removeColumn(String schema, String table, String column) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
        sql.append(" DROP COLUMN IF EXISTS ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
        if (sqlgGraph.getSqlDialect().supportsCascade()) {
            sql.append(" CASCADE");
        }
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

    /**
     * remove a given index that was on this label
     *
     * @param idx          the index
     * @param preserveData should we keep the SQL data
     */
    void removeIndex(Index idx, boolean preserveData) {
        this.getSchema().getTopology().lock();
        if (!uncommittedRemovedIndexes.contains(idx.getName())) {
            uncommittedRemovedIndexes.add(idx.getName());
            TopologyManager.removeIndex(this.sqlgGraph, idx);
            if (!preserveData) {
                idx.delete(sqlgGraph);
            }
            this.getSchema().getTopology().fire(idx, "", TopologyChangeAction.DELETE);
        }
    }

    /**
     * check if we're valid (have a valid schema, for example)
     * this is used for edge labels that require at least one out vertex but sometimes don't (in the middle of deletion operations)
     *
     * @return
     */
    boolean isValid() {
        return true;
    }

    public void removePartition(Partition partition, boolean preserveData) {
        this.getSchema().getTopology().lock();
        String fn = partition.getName();
        if (!uncommittedRemovedPartitions.contains(fn)) {
            uncommittedRemovedPartitions.add(fn);
            TopologyManager.removePartition(this.sqlgGraph, partition);
            if (!preserveData) {
                partition.delete();
            } else {
                partition.detach();
            }
            this.getSchema().getTopology().fire(partition, "", TopologyChangeAction.DELETE);
        }

    }
}
