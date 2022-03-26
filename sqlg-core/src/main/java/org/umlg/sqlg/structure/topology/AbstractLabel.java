package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.ThreadLocalListOrderedSet;
import org.umlg.sqlg.util.ThreadLocalMap;
import org.umlg.sqlg.util.ThreadLocalSet;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2016/09/14
 * Time: 11:19 AM
 */
@SuppressWarnings("Duplicates")
public abstract class AbstractLabel implements TopologyInf {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLabel.class);
    boolean committed = true;
    final String label;
    final SqlgGraph sqlgGraph;
    final Map<String, PropertyColumn> properties = new ConcurrentHashMap<>();
    final Map<String, PropertyColumn> uncommittedProperties = new ThreadLocalMap<>();
    final Set<String> uncommittedRemovedProperties = new ThreadLocalSet<>();

    private final TreeMap<Integer, String> identifierMap = new TreeMap<>();
    final ListOrderedSet<String> identifiers = new ListOrderedSet<>();
    private final Set<String> uncommittedIdentifiers = new ThreadLocalListOrderedSet<>();
    //Pair of <old,new> identifiers
    final Set<Pair<String, String>> renamedIdentifiers = new ThreadLocalListOrderedSet<>();

    //Citus sharding
    private PropertyColumn distributionPropertyColumn;
    private PropertyColumn uncommittedDistributionPropertyColumn;
    private AbstractLabel distributionColocateAbstractLabel;
    private AbstractLabel uncommittedDistributionColocateAbstractLabel;
    private int shardCount = -1;
    private int uncommittedShardCount = -1;

    private final Map<String, Index> indexes = new ConcurrentHashMap<>();
    private final Map<String, Index> uncommittedIndexes = new ThreadLocalMap<>();
    private final Set<String> uncommittedRemovedIndexes = new ThreadLocalSet<>();

    /**
     * Indicates if the table is partitioned or not.
     * {@link PartitionType#NONE} indicates a normal non partitioned table.
     * {@link PartitionType#RANGE} and {@link PartitionType#LIST} indicate the type of partitioning.
     */
    PartitionType partitionType = PartitionType.NONE;
    String partitionExpression;
    private final Map<String, Partition> partitions = new ConcurrentHashMap<>();
    private final Map<String, Partition> uncommittedPartitions = new ThreadLocalMap<>();
    private final Set<String> uncommittedRemovedPartitions = new ThreadLocalSet<>();

    protected final boolean isForeignAbstractLabel;

    /**
     * Only called for a partitioned vertex/edge label being added.
     *
     * @param label The vertex or edge's label.
     */
    AbstractLabel(SqlgGraph sqlgGraph, String label, PartitionType partitionType, String partitionExpression) {
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        this.partitionType = partitionType;
        this.partitionExpression = partitionExpression;
        this.isForeignAbstractLabel = false;
    }

    /**
     * Only called for a new vertex/edge label being added.
     *
     * @param label       The vertex or edge's label.
     * @param properties  The vertex's properties.
     * @param identifiers The vertex or edge's identifiers
     */
    AbstractLabel(SqlgGraph sqlgGraph, String label, Map<String, PropertyType> properties, ListOrderedSet<String> identifiers) {
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        for (Map.Entry<String, PropertyType> propertyEntry : properties.entrySet()) {
            PropertyColumn property = new PropertyColumn(this, propertyEntry.getKey(), propertyEntry.getValue());
            property.setCommitted(false);
            this.uncommittedProperties.put(propertyEntry.getKey(), property);
        }
        this.uncommittedIdentifiers.addAll(identifiers);
        this.isForeignAbstractLabel = false;
    }

    /**
     * Only called for a new partitioned vertex/edge label being added.
     *
     * @param label               The vertex or edge's label.
     * @param properties          The element's properties.
     * @param identifiers         The element's identifiers.
     * @param partitionType       The partition type. i.e. RANGE or LIST.
     * @param partitionExpression The sql fragment to express the partition column or expression.
     */
    AbstractLabel(SqlgGraph sqlgGraph, String label, Map<String, PropertyType> properties, ListOrderedSet<String> identifiers, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkArgument(partitionType == PartitionType.RANGE || partitionType == PartitionType.LIST || partitionType == PartitionType.HASH, "Only RANGE and LIST partitions are supported. Found %s", partitionType.name());
        Preconditions.checkArgument(!partitionExpression.isEmpty(), "partitionExpression may not be an empty string.");
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        for (Map.Entry<String, PropertyType> propertyEntry : properties.entrySet()) {
            PropertyColumn property = new PropertyColumn(this, propertyEntry.getKey(), propertyEntry.getValue());
            property.setCommitted(false);
            this.uncommittedProperties.put(propertyEntry.getKey(), property);
        }
        this.uncommittedIdentifiers.addAll(identifiers);
        this.partitionType = partitionType;
        this.partitionExpression = partitionExpression;
        this.isForeignAbstractLabel = false;
    }

    AbstractLabel(SqlgGraph sqlgGraph, String label) {
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        this.isForeignAbstractLabel = false;
    }

    AbstractLabel(SqlgGraph sqlgGraph, String label, boolean isForeignAbstractLabel) {
        Preconditions.checkState(isForeignAbstractLabel);
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        this.isForeignAbstractLabel = isForeignAbstractLabel;
    }

    public boolean isForeign() {
        return isForeignAbstractLabel;
    }

    /**
     * Ensures that a RANGE partition exists.
     *
     * @param name The partition's name
     * @param from The RANGE partition's start clause.
     * @param to   THe RANGE partition's end clause.
     * @return The {@link Partition}
     */
    public Partition ensureRangePartitionExists(String name, String from, String to) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(name, "RANGE Partition's \"name\" must not be null");
        Objects.requireNonNull(from, "RANGE Partition's \"from\" must not be null");
        Objects.requireNonNull(to, "RANGE Partition's \"to\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.RANGE, "ensureRangePartitionExists(String name, String from, String to) can only be called for a RANGE partitioned VertexLabel. Found %s", this.partitionType.name());
        this.sqlgGraph.getSqlDialect().validateTableName(name);
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            getTopology().startSchemaChange();
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createRangePartition(name, from, to));
        } else {
            return partitionOptional.get();
        }
    }

    /**
     * Ensures that a RANGE partition exists.
     *
     * @param name                The partition's name
     * @param from                The RANGE partition's start clause.
     * @param to                  The RANGE partition's end clause.
     * @param partitionType       The partition's {@link PartitionType} if is is going to be sub-partitioned.
     * @param partitionExpression The partition's partitionExpression if is is going to be sub-partitioned.
     * @return The {@link Partition}
     */
    public Partition ensureRangePartitionWithSubPartitionExists(String name, String from, String to, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(name, "RANGE Partition's \"name\" must not be null");
        Objects.requireNonNull(from, "RANGE Partition's \"from\" must not be null");
        Objects.requireNonNull(to, "RANGE Partition's \"to\" must not be null");
        Objects.requireNonNull(partitionType, "Sub-partition's \"partitionType\" must not be null");
        Objects.requireNonNull(partitionExpression, "Sub-partition's \"partitionExpression\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.RANGE, "ensureRangePartitionExists(String name, String from, String to, PartitionType partitionType, String partitionExpression) can only be called for a RANGE partitioned VertexLabel. Found %s", this.partitionType.name());
        this.sqlgGraph.getSqlDialect().validateTableName(name);
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            getTopology().startSchemaChange();
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createRangePartitionWithSubPartition(name, from, to, partitionType, partitionExpression));
        } else {
            return partitionOptional.get();
        }
    }

    /**
     * Ensures that a HASH partition exists.
     *
     * @param name      The partition's name.
     * @param modulus   The HASH partition's 'modulus'.
     * @param remainder The HASH partition's 'remainder'.
     * @return The {@link Partition}
     */
    @SuppressWarnings("UnusedReturnValue")
    public Partition ensureHashPartitionExists(String name, int modulus, int remainder) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(name, "HASH Partition's \"name\" must not be null");
        Preconditions.checkState(modulus > 0, "HASH Partition's \"modulus\" must be > 0");
        Preconditions.checkState(remainder >= 0, "HASH Partition's \"remainder\" must be >= 0");
        Preconditions.checkState(this.partitionType == PartitionType.HASH, "ensureHashPartitionExists(String name, String ... in) can only be called for a LIST partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            getTopology().startSchemaChange();
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createHashPartition(name, modulus, remainder));
        } else {
            return partitionOptional.get();
        }
    }

    /**
     * Ensures that a LIST partition exists.
     *
     * @param name The partition's name.
     * @param in   The LIST partition's 'in' clause.
     * @return The {@link Partition}
     */
    public Partition ensureListPartitionExists(String name, String in) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(name, "LIST Partition's \"name\" must not be null");
        Objects.requireNonNull(in, "LIST Partition's \"in\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.LIST, "ensureListPartitionExists(String name, String ... in) can only be called for a LIST partitioned VertexLabel. Found %s", this.partitionType.name());
        this.sqlgGraph.getSqlDialect().validateTableName(name);
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            getTopology().startSchemaChange();
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createListPartition(name, in));
        } else {
            return partitionOptional.get();
        }
    }

    /**
     * Ensures that a LIST partition exists.
     *
     * @param name                The partition's name.
     * @param in                  The LIST partition's 'in' clause.
     * @param partitionType       The partition's {@link PartitionType} if it is going to be sub-partitioned.
     * @param partitionExpression The partition's partitionExpression if it is going to be sub-partitioned.
     * @return The {@link Partition}
     */
    public Partition ensureListPartitionWithSubPartitionExists(String name, String in, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(name, "LIST Partition's \"name\" must not be null");
        Objects.requireNonNull(in, "LIST Partition's \"in\" must not be null");
        Objects.requireNonNull(partitionType, "Sub-partition's \"partitionType\" must not be null");
        Objects.requireNonNull(partitionExpression, "Sub-partition's \"partitionExpression\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.LIST, "ensureRangePartitionExists(String name, String ... in) can only be called for a LIST partitioned VertexLabel. Found %s", this.partitionType.name());
        this.sqlgGraph.getSqlDialect().validateTableName(name);
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            getTopology().startSchemaChange();
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createListPartitionWithSubPartition(name, in, partitionType, partitionExpression));
        } else {
            return partitionOptional.get();
        }
    }

    /**
     * Ensures that a HASH partition exists.
     *
     * @param name                The partition's name.
     * @param modulus             The HASH partition's 'modulus' clause.
     * @param remainder           The HASH partition's 'remainder' clause.
     * @param partitionType       The partition's {@link PartitionType} if it is going to be sub-partitioned.
     * @param partitionExpression The partition's partitionExpression if it is going to be sub-partitioned.
     * @return The {@link Partition}
     */
    public Partition ensureHashPartitionWithSubPartitionExists(String name, Integer modulus, Integer remainder, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(name, "HASH Partition's \"name\" must not be null");
        Preconditions.checkState(modulus > 0, "HASH Partition's \"modulus\" must > 0");
        Preconditions.checkState(remainder >= 0, "HASH Partition's \"remainder\" must >= 0");
        Objects.requireNonNull(partitionType, "Sub-partition's \"partitionType\" must not be null");
        Objects.requireNonNull(partitionExpression, "Sub-partition's \"partitionExpression\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.HASH, "ensureHashPartitionWithSubPartitionExists can only be called for a HASH partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            getTopology().startSchemaChange();
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createHashPartitionWithSubPartition(name, modulus, remainder, partitionType, partitionExpression));
        } else {
            return partitionOptional.get();
        }
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    public boolean isRangePartition() {
        return partitionType.isRange();
    }

    @SuppressWarnings("unused")
    public boolean isListPartition() {
        return partitionType.isList();
    }

    public boolean isHashPartition() {
        return partitionType.isHash();
    }

    public boolean isPartition() {
        return !partitionType.isNone();
    }

    private Partition createRangePartition(String name, String from, String to) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createRangePartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createRangePartition(this.sqlgGraph, this, name, from, to);
        this.uncommittedPartitions.put(name, partition);
        getTopology().fire(partition, null, TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createRangePartitionWithSubPartition(String name, String from, String to, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createRangePartitionWithSubPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createRangePartitionWithSubPartition(this.sqlgGraph, this, name, from, to, partitionType, partitionExpression);
        this.uncommittedPartitions.put(name, partition);
        getTopology().fire(partition, null, TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createHashPartition(String name, int modulus, int remainder) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createListPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createHashPartition(this.sqlgGraph, this, name, modulus, remainder);
        this.uncommittedPartitions.put(name, partition);
        getTopology().fire(partition, null, TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createListPartition(String name, String in) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createListPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createListPartition(this.sqlgGraph, this, name, in);
        this.uncommittedPartitions.put(name, partition);
        getTopology().fire(partition, null, TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createListPartitionWithSubPartition(String name, String in, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createListPartitionWithSubPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createListPartitionWithSubPartition(this.sqlgGraph, this, name, in, partitionType, partitionExpression);
        this.uncommittedPartitions.put(name, partition);
        getTopology().fire(partition, null, TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createHashPartitionWithSubPartition(String name, Integer modulus, Integer remainder, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createHashPartitionWithSubPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createHashPartitionWithSubPartition(this.sqlgGraph, this, name, modulus, remainder, partitionType, partitionExpression);
        this.uncommittedPartitions.put(name, partition);
        getTopology().fire(partition, null, TopologyChangeAction.CREATE);
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

            getTopology().startSchemaChange();
            for (Index idx : this.getIndexes().values()) {
                if (idx.getProperties().equals(properties)) {
                    return idx;
                }
            }
            indexName = Index.generateName(this.sqlgGraph.getSqlDialect());

            return this.createIndex(indexName, indexType, properties);

        } else {

            Optional<Index> indexOptional = this.getIndex(indexName);
            if (indexOptional.isEmpty()) {
                this.getTopology().startSchemaChange();
                indexOptional = this.getIndex(indexName);
                if (indexOptional.isEmpty()) {
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
        this.getTopology().fire(index, null, TopologyChangeAction.CREATE);
        return index;
    }

    void addIndex(Index i) {
        this.indexes.put(i.getName(), i);
    }

    public abstract Schema getSchema();

    public abstract Topology getTopology();

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

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public void setPartitionExpression(String partitionExpression) {
        this.partitionExpression = partitionExpression;
    }

    /**
     * Does a recursive search for a partition with the given name.
     *
     * @param name The partition to returns name.
     * @return The partition as an Optional.
     */
    public Optional<Partition> getPartition(String name) {
        boolean schemaChanged = getTopology().isSchemaChanged();
        if (schemaChanged && this.uncommittedRemovedPartitions.contains(name)) {
            return Optional.empty();
        }
        Partition result = null;
        if (schemaChanged) {
            result = this.uncommittedPartitions.get(name);
        }
        if (result == null) {
            result = this.partitions.get(name);
        }
        if (result == null) {
            for (Partition uncommittedPartition : this.uncommittedPartitions.values()) {
                Optional<Partition> p = uncommittedPartition.getPartition(name);
                if (p.isPresent()) {
                    return p;
                }
            }
            for (Partition partition : this.partitions.values()) {
                Optional<Partition> p = partition.getPartition(name);
                if (p.isPresent()) {
                    return p;
                }
            }
        }
        return Optional.ofNullable(result);
    }

    public Map<String, Partition> getPartitions() {
        Map<String, Partition> result = new HashMap<>(this.partitions);
        if (getTopology().isSchemaChanged()) {
            result.putAll(this.uncommittedPartitions);
            for (String s : this.uncommittedRemovedPartitions) {
                result.remove(s);
            }
        }
        return result;
    }

    public Map<String, PropertyColumn> getProperties() {
        Map<String, PropertyColumn> result = new HashMap<>(this.properties);
        if (getTopology().isSchemaChanged()) {
            result.putAll(this.uncommittedProperties);
            for (String s : this.uncommittedRemovedProperties) {
                result.remove(s);
            }
        }
        return result;
    }

    public ListOrderedSet<String> getIdentifiers() {
        ListOrderedSet<String> result = ListOrderedSet.listOrderedSet(new ArrayList<>(this.identifiers.asList()));
        if (getTopology().isSchemaChanged()) {
            result.addAll(this.uncommittedIdentifiers);
            for (Pair<String, String> oldNew : this.renamedIdentifiers) {
                result.remove(oldNew.getLeft());
                result.add(oldNew.getRight());
            }
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
        Map<String, Index> result = new HashMap<>(this.indexes);
        if (getTopology().isSchemaChanged()) {
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
        if (getTopology().isSchemaChanged()) {
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
        if (getTopology().isSchemaChanged()) {
            return this.uncommittedProperties;
        } else {
            return Collections.emptyMap();
        }
    }

    Set<String> getUncommittedRemovedProperties() {
        if (getTopology().isSchemaChanged()) {
            return this.uncommittedRemovedProperties;
        } else {
            return Collections.emptySet();
        }
    }

    static void buildColumns(SqlgGraph sqlgGraph, ListOrderedSet<String> identifiers, Map<String, PropertyType> columns, StringBuilder sql) {
        int i = 1;
        //This is to make the columns sorted
        List<String> keys = new ArrayList<>(columns.keySet());
        //if there are identifiers, do them first.
        for (String identifier : identifiers) {
            PropertyType propertyType = columns.get(identifier);
            Preconditions.checkState(propertyType != null, "PropertyType is null for %s", identifier);
            int count = 1;
            String[] propertyTypeToSqlDefinition = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
            for (String sqlDefinition : propertyTypeToSqlDefinition) {
                sql.append("\n\t");
                if (count > 1) {
                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier + propertyType.getPostFixes()[count - 2])).append(" ").append(sqlDefinition);
                } else {
                    //The first column existVertexLabel no postfix
                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier)).append(" ").append(sqlDefinition);
                }
                if (count++ < propertyTypeToSqlDefinition.length) {
                    sql.append(", ");
                }
            }
            if (i++ < identifiers.size()) {
                sql.append(", ");
            }
        }
        if (!identifiers.isEmpty() && columns.size() > identifiers.size()) {
            sql.append(", ");
        }
        keys.removeAll(identifiers);
        for (String column : keys) {
            PropertyType propertyType = columns.get(column);
            int count = 1;
            String[] propertyTypeToSqlDefinition = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
            for (String sqlDefinition : propertyTypeToSqlDefinition) {
                sql.append("\n\t");
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

    void addColumn(String schema, String table, ImmutablePair<String, PropertyType> keyValue) {
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql.toString());
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
        Preconditions.checkState(getTopology().isSchemaChanged());
        PropertyColumn property = new PropertyColumn(this, propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME), PropertyType.valueOf(propertyVertex.value(SQLG_SCHEMA_PROPERTY_TYPE)));
        this.properties.put(propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME), property);
    }

    void addIdentifier(String propertyName, int index) {
        Preconditions.checkState(getTopology().isSchemaChanged());
        this.identifierMap.put(index, propertyName);
        this.identifiers.clear();
        for (Integer mapIndex : this.identifierMap.keySet()) {
            this.identifiers.add(this.identifierMap.get(mapIndex));
        }
    }

    void clearIdentifiersMap() {
        this.identifierMap.clear();
    }

    void addDistributionColocate(Vertex colocate) {
        Preconditions.checkState(getTopology().isSchemaChanged());
        this.distributionColocateAbstractLabel = getSchema().getVertexLabel(colocate.value(SQLG_SCHEMA_VERTEX_LABEL_NAME)).orElseThrow(() -> new IllegalStateException("Distribution Co-locate vertex label %s not found", colocate.value(SQLG_SCHEMA_VERTEX_LABEL_NAME)));
    }

    void addDistributionProperty(Vertex distributionProperty) {
        Preconditions.checkState(this.getTopology().isSchemaChanged());
        this.distributionPropertyColumn = new PropertyColumn(this, distributionProperty.value(SQLG_SCHEMA_PROPERTY_NAME), PropertyType.valueOf(distributionProperty.value(SQLG_SCHEMA_PROPERTY_TYPE)));
    }

    Partition addPartition(Vertex partitionVertex) {
        Preconditions.checkState(this.getTopology().isSchemaChanged());
        VertexProperty<String> from = partitionVertex.property(SQLG_SCHEMA_PARTITION_FROM);
        VertexProperty<String> to = partitionVertex.property(SQLG_SCHEMA_PARTITION_TO);
        VertexProperty<String> in = partitionVertex.property(SQLG_SCHEMA_PARTITION_IN);
        VertexProperty<Integer> modulus = partitionVertex.property(SQLG_SCHEMA_PARTITION_MODULUS);
        VertexProperty<Integer> remainder = partitionVertex.property(SQLG_SCHEMA_PARTITION_REMAINDER);
        VertexProperty<String> partitionType = partitionVertex.property(SQLG_SCHEMA_PARTITION_PARTITION_TYPE);
        VertexProperty<String> partitionExpression = partitionVertex.property(SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION);
        Partition partition;
        if (from.isPresent()) {
            Preconditions.checkState(to.isPresent());
            Preconditions.checkState(!in.isPresent());
            Preconditions.checkState(!modulus.isPresent());
            Preconditions.checkState(!remainder.isPresent());
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                    from.value(),
                    to.value(),
                    PartitionType.from(partitionType.value()),
                    partitionExpression.isPresent() ? partitionExpression.value() : null);
        } else if (in.isPresent()) {
            Preconditions.checkState(in.isPresent());
            Preconditions.checkState(!to.isPresent());
            Preconditions.checkState(!modulus.isPresent());
            Preconditions.checkState(!remainder.isPresent());
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                    in.value(),
                    PartitionType.from(partitionType.value()),
                    partitionExpression.isPresent() ? partitionExpression.value() : null);
        } else {
            Preconditions.checkState(modulus.isPresent());
            Preconditions.checkState(remainder.isPresent());
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                    modulus.value(),
                    remainder.value(),
                    PartitionType.from(partitionType.value()),
                    partitionExpression.isPresent() ? partitionExpression.value() : null);
        }
        this.partitions.put(partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME), partition);
        return partition;
    }

    void afterCommit() {
        Preconditions.checkState(this.getTopology().isSchemaChanged(), "AbstractLabel.afterCommit must have schemaChanged ThreadLocal var as true");
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            this.properties.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }
        for (Iterator<String> it = this.uncommittedRemovedProperties.iterator(); it.hasNext(); ) {
            String prop = it.next();
            this.properties.remove(prop);
            it.remove();
        }
        this.identifiers.addAll(this.uncommittedIdentifiers);
        int index = -1;
        for (Iterator<Pair<String, String>> it = this.renamedIdentifiers.iterator(); it.hasNext(); ) {
            index++;
            Pair<String, String> oldName = it.next();
            this.identifiers.remove(oldName.getLeft());
            this.identifiers.add(index, oldName.getRight());
            it.remove();
        }
        this.uncommittedIdentifiers.clear();
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
        for (Map.Entry<String, PropertyColumn> entry : this.properties.entrySet()) {
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
        for (Map.Entry<String, Partition> entry : this.partitions.entrySet()) {
            entry.getValue().afterCommit();
        }
        if (this.uncommittedDistributionPropertyColumn != null) {
            this.distributionPropertyColumn = this.uncommittedDistributionPropertyColumn;
        }
        if (this.uncommittedDistributionColocateAbstractLabel != null) {
            this.distributionColocateAbstractLabel = this.uncommittedDistributionColocateAbstractLabel;
        }
        if (this.uncommittedShardCount != -1) {
            this.shardCount = this.uncommittedShardCount;
        }
        this.committed = true;
    }

    void afterRollback() {
        Preconditions.checkState(this.getTopology().isSchemaChanged(), "AbstractLabel.afterRollback must have schemaChanged ThreadLocal var as true");
        for (Iterator<Map.Entry<String, PropertyColumn>> it = this.uncommittedProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyColumn> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        this.uncommittedRemovedProperties.clear();
        this.uncommittedIdentifiers.clear();
        this.renamedIdentifiers.clear();
        for (Iterator<Map.Entry<String, Index>> it = this.uncommittedIndexes.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Index> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        this.uncommittedRemovedIndexes.clear();
        for (Map.Entry<String, PropertyColumn> entry : this.properties.entrySet()) {
            entry.getValue().afterRollback();
        }
        this.uncommittedRemovedPartitions.clear();
        for (Map.Entry<String, Partition> entry : this.partitions.entrySet()) {
            entry.getValue().afterRollback();
        }
        this.uncommittedDistributionPropertyColumn = null;
        this.uncommittedDistributionColocateAbstractLabel = null;
    }

    JsonNode toJson() {
        ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        List<PropertyColumn> propertyColumns = new ArrayList<>(this.properties.values());
        propertyColumns.sort(Comparator.comparing(PropertyColumn::getName));
        for (PropertyColumn property : propertyColumns) {
            propertyArrayNode.add(property.toNotifyJson());
        }
        return propertyArrayNode;
    }

    Optional<JsonNode> toNotifyJson() {
        if (getTopology().isSchemaChanged()) {
            ObjectNode result = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
            ArrayNode propertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (PropertyColumn property : this.uncommittedProperties.values()) {
                propertyArrayNode.add(property.toNotifyJson());
            }
            ArrayNode removedPropertyArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String property : this.uncommittedRemovedProperties) {
                removedPropertyArrayNode.add(property);
            }
            ArrayNode identifierArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String identifier : this.uncommittedIdentifiers) {
                identifierArrayNode.add(identifier);
            }
            ArrayNode renamedIdentifierArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (Pair<String, String> oldNew : this.renamedIdentifiers) {
                ObjectNode renamedObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                renamedObjectNode.put("old", oldNew.getLeft());
                renamedObjectNode.put("new", oldNew.getRight());
                renamedIdentifierArrayNode.add(renamedObjectNode);
            }
            ArrayNode uncommittedPartitionArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (Partition partition : this.uncommittedPartitions.values()) {
                Optional<ObjectNode> json = partition.toUncommittedPartitionNotifyJson();
                json.ifPresent(uncommittedPartitionArrayNode::add);
            }
            ArrayNode removedPartitionArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String partition : this.uncommittedRemovedPartitions) {
                removedPartitionArrayNode.add(partition);
            }
            ArrayNode committedPartitionArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            //Need to check for nested uncommitted partitions
            for (Partition partition : this.partitions.values()) {
                Optional<ObjectNode> json = partition.toCommittedPartitionNotifyJson();
                json.ifPresent(committedPartitionArrayNode::add);
            }
            ObjectNode uncommittedDistributionPropertyColumnObjectNode = null;
            if (this.uncommittedDistributionPropertyColumn != null) {
                uncommittedDistributionPropertyColumnObjectNode = this.uncommittedDistributionPropertyColumn.toNotifyJson();
            }
            ObjectNode uncommittedDistributionColocateAbstractLabelObjectNode = null;
            if (this.uncommittedDistributionColocateAbstractLabel != null) {
                String colocateLabel = this.uncommittedDistributionColocateAbstractLabel.label;
                uncommittedDistributionColocateAbstractLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                uncommittedDistributionColocateAbstractLabelObjectNode.put("colocateLabel", colocateLabel);
            }
            ObjectNode uncommittedShardCountObjectNode = null;
            if (this.uncommittedShardCount != -1) {
                uncommittedShardCountObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                uncommittedShardCountObjectNode.put("uncommittedShardCount", this.uncommittedShardCount);
            }

            ArrayNode indexArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (Index index : this.uncommittedIndexes.values()) {
                Optional<JsonNode> indexJsonOptional = index.toNotifyJson();
                Preconditions.checkState(indexJsonOptional.isPresent());
                indexArrayNode.add(indexJsonOptional.get());
            }
            ArrayNode removedIndexArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String property : this.uncommittedRemovedIndexes) {
                removedIndexArrayNode.add(property);
            }
            result.set("uncommittedProperties", propertyArrayNode);
            result.set("uncommittedRemovedProperties", removedPropertyArrayNode);
            result.set("uncommittedIdentifiers", identifierArrayNode);
            result.set("renamedIdentifiers", renamedIdentifierArrayNode);
            result.set("uncommittedPartitions", uncommittedPartitionArrayNode);
            result.set("partitions", committedPartitionArrayNode);
            result.set("uncommittedRemovedPartitions", removedPartitionArrayNode);
            if (uncommittedDistributionPropertyColumnObjectNode != null) {
                result.set("uncommittedDistributionPropertyColumn", uncommittedDistributionPropertyColumnObjectNode);
            }
            if (uncommittedDistributionColocateAbstractLabelObjectNode != null) {
                result.set("uncommittedDistributionColocateAbstractLabel", uncommittedDistributionColocateAbstractLabelObjectNode);
            }
            if (uncommittedShardCountObjectNode != null) {
                result.set("uncommittedShardCount", uncommittedShardCountObjectNode);
            }
            result.set("uncommittedIndexes", indexArrayNode);
            result.set("uncommittedRemovedIndexes", removedIndexArrayNode);
            if (propertyArrayNode.size() == 0 && removedPropertyArrayNode.size() == 0 &&
                    identifierArrayNode.size() == 0 &&
                    uncommittedPartitionArrayNode.size() == 0 && removedPartitionArrayNode.size() == 0 && committedPartitionArrayNode.size() == 0 &&
                    indexArrayNode.size() == 0 && removedIndexArrayNode.size() == 0 &&
                    uncommittedDistributionPropertyColumnObjectNode == null && uncommittedShardCountObjectNode == null &&
                    uncommittedDistributionColocateAbstractLabelObjectNode == null) {
                return Optional.empty();
            }
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }

    /**
     * @param vertexLabelJson The json for the vertexLabel
     * @param fire            should we fire topology events
     */
    void fromPropertyNotifyJson(JsonNode vertexLabelJson, boolean fire) {
        ArrayNode propertiesNode = (ArrayNode) vertexLabelJson.get("uncommittedProperties");
        if (propertiesNode != null) {
            for (JsonNode propertyNode : propertiesNode) {
                PropertyColumn propertyColumn = PropertyColumn.fromNotifyJson(this, propertyNode);
                PropertyColumn old = this.properties.put(propertyColumn.getName(), propertyColumn);
                if (fire && old == null) {
                    this.getTopology().fire(propertyColumn, null, TopologyChangeAction.CREATE);
                }
            }
        }
        ArrayNode identifiersNode = (ArrayNode) vertexLabelJson.get("uncommittedIdentifiers");
        if (identifiersNode != null) {
            for (JsonNode identifierNode : identifiersNode) {
                this.identifiers.add(identifierNode.asText());
            }
        }
        ArrayNode renamedIdentifiersNode = (ArrayNode) vertexLabelJson.get("renamedIdentifiers");
        if (renamedIdentifiersNode != null) {
            for (JsonNode identifierNode : renamedIdentifiersNode) {
                ObjectNode identifierObjectNode = (ObjectNode)identifierNode;
                String oldIdentifier = identifierObjectNode.get("old").asText();
                String newIdentifier = identifierObjectNode.get("new").asText();
                this.identifiers.remove(oldIdentifier);
                this.identifiers.add(newIdentifier);
            }
        }
        ArrayNode removedPropertyArrayNode = (ArrayNode) vertexLabelJson.get("uncommittedRemovedProperties");
        if (removedPropertyArrayNode != null) {
            for (JsonNode propertyNode : removedPropertyArrayNode) {
                String pName = propertyNode.asText();
                PropertyColumn old = this.properties.remove(pName);
                if (fire && old != null) {
                    this.getTopology().fire(old, old, TopologyChangeAction.DELETE);
                }
            }
        }
        ArrayNode uncommittedPartitionsNode = (ArrayNode) vertexLabelJson.get("uncommittedPartitions");
        if (uncommittedPartitionsNode != null) {
            for (JsonNode partitionNode : uncommittedPartitionsNode) {
                Partition partitionColumn = Partition.fromUncommittedPartitionNotifyJson(this, partitionNode);
                Partition old = this.partitions.put(partitionColumn.getName(), partitionColumn);
                if (fire && old == null) {
                    this.getTopology().fire(partitionColumn, null, TopologyChangeAction.CREATE);
                }
            }
        }
        ArrayNode partitionsNode = (ArrayNode) vertexLabelJson.get("partitions");
        if (partitionsNode != null) {
            for (JsonNode partitionNode : partitionsNode) {
                Optional<Partition> optionalPartition = getPartition(partitionNode.get("name").asText());
                if (optionalPartition.isPresent()) {
                    Partition committedPartition = optionalPartition.get();
                    committedPartition.fromNotifyJson(partitionNode, fire);
                    Partition old = this.partitions.put(committedPartition.getName(), committedPartition);
                    if (fire && old == null) {
                        this.getTopology().fire(committedPartition, null, TopologyChangeAction.CREATE);
                    }
                }
            }
        }
        ArrayNode removedPartitionsArrayNode = (ArrayNode) vertexLabelJson.get("uncommittedRemovedPartitions");
        if (removedPartitionsArrayNode != null) {
            for (JsonNode partitionNode : removedPartitionsArrayNode) {
                String pName = partitionNode.asText();
                Partition old = this.partitions.remove(pName);
                if (fire && old != null) {
                    this.getTopology().fire(old, old, TopologyChangeAction.DELETE);
                }
            }
        }

        ObjectNode distributionPropertyColumnObjectNode = (ObjectNode) vertexLabelJson.get("uncommittedDistributionPropertyColumn");
        if (distributionPropertyColumnObjectNode != null) {
            this.distributionPropertyColumn = PropertyColumn.fromNotifyJson(this, distributionPropertyColumnObjectNode);
        }
        ObjectNode distributionColocateAbstractLabelObjectNode = (ObjectNode) vertexLabelJson.get("uncommittedDistributionColocateAbstractLabel");
        if (distributionColocateAbstractLabelObjectNode != null) {
            Optional<VertexLabel> colocateVertexLabelOpt = getSchema().getVertexLabel(distributionColocateAbstractLabelObjectNode.get("colocateLabel").asText());
            Preconditions.checkState(colocateVertexLabelOpt.isPresent());
            this.distributionColocateAbstractLabel = colocateVertexLabelOpt.get();
        }
        ObjectNode shardCountObjectNode = (ObjectNode) vertexLabelJson.get("uncommittedShardCount");
        if (shardCountObjectNode != null) {
            this.shardCount = shardCountObjectNode.get("uncommittedShardCount").asInt();
        }

        ArrayNode indexNodes = (ArrayNode) vertexLabelJson.get("uncommittedIndexes");
        if (indexNodes != null) {
            for (JsonNode indexNode : indexNodes) {
                Index index = Index.fromNotifyJson(this, indexNode);
                this.indexes.put(index.getName(), index);
                this.getTopology().fire(index, null, TopologyChangeAction.CREATE);
            }
        }
        ArrayNode removedIndexArrayNode = (ArrayNode) vertexLabelJson.get("uncommittedRemovedIndexes");
        if (removedIndexArrayNode != null) {
            for (JsonNode indexNode : removedIndexArrayNode) {
                String iName = indexNode.asText();
                Index old = this.indexes.remove(iName);
                if (fire && old != null) {
                    this.getTopology().fire(old, old, TopologyChangeAction.DELETE);
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
        return this.label.equals(other.label);
    }

    protected abstract List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException;

    public abstract String getPrefix();

    /**
     * remove a given property
     *
     * @param propertyColumn the property column
     * @param preserveData   should we preserve the SQL data?
     */
    abstract void removeProperty(PropertyColumn propertyColumn, boolean preserveData);

    /**
     * rename the given property
     *
     * @param name           the new name for the property column
     * @param propertyColumn the property column
     */
    abstract void renameProperty(String name, PropertyColumn propertyColumn);

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
        sql.append(" DROP COLUMN ");
        if (sqlgGraph.getSqlDialect().supportsIfExists()) {
            sql.append("IF EXISTS ");
        }
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
        //H2 does not support CASCADE on drop column
        if (!sqlgGraph.getSqlDialect().isH2() && sqlgGraph.getSqlDialect().supportsCascade()) {
            sql.append(" CASCADE");
        }
        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * rename a column of the table
     *
     * @param schema the schema
     * @param table  the table name
     * @param column the column to rename
     * @param newName the column to rename to
     */
    void removeColumn(String schema, String table, String column, String newName) {
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * rename a column of the table
     *
     * @param schema  the schema
     * @param table   the table name
     * @param column  the column name
     * @param newName the new column name
     */
    void renameColumn(String schema, String table, String column, String newName) {
        String sql = this.sqlgGraph.getSqlDialect().renameColumn(schema, table, column, newName);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql);
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
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
        this.getTopology().startSchemaChange();
        if (!uncommittedRemovedIndexes.contains(idx.getName())) {
            uncommittedRemovedIndexes.add(idx.getName());
            TopologyManager.removeIndex(this.sqlgGraph, idx);
            if (!preserveData) {
                idx.delete(sqlgGraph);
            }
            this.getTopology().fire(idx, idx, TopologyChangeAction.DELETE);
        }
    }

    /**
     * check if we're valid (have a valid schema, for example)
     * this is used for edge labels that require at least one out vertex but sometimes don't (in the middle of deletion operations)
     *
     * @return true if it is valid
     */
    boolean isValid() {
        return true;
    }

    public void removePartition(Partition partition, boolean preserveData) {
        this.getTopology().startSchemaChange();

        for (Partition partition1 : partition.getPartitions().values()) {
            partition1.remove(preserveData);
        }

        String fn = partition.getName();
        if (!uncommittedRemovedPartitions.contains(fn)) {
            uncommittedRemovedPartitions.add(fn);
            TopologyManager.removePartition(this.sqlgGraph, partition);
            if (!preserveData) {
                partition.delete();
            } else {
                partition.detach();
            }
            this.getTopology().fire(partition, partition, TopologyChangeAction.DELETE);
        }
    }

    public boolean hasIDPrimaryKey() {
        return this.identifiers.isEmpty() && this.uncommittedIdentifiers.isEmpty();
    }

    private void distribute(int shard_count, PropertyColumn distributionPropertyColumn, AbstractLabel colocate) {
        Preconditions.checkArgument(getIdentifiers().contains(distributionPropertyColumn.getName()), "The distribution column must be part of the primary key");
        Connection conn = sqlgGraph.tx().getConnection();
        if (shard_count > -1) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SET citus.shard_count = " + shard_count + ";";
                LOGGER.debug(sql);
                stmt.execute(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        distribute(conn, distributionPropertyColumn, colocate);
    }

    private void distribute(Connection connection, PropertyColumn distributionPropertyColumn, AbstractLabel colocate) {
        StringBuilder sql = new StringBuilder();
        //If its not the public schema then first make sure the schema exist on all workers
//        if (!this.getSchema().getName().equals(this.sqlgGraph.getSqlDialect().getPublicSchema())) {
//            sql.append("SELECT run_command_on_workers($cmd$CREATE SCHEMA IF NOT EXISTS \"").append(getSchema().getName()).append("\"$cmd$);\n");
//        }
        sql.append("SELECT create_distributed_table('");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getSchema().getName()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(getPrefix() + getLabel()));
        sql.append("', '");
        sql.append(distributionPropertyColumn.getName());
        if (colocate != null) {
            sql.append("', ");
            sql.append("colocate_with => '");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(colocate.getSchema().getName()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(colocate.getPrefix() + colocate.getLabel()));
        }
        sql.append("')");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void ensureDistributed(int shardCount, PropertyColumn distributionPropertyColumn, AbstractLabel colocate) {
        Preconditions.checkState(getProperty(distributionPropertyColumn.getName()).isPresent(), "distributionPropertyColumn \"%s\" not found.", distributionPropertyColumn.getName());
        Preconditions.checkState(getProperty(distributionPropertyColumn.getName()).get().equals(distributionPropertyColumn), "distributionPropertyColumn \"%s\" must be a property of \"%s\"", distributionPropertyColumn.getName(), this.getFullName());
        Preconditions.checkArgument(getIdentifiers().contains(distributionPropertyColumn.getName()), "The distribution column must be part of the primary key");
        if (!this.isDistributed()) {
            this.getTopology().startSchemaChange();
            if (!this.isDistributed()) {
                TopologyManager.distributeAbstractLabel(this.sqlgGraph, this, shardCount, distributionPropertyColumn, colocate);
                distribute(shardCount, distributionPropertyColumn, colocate);
                this.uncommittedDistributionPropertyColumn = distributionPropertyColumn;
                this.uncommittedDistributionColocateAbstractLabel = colocate;
                this.uncommittedShardCount = shardCount;
            }
        }
    }

    public boolean isDistributed() {
        return this.distributionPropertyColumn != null || this.uncommittedDistributionPropertyColumn != null;
    }

    public PropertyColumn getDistributionPropertyColumn() {
        if (this.distributionPropertyColumn != null) {
            return this.distributionPropertyColumn;
        } else {
            return this.uncommittedDistributionPropertyColumn;
        }
    }

    public AbstractLabel getDistributionColocate() {
        if (this.distributionColocateAbstractLabel != null) {
            return this.distributionColocateAbstractLabel;
        } else {
            return this.uncommittedDistributionColocateAbstractLabel;

        }
    }

    public int getShardCount() {
        if (this.shardCount != -1) {
            return this.shardCount;
        } else {
            return this.uncommittedShardCount;
        }
    }

    void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

}
