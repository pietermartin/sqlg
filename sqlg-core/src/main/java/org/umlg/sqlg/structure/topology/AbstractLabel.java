package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.umlg.sqlg.util.Preconditions;
import org.apache.commons.collections4.list.UnmodifiableList;
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
@SuppressWarnings({"Duplicates", "SqlSourceToSinkFlow"})
public abstract class AbstractLabel implements TopologyInf {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLabel.class);
    final String label;
    final SqlgGraph sqlgGraph;
    final ThreadLocal<Boolean> isDirty = ThreadLocal.withInitial(() -> false);
    final Map<String, PropertyColumn> properties = new ConcurrentHashMap<>();
    final Map<String, PropertyColumn> uncommittedProperties = new ThreadLocalMap<>();
    final Set<String> uncommittedRemovedProperties = new ThreadLocalSet<>();

    final Map<String, PropertyColumn> uncommittedUpdatedProperties = new ThreadLocalMap<>();

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
    protected final Set<String> uncommittedRemovedPartitions = new ThreadLocalSet<>();

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
    AbstractLabel(SqlgGraph sqlgGraph, String label, Map<String, PropertyDefinition> properties, ListOrderedSet<String> identifiers) {
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        for (Map.Entry<String, PropertyDefinition> propertyEntry : properties.entrySet()) {
            PropertyColumn property = new PropertyColumn(this, propertyEntry.getKey(), propertyEntry.getValue());
            putUncommittedProperties(propertyEntry.getKey(), property);
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
    AbstractLabel(SqlgGraph sqlgGraph, String label, Map<String, PropertyDefinition> properties, ListOrderedSet<String> identifiers, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkArgument(partitionType == PartitionType.RANGE || partitionType == PartitionType.LIST || partitionType == PartitionType.HASH, "Only RANGE and LIST partitions are supported. Found %s", partitionType.name());
        Preconditions.checkArgument(!partitionExpression.isEmpty(), "partitionExpression may not be an empty string.");
        this.sqlgGraph = sqlgGraph;
        this.label = label;
        for (Map.Entry<String, PropertyDefinition> propertyEntry : properties.entrySet()) {
            PropertyColumn property = new PropertyColumn(this, propertyEntry.getKey(), propertyEntry.getValue());
            putUncommittedProperties(propertyEntry.getKey(), property);
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

    void markDirty() {
       this.isDirty.set(true);
    }

    void markClean() {
        this.isDirty.set(false);
    }

    public boolean isDirty() {
        return isDirty.get();
    }

    protected void putUncommittedProperties(String key, PropertyColumn propertyColumn) {
        markDirty();
        this.uncommittedProperties.put(key, propertyColumn);
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
            getTopology().startSchemaChange(
                    String.format("AbstractLabel '%s' ensureRangePartitionExists with '%s', '%s', '%s'", getFullName(), name, from, to)
            );
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
            getTopology().startSchemaChange(
                    String.format("AbstractLabel '%s' ensureRangePartitionWithSubPartitionExists with '%s', '%s', '%s'", getFullName(), name, from, to)
            );
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
            getTopology().startSchemaChange(
                    String.format("AbstractLabel '%s' ensureHashPartitionExists with '%s', '%d', '%d'", getFullName(), name, modulus, remainder)
            );
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
            getTopology().startSchemaChange(
                    String.format("AbstractLabel '%s' ensureListPartitionExists with '%s', '%s'", getFullName(), name, in)
            );
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
            getTopology().startSchemaChange(
                    String.format("AbstractLabel '%s' ensureListPartitionWithSubPartitionExists with '%s', '%s'", getFullName(), name, in)
            );
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
            getTopology().startSchemaChange(
                    String.format("AbstractLabel '%s' ensureHashPartitionWithSubPartitionExists with '%s' '%d' '%d'", getFullName(), name, modulus, remainder)
            );
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createHashPartitionWithSubPartition(name, modulus, remainder, partitionType, partitionExpression));
        } else {
            return partitionOptional.get();
        }
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
        markDirty();
        getSchema().markDirty();
        if (this instanceof EdgeLabel edgeLabel) {
            edgeLabel.getInVertexLabels().forEach(AbstractLabel::markDirty);
            edgeLabel.getOutVertexLabels().forEach(AbstractLabel::markDirty);
        }
        getTopology().fire(partition, null, TopologyChangeAction.CREATE, true);
        return partition;
    }

    private Partition createRangePartitionWithSubPartition(String name, String from, String to, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createRangePartitionWithSubPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createRangePartitionWithSubPartition(this.sqlgGraph, this, name, from, to, partitionType, partitionExpression);
        this.uncommittedPartitions.put(name, partition);
        markDirty();
        getSchema().markDirty();
        if (this instanceof EdgeLabel edgeLabel) {
            edgeLabel.getInVertexLabels().forEach(AbstractLabel::markDirty);
            edgeLabel.getOutVertexLabels().forEach(AbstractLabel::markDirty);
        }
        getTopology().fire(partition, null, TopologyChangeAction.CREATE, true);
        return partition;
    }

    private Partition createHashPartition(String name, int modulus, int remainder) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createListPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createHashPartition(this.sqlgGraph, this, name, modulus, remainder);
        this.uncommittedPartitions.put(name, partition);
        markDirty();
        getSchema().markDirty();
        if (this instanceof EdgeLabel edgeLabel) {
            edgeLabel.getInVertexLabels().forEach(AbstractLabel::markDirty);
            edgeLabel.getOutVertexLabels().forEach(AbstractLabel::markDirty);
        }
        getTopology().fire(partition, null, TopologyChangeAction.CREATE, true);
        return partition;
    }

    private Partition createListPartition(String name, String in) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createListPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createListPartition(this.sqlgGraph, this, name, in);
        this.uncommittedPartitions.put(name, partition);
        markDirty();
        getSchema().markDirty();
        if (this instanceof EdgeLabel edgeLabel) {
            edgeLabel.getInVertexLabels().forEach(AbstractLabel::markDirty);
            edgeLabel.getOutVertexLabels().forEach(AbstractLabel::markDirty);
        }
        getTopology().fire(partition, null, TopologyChangeAction.CREATE, true);
        return partition;
    }

    private Partition createListPartitionWithSubPartition(String name, String in, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createListPartitionWithSubPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createListPartitionWithSubPartition(this.sqlgGraph, this, name, in, partitionType, partitionExpression);
        this.uncommittedPartitions.put(name, partition);
        markDirty();
        getSchema().markDirty();
        if (this instanceof EdgeLabel edgeLabel) {
            edgeLabel.getInVertexLabels().forEach(AbstractLabel::markDirty);
            edgeLabel.getOutVertexLabels().forEach(AbstractLabel::markDirty);
        }
        getTopology().fire(partition, null, TopologyChangeAction.CREATE, true);
        return partition;
    }

    private Partition createHashPartitionWithSubPartition(String name, Integer modulus, Integer remainder, PartitionType partitionType, String partitionExpression) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "createHashPartitionWithSubPartition may not be called for \"%s\"", SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createHashPartitionWithSubPartition(this.sqlgGraph, this, name, modulus, remainder, partitionType, partitionExpression);
        this.uncommittedPartitions.put(name, partition);
        markDirty();
        getSchema().markDirty();
        if (this instanceof EdgeLabel edgeLabel) {
            edgeLabel.getInVertexLabels().forEach(AbstractLabel::markDirty);
            edgeLabel.getOutVertexLabels().forEach(AbstractLabel::markDirty);
        }
        getTopology().fire(partition, null, TopologyChangeAction.CREATE, true);
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

            getTopology().startSchemaChange(
                    String.format("AbstractLabel '%s' ensureIndexExists with '%s' '%s'", getFullName(), indexType.getName(), properties.stream().map(PropertyColumn::getName).reduce((a, b) -> a + "," + b).orElse(""))
            );
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
                this.getTopology().startSchemaChange(
                        String.format("AbstractLabel '%s' ensureIndexExists with '%s' '%s'", getFullName(), indexType.getName(), properties.stream().map(PropertyColumn::getName).reduce((a, b) -> a + "," + b).orElse(""))
                );
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

    protected Index createIndex(String indexName, IndexType indexType, List<PropertyColumn> properties) {
        Index index = Index.createIndex(this.sqlgGraph, this, indexName, indexType, properties);
        this.uncommittedIndexes.put(indexName, index);
        markDirty();
        getSchema().markDirty();
        this.getTopology().fire(index, null, TopologyChangeAction.CREATE, true);
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
            if (!this.uncommittedPartitions.isEmpty()) {
                result = this.uncommittedPartitions.get(name);
            }
        }
        if (result == null) {
            result = this.partitions.get(name);
        }
        if (result == null) {
            if (!this.uncommittedPartitions.isEmpty()) {
                for (Partition uncommittedPartition : this.uncommittedPartitions.values()) {
                    Optional<Partition> p = uncommittedPartition.getPartition(name);
                    if (p.isPresent()) {
                        return p;
                    }
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
        if (getTopology().isSchemaChanged()) {
            Map<String, Partition> result = new HashMap<>(this.partitions);
            if (!this.uncommittedPartitions.isEmpty()) {
                result.putAll(this.uncommittedPartitions);
            }
            for (String s : this.uncommittedRemovedPartitions) {
                result.remove(s);
            }
            return Collections.unmodifiableMap(result);
        } else {
            return Collections.unmodifiableMap(this.partitions);
        }
    }

    public Map<String, PropertyColumn> getProperties() {
        if (getTopology().isSchemaChanged()) {
            Map<String, PropertyColumn> result = new HashMap<>(this.properties);
            if (!this.uncommittedProperties.isEmpty()) {
                result.putAll(this.uncommittedProperties);
            }
            if (!this.uncommittedUpdatedProperties.isEmpty()) {
                result.putAll(this.uncommittedUpdatedProperties);
            }
            for (String s : this.uncommittedRemovedProperties) {
                result.remove(s);
            }
            return Collections.unmodifiableMap(result);
        } else {
            return Collections.unmodifiableMap(this.properties);
        }
    }

    public ListOrderedSet<String> getIdentifiers() {
        if (getTopology().isSchemaChanged()) {
            ListOrderedSet<String> result = ListOrderedSet.listOrderedSet(new ArrayList<>(this.identifiers.asList()));
            result.addAll(this.uncommittedIdentifiers);
            for (Pair<String, String> oldNew : this.renamedIdentifiers) {
                result.remove(oldNew.getLeft());
                result.add(oldNew.getRight());
            }
            return result;
        } else {
            return ListOrderedSet.listOrderedSet(UnmodifiableList.unmodifiableList(this.identifiers.asList()));
        }
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
        if (getTopology().isSchemaChanged()) {
            Map<String, Index> result = new HashMap<>(this.indexes);
            if (!this.uncommittedIndexes.isEmpty()) {
                result.putAll(this.uncommittedIndexes);
            }
            for (String i : this.uncommittedRemovedIndexes) {
                result.remove(i);
            }
            return Collections.unmodifiableMap(result);
        } else {
            return Collections.unmodifiableMap(this.indexes);
        }
    }

    public Optional<Index> getIndex(String key) {
        Index index = getIndexes().get(key);
        if (index != null) {
            return Optional.of(index);
        } else {
            return Optional.empty();
        }
    }

    Map<String, PropertyDefinition> getPropertyDefinitionMap() {
        if (getTopology().isSchemaChanged()) {
            Map<String, PropertyDefinition> result = new HashMap<>();
            for (Map.Entry<String, PropertyColumn> propertyEntry : this.properties.entrySet()) {
                result.put(propertyEntry.getKey(), propertyEntry.getValue().getPropertyDefinition());
            }
            if (!this.uncommittedProperties.isEmpty()) {
                for (Map.Entry<String, PropertyColumn> uncommittedPropertyEntry : this.uncommittedProperties.entrySet()) {
                    result.put(uncommittedPropertyEntry.getKey(), uncommittedPropertyEntry.getValue().getPropertyDefinition());
                }
            }
            for (String s : this.uncommittedRemovedProperties) {
                result.remove(s);
            }
            return Collections.unmodifiableMap(result);
        } else {
            Map<String, PropertyDefinition> result = new HashMap<>();
            for (Map.Entry<String, PropertyColumn> propertyEntry : this.properties.entrySet()) {
                result.put(propertyEntry.getKey(), propertyEntry.getValue().getPropertyDefinition());
            }
            return Collections.unmodifiableMap(result);
        }
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

    static void buildColumns(SqlgGraph sqlgGraph, ListOrderedSet<String> identifiers, Map<String, PropertyDefinition> columns, StringBuilder sql) {
        int i = 1;
        //This is to make the columns sorted
        List<String> keys = new ArrayList<>(columns.keySet());
        //if there are identifiers, do them first.
        for (String identifier : identifiers) {
            PropertyType propertyType = columns.get(identifier).propertyType();
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
            PropertyType propertyType = columns.get(column).propertyType();
            Multiplicity multiplicity = columns.get(column).multiplicity();
            String defaultLiteral = columns.get(column).defaultLiteral();
            String checkConstraint = columns.get(column).checkConstraint();
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
                if (defaultLiteral != null) {
                    sql.append(" DEFAULT ");
                    sql.append(defaultLiteral);
                }
                if (multiplicity.isRequired()) {
                    sql.append(" NOT NULL");
                }
                if (propertyType.isArray() && multiplicity.hasLimits()) {
                    if (checkConstraint != null) {
                        sql.append(" CHECK ").append("((").append(checkConstraint).append(") AND (");
                        sql.append(multiplicity.toCheckConstraint(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column))).append("))");
                    } else {
                        sql.append(" CHECK ").append("(").append(multiplicity.toCheckConstraint(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column))).append(")");
                    }
                } else {
                    if (checkConstraint != null) {
                        sql.append(" CHECK ").append("(").append(checkConstraint).append(")");
                    }
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

    void addColumn(String schema, String table, ImmutablePair<String, PropertyDefinition> keyValue) {
        int count = 1;
        PropertyDefinition propertyDefinition = keyValue.getRight();
        PropertyType propertyType = propertyDefinition.propertyType();
        Multiplicity multiplicity = propertyDefinition.multiplicity();
        String defaultLiteral = propertyDefinition.defaultLiteral();
        String checkConstraint = propertyDefinition.checkConstraint();
        String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyDefinition.propertyType());
        for (String sqlDefinition : propertyTypeToSqlDefinition) {
            StringBuilder sql = new StringBuilder("ALTER TABLE ");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sql.append(" ADD ");
            String column;
            if (count > 1) {
                column = keyValue.getLeft() + keyValue.getRight().propertyType().getPostFixes()[count - 2];
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
            } else {
                //The first column existVertexLabel no postfix
                column = keyValue.getLeft();
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
            }

            count++;
            sql.append(" ");
            sql.append(sqlDefinition);

            if (defaultLiteral != null) {
                sql.append(" DEFAULT ");
                sql.append(defaultLiteral);
            }
            if (multiplicity.isRequired()) {
                sql.append(" NOT NULL");
            }
            if (propertyType.isArray() && multiplicity.hasLimits()) {
                if (checkConstraint != null) {
                    sql.append(" CHECK ").append("((").append(checkConstraint).append(") AND (");
                    sql.append(multiplicity.toCheckConstraint(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column))).append("))");
                } else {
                    sql.append(" CHECK ").append("(").append(multiplicity.toCheckConstraint(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column))).append(")");
                }
            } else {
                if (checkConstraint != null) {
                    sql.append(" CHECK ").append("(").append(checkConstraint).append(")");
                }
            }

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

    void addToPropertyColumns(PropertyColumn propertyColumn) {
        this.properties.put(propertyColumn.getName(), propertyColumn);
    }

    void addPropertyColumn(Vertex propertyVertex) {
        Preconditions.checkState(getTopology().isSchemaChanged());
        VertexProperty<String> defaultLiteralProperty = propertyVertex.property(SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL);
        VertexProperty<String> checkConstraintProperty = propertyVertex.property(SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT);
        PropertyColumn property = new PropertyColumn(
                this,
                propertyVertex.value(SQLG_SCHEMA_PROPERTY_NAME),
                PropertyDefinition.of(
                        PropertyType.valueOf(propertyVertex.value(SQLG_SCHEMA_PROPERTY_TYPE)),
                        Multiplicity.of(
                                propertyVertex.value(SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER),
                                propertyVertex.value(SQLG_SCHEMA_PROPERTY_MULTIPLICITY_UPPER)
                        ),
                        defaultLiteralProperty.value(),
                        checkConstraintProperty.value()
                )
        );
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

    void addDistributionPropertyColumn(Vertex distributionProperty) {
        Preconditions.checkState(this.getTopology().isSchemaChanged());
        this.distributionPropertyColumn = new PropertyColumn(
                this,
                distributionProperty.value(SQLG_SCHEMA_PROPERTY_NAME),
                PropertyDefinition.of(PropertyType.valueOf(distributionProperty.value(SQLG_SCHEMA_PROPERTY_TYPE)))
        );
    }

    Partition addPartition(
            String from,
            String to,
            String in,
            Integer modulus,
            Integer remainder,
            String partitionType,
            String partitionExpression,
            String name) {

        Preconditions.checkState(this.getTopology().isSchemaChanged());
        Partition partition;
        if (from != null) {
            Preconditions.checkState(to != null);
            Preconditions.checkState(in == null);
            Preconditions.checkState(modulus == null);
            Preconditions.checkState(remainder == null);
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    name,
                    from,
                    to,
                    PartitionType.from(partitionType),
                    partitionExpression);
        } else if (in != null) {
            Preconditions.checkState(to == null);
            Preconditions.checkState(modulus == null);
            Preconditions.checkState(remainder == null);
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    name,
                    in,
                    PartitionType.from(partitionType),
                    partitionExpression);
        } else {
            Preconditions.checkState(modulus != null);
            Preconditions.checkState(remainder != null);
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    name,
                    modulus,
                    remainder,
                    PartitionType.from(partitionType),
                    partitionExpression);
        }
        this.partitions.put(name, partition);
        return partition;

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
        if (from.value() != null) {
            Preconditions.checkState(to.value() != null);
            Preconditions.checkState(in.value() == null);
            Preconditions.checkState(modulus.value() == null);
            Preconditions.checkState(remainder.value() == null);
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                    from.value(),
                    to.value(),
                    PartitionType.from(partitionType.value()),
                    partitionExpression.value());
        } else if (in.value() != null) {
            Preconditions.checkState(in.value() != null);
            Preconditions.checkState(to.value() == null);
            Preconditions.checkState(modulus.value() == null);
            Preconditions.checkState(remainder.value() == null);
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                    in.value(),
                    PartitionType.from(partitionType.value()),
                    partitionExpression.value());
        } else {
            Preconditions.checkState(modulus.value() != null);
            Preconditions.checkState(remainder.value() != null);
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                    modulus.value(),
                    remainder.value(),
                    PartitionType.from(partitionType.value()),
                    partitionExpression.value());
        }
        this.partitions.put(partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME), partition);
        return partition;
    }

    void afterCommit() {
        if (!this.uncommittedProperties.isEmpty()) {
            this.properties.putAll(this.uncommittedProperties);
            this.uncommittedProperties.clear();
        }
        if (!this.uncommittedRemovedProperties.isEmpty()) {
            for (String prop : this.uncommittedRemovedProperties) {
                this.properties.remove(prop);
            }
            this.uncommittedRemovedProperties.clear();
        }
        if (!this.uncommittedUpdatedProperties.isEmpty()) {
            this.properties.putAll(this.uncommittedUpdatedProperties);
            this.uncommittedUpdatedProperties.clear();
        }
        this.identifiers.addAll(this.uncommittedIdentifiers);
        if (!this.renamedIdentifiers.isEmpty()) {
            int index = -1;
            for (Pair<String, String> renamedIdentifier : this.renamedIdentifiers) {
                index++;
                this.identifiers.remove(renamedIdentifier.getLeft());
                this.identifiers.add(index, renamedIdentifier.getRight());
            }
            this.renamedIdentifiers.clear();
        }
        this.uncommittedIdentifiers.clear();
        if (!this.uncommittedIndexes.isEmpty()) {
            for (Map.Entry<String, Index> entry : this.uncommittedIndexes.entrySet()) {
                this.indexes.put(entry.getKey(), entry.getValue());
                entry.getValue().afterCommit();
            }
            this.uncommittedIndexes.clear();
        }
        if (!this.uncommittedRemovedIndexes.isEmpty()) {
            for (String prop : this.uncommittedRemovedIndexes) {
                this.indexes.remove(prop);
            }
            this.uncommittedRemovedIndexes.clear();
        }
        //PropertyColumn does not have an afterCommit
//        if (!this.properties.isEmpty()) {
//        }
        if (!this.uncommittedPartitions.isEmpty()) {
            for (Map.Entry<String, Partition> entry : this.uncommittedPartitions.entrySet()) {
                this.partitions.put(entry.getKey(), entry.getValue());
                entry.getValue().afterCommit();
            }
            this.uncommittedPartitions.clear();
        }
        if (!this.uncommittedRemovedPartitions.isEmpty()) {
            for (String prop : this.uncommittedRemovedPartitions) {
                this.partitions.remove(prop);
            }
            this.uncommittedRemovedPartitions.clear();
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
    }

    void afterRollback() {
        Preconditions.checkState(this.getTopology().isSchemaChanged(), "AbstractLabel.afterRollback must have schemaChanged ThreadLocal var as true");
        if (!this.uncommittedProperties.isEmpty()) {
            for (Map.Entry<String, PropertyColumn> entry : this.uncommittedProperties.entrySet()) {
                entry.getValue().afterRollback();
            }
            this.uncommittedProperties.clear();
        }
        if (!this.uncommittedUpdatedProperties.isEmpty()) {
            for (Map.Entry<String, PropertyColumn> entry : this.uncommittedUpdatedProperties.entrySet()) {
                entry.getValue().afterRollback();
            }
            this.uncommittedUpdatedProperties.clear();
        }
        this.uncommittedRemovedProperties.clear();
        this.uncommittedIdentifiers.clear();
        this.renamedIdentifiers.clear();
        if (!this.uncommittedIndexes.isEmpty()) {
            for (Map.Entry<String, Index> entry : this.uncommittedIndexes.entrySet()) {
                entry.getValue().afterRollback();
            }
            this.uncommittedIndexes.clear();
        }
        this.uncommittedRemovedIndexes.clear();
        if (!this.properties.isEmpty()) {
            for (Map.Entry<String, PropertyColumn> entry : this.properties.entrySet()) {
                entry.getValue().afterRollback();
            }
        }
        this.uncommittedRemovedPartitions.clear();
        if (!this.partitions.isEmpty()) {
            for (Map.Entry<String, Partition> entry : this.partitions.entrySet()) {
                entry.getValue().afterRollback();
            }
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
            ObjectNode result = Topology.OBJECT_MAPPER.createObjectNode();
            ArrayNode propertyArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            if (!this.uncommittedProperties.isEmpty()) {
                for (PropertyColumn property : this.uncommittedProperties.values()) {
                    propertyArrayNode.add(property.toNotifyJson());
                }
            }
            ArrayNode updatedPropertyArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            if (!this.uncommittedUpdatedProperties.isEmpty()) {
                for (PropertyColumn property : this.uncommittedUpdatedProperties.values()) {
                    updatedPropertyArrayNode.add(property.toNotifyJson());
                }
            }
            ArrayNode removedPropertyArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String property : this.uncommittedRemovedProperties) {
                removedPropertyArrayNode.add(property);
            }
            ArrayNode identifierArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String identifier : this.uncommittedIdentifiers) {
                identifierArrayNode.add(identifier);
            }
            ArrayNode renamedIdentifierArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (Pair<String, String> oldNew : this.renamedIdentifiers) {
                ObjectNode renamedObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
                renamedObjectNode.put("old", oldNew.getLeft());
                renamedObjectNode.put("new", oldNew.getRight());
                renamedIdentifierArrayNode.add(renamedObjectNode);
            }
            ArrayNode uncommittedPartitionArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            if (!this.uncommittedPartitions.isEmpty()) {
                for (Partition partition : this.uncommittedPartitions.values()) {
                    Optional<ObjectNode> json = partition.toUncommittedPartitionNotifyJson();
                    json.ifPresent(uncommittedPartitionArrayNode::add);
                }
            }
            ArrayNode removedPartitionArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String partition : this.uncommittedRemovedPartitions) {
                removedPartitionArrayNode.add(partition);
            }
            ArrayNode committedPartitionArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
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
                uncommittedDistributionColocateAbstractLabelObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
                uncommittedDistributionColocateAbstractLabelObjectNode.put("colocateLabel", colocateLabel);
            }
            ObjectNode uncommittedShardCountObjectNode = null;
            if (this.uncommittedShardCount != -1) {
                uncommittedShardCountObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
                uncommittedShardCountObjectNode.put("uncommittedShardCount", this.uncommittedShardCount);
            }

            ArrayNode indexArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            if (!this.uncommittedIndexes.isEmpty()) {
                for (Index index : this.uncommittedIndexes.values()) {
                    Optional<JsonNode> indexJsonOptional = index.toNotifyJson();
                    Preconditions.checkState(indexJsonOptional.isPresent());
                    indexArrayNode.add(indexJsonOptional.get());
                }
            }
            ArrayNode removedIndexArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String property : this.uncommittedRemovedIndexes) {
                removedIndexArrayNode.add(property);
            }
            result.set("uncommittedProperties", propertyArrayNode);
            result.set("uncommittedUpdatedProperties", updatedPropertyArrayNode);
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
            if (propertyArrayNode.isEmpty() && uncommittedUpdatedProperties.isEmpty() && removedPropertyArrayNode.isEmpty() &&
                    identifierArrayNode.isEmpty() &&
                    uncommittedPartitionArrayNode.isEmpty() && removedPartitionArrayNode.isEmpty() && committedPartitionArrayNode.isEmpty() &&
                    indexArrayNode.isEmpty() && removedIndexArrayNode.isEmpty() &&
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
                    this.getTopology().fire(propertyColumn, null, TopologyChangeAction.CREATE, false);
                }
            }
        }
        ArrayNode updatedPropertiesNode = (ArrayNode) vertexLabelJson.get("uncommittedUpdatedProperties");
        if (updatedPropertiesNode != null) {
            for (JsonNode propertyNode : updatedPropertiesNode) {
                PropertyColumn propertyColumn = PropertyColumn.fromNotifyJson(this, propertyNode);
                PropertyColumn old = this.properties.put(propertyColumn.getName(), propertyColumn);
                if (fire) {
                    this.getTopology().fire(propertyColumn, old, TopologyChangeAction.UPDATE, false);
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
                ObjectNode identifierObjectNode = (ObjectNode) identifierNode;
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
                    this.getTopology().fire(old, old, TopologyChangeAction.DELETE, false);
                }
            }
        }
        ArrayNode uncommittedPartitionsNode = (ArrayNode) vertexLabelJson.get("uncommittedPartitions");
        if (uncommittedPartitionsNode != null) {
            for (JsonNode partitionNode : uncommittedPartitionsNode) {
                Partition partitionColumn = Partition.fromUncommittedPartitionNotifyJson(this, partitionNode);
                Partition old = this.partitions.put(partitionColumn.getName(), partitionColumn);
                if (fire && old == null) {
                    this.getTopology().fire(partitionColumn, null, TopologyChangeAction.CREATE, false);
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
                        this.getTopology().fire(committedPartition, null, TopologyChangeAction.CREATE, false);
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
                    this.getTopology().fire(old, old, TopologyChangeAction.DELETE, false);
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
                this.getTopology().fire(index, null, TopologyChangeAction.CREATE, false);
            }
        }
        ArrayNode removedIndexArrayNode = (ArrayNode) vertexLabelJson.get("uncommittedRemovedIndexes");
        if (removedIndexArrayNode != null) {
            for (JsonNode indexNode : removedIndexArrayNode) {
                String iName = indexNode.asText();
                Index old = this.indexes.remove(iName);
                if (fire && old != null) {
                    this.getTopology().fire(old, old, TopologyChangeAction.DELETE, false);
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof AbstractLabel other)) {
            return false;
        }
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
     * Updates the {@link PropertyDefinition}. Only the multiplicity, defaultLiteral and checkConstraint can be updated.
     * The {@link PropertyType} of the property can not be updated.
     *
     * @param propertyColumn     The column to update.
     * @param propertyDefinition The {@link PropertyDefinition} to update to.
     */
    abstract void updatePropertyDefinition(PropertyColumn propertyColumn, PropertyDefinition propertyDefinition);

    protected void internalUpdatePropertyDefinition(PropertyColumn propertyColumn, PropertyDefinition propertyDefinition, PropertyDefinition currentPropertyDefinition, String name, PropertyColumn copy) {
        if (currentPropertyDefinition.multiplicity().isRequired() && !propertyDefinition.multiplicity().isRequired()) {
            //remove NOT NULL
            removeNotNull(getSchema().getName(), getPrefix() + getLabel(), name);
        }
        if (!currentPropertyDefinition.multiplicity().isRequired() && propertyDefinition.multiplicity().isRequired()) {
            //add NOT NULL
            addNotNull(getSchema().getName(), getPrefix() + getLabel(), name, currentPropertyDefinition.propertyType());
        }
        if (currentPropertyDefinition.defaultLiteral() != null && propertyDefinition.defaultLiteral() == null) {
            //remove defaultLiteral
            removeDefaultLiteral(getSchema().getName(), getPrefix() + getLabel(), name);
        }
        if (currentPropertyDefinition.defaultLiteral() == null && propertyDefinition.defaultLiteral() != null) {
            //add defaultLiteral
            addDefaultLiteral(getSchema().getName(), getPrefix() + getLabel(), name, propertyDefinition.defaultLiteral());
        }
        if (currentPropertyDefinition.defaultLiteral() != null && propertyDefinition.defaultLiteral() != null &&
                !currentPropertyDefinition.defaultLiteral().equals(propertyDefinition.defaultLiteral())) {

            //remove defaultLiteral
            removeDefaultLiteral(getSchema().getName(), getPrefix() + getLabel(), name);
            //add defaultLiteral
            addDefaultLiteral(getSchema().getName(), getPrefix() + getLabel(), name, propertyDefinition.defaultLiteral());
        }
        if (!currentPropertyDefinition.propertyType().isArray()) {
            if (currentPropertyDefinition.checkConstraint() != null && propertyDefinition.checkConstraint() == null) {
                //remove checkConstraint
                String checkConstraintName = sqlgGraph.getSqlDialect().checkConstraintName(sqlgGraph, getSchema().getName(), getPrefix() + getName(), name, currentPropertyDefinition.checkConstraint());
                Preconditions.checkNotNull(checkConstraintName, "Failed to fine check constraint for '%s', '%s' '%s'", getSchema().getName(), getName(), name);
                removeCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), checkConstraintName);
            }
            if (currentPropertyDefinition.checkConstraint() == null && propertyDefinition.checkConstraint() != null) {
                //add checkConstraint
                addCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), name, null, propertyDefinition.checkConstraint());
            }
            if (currentPropertyDefinition.checkConstraint() != null && propertyDefinition.checkConstraint() != null &&
                    !currentPropertyDefinition.checkConstraint().equals(propertyDefinition.checkConstraint())) {

                //add new checkConstraint
                //remove checkConstraint
                String checkConstraintName = sqlgGraph.getSqlDialect().checkConstraintName(sqlgGraph, getSchema().getName(), getPrefix() + getName(), name, currentPropertyDefinition.checkConstraint());
                Preconditions.checkNotNull(checkConstraintName, "Failed to fine check constraint for '%s', '%s' '%s'", getSchema().getName(), getName(), name);
                removeCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), checkConstraintName);
                //add checkConstraint
                addCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), name, null, propertyDefinition.checkConstraint());
            }
        } else {
            //arrays
            if ((currentPropertyDefinition.multiplicity().hasLimits() && !propertyDefinition.multiplicity().hasLimits()) ||
                    (currentPropertyDefinition.checkConstraint() != null && propertyDefinition.checkConstraint() == null) ||
                    (propertyDefinition.multiplicity().hasLimits() && !currentPropertyDefinition.multiplicity().equals(propertyDefinition.multiplicity())) ||
                    (currentPropertyDefinition.checkConstraint() != null && !currentPropertyDefinition.checkConstraint().equals(propertyDefinition.checkConstraint()))) {
                //remove checkConstraint
            }
            if (propertyDefinition.multiplicity().hasLimits() && propertyDefinition.checkConstraint() != null) {
                if (currentPropertyDefinition.multiplicity().hasLimits() || currentPropertyDefinition.checkConstraint() != null) {
                    //remove checkConstraint
                    String checkConstraintName = sqlgGraph.getSqlDialect().checkConstraintName(sqlgGraph, getSchema().getName(), getPrefix() + getName(), name, currentPropertyDefinition.checkConstraint());
                    Preconditions.checkNotNull(checkConstraintName, "Failed to find check constraint for '%s', '%s' '%s'", getSchema().getName(), getName(), name);
                    removeCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), checkConstraintName);
                }
                //add checkConstraint
                addCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), name, propertyDefinition.multiplicity(), propertyDefinition.checkConstraint());
            } else if (propertyDefinition.multiplicity().hasLimits()) {
                if (currentPropertyDefinition.multiplicity().hasLimits() || currentPropertyDefinition.checkConstraint() != null) {
                    //remove checkConstraint
                    String checkConstraintName = sqlgGraph.getSqlDialect().checkConstraintName(sqlgGraph, getSchema().getName(), getPrefix() + getName(), name, currentPropertyDefinition.checkConstraint());
                    Preconditions.checkNotNull(checkConstraintName, "Failed to fine check constraint for '%s', '%s' '%s'", getSchema().getName(), getName(), name);
                    removeCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), checkConstraintName);
                }
                //add checkConstraint
                addCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), name, propertyDefinition.multiplicity(), null);

            } else if (propertyDefinition.checkConstraint() != null) {
                if (currentPropertyDefinition.multiplicity().hasLimits() || currentPropertyDefinition.checkConstraint() != null) {
                    //remove checkConstraint
                    String checkConstraintName = sqlgGraph.getSqlDialect().checkConstraintName(sqlgGraph, getSchema().getName(), getPrefix() + getName(), name, currentPropertyDefinition.checkConstraint());
                    Preconditions.checkNotNull(checkConstraintName, "Failed to fine check constraint for '%s', '%s' '%s'", getSchema().getName(), getName(), name);
                    removeCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), checkConstraintName);
                }
                //add checkConstraint
                addCheckConstraint(getSchema().getName(), getPrefix() + getLabel(), name, null, propertyDefinition.checkConstraint());
            }
        }
        this.getSchema().getTopology().fire(copy, propertyColumn, TopologyChangeAction.UPDATE, true);
    }

    void addDefaultLiteral(String schema, String table, String column, String defaultLiteral) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
        sql.append(" ALTER COLUMN ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
        sql.append(" SET DEFAULT ");
        sql.append(defaultLiteral);
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

    void addNotNull(String schema, String table, String column, PropertyType propertyType) {
        String sql = this.sqlgGraph.getSqlDialect().addNotNullConstraint(this.sqlgGraph, schema, table, column, propertyType);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql);
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void removeCheckConstraint(String schema, String table, String checkConstraintName) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
        sql.append(" DROP CONSTRAINT ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(checkConstraintName));
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void addCheckConstraint(String schema, String table, String column, Multiplicity multiplicity, String checkConstraint) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
        sql.append(" ADD CONSTRAINT ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table + "_" + column));
        if (multiplicity != null && checkConstraint != null) {
            sql.append(" CHECK ").append("((").append(checkConstraint).append(") AND (");
            sql.append(multiplicity.toCheckConstraint(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column))).append("))");
        } else if (multiplicity != null) {
            sql.append(" CHECK ").append("(").append(multiplicity.toCheckConstraint(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column))).append(")");
        } else {
            sql.append(" CHECK ").append("(").append(checkConstraint).append(")");

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

    void removeDefaultLiteral(String schema, String table, String column) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
        sql.append(" ALTER COLUMN ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
        sql.append(" DROP DEFAULT");
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

    void removeNotNull(String schema, String table, String column) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
        sql.append(" ALTER COLUMN ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
        sql.append(" DROP NOT NULL");
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
     * @param schema  the schema
     * @param table   the table name
     * @param column  the column to rename
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
        this.getTopology().startSchemaChange(
                String.format("AbstractLabel '%s' removeIndex with '%s'", getFullName(), idx.getName())
        );
        if (!uncommittedRemovedIndexes.contains(idx.getName())) {
            uncommittedRemovedIndexes.add(idx.getName());
            markDirty();
            getSchema().markDirty();
            TopologyManager.removeIndex(this.sqlgGraph, idx);
            if (!preserveData) {
                idx.delete(sqlgGraph);
            }
            this.getTopology().fire(idx, idx, TopologyChangeAction.DELETE, true);
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
        this.getTopology().startSchemaChange(
                String.format("AbstractLabel '%s' removePartition with '%s'", getFullName(), partition.getName())
        );

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
            markDirty();
            getSchema().markDirty();
            if (this instanceof EdgeLabel edgeLabel) {
                edgeLabel.getInVertexLabels().forEach(AbstractLabel::markDirty);
                edgeLabel.getOutVertexLabels().forEach(AbstractLabel::markDirty);
            }
            this.getTopology().fire(partition, partition, TopologyChangeAction.DELETE, true);
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
            this.getTopology().startSchemaChange(
                    String.format("AbstractLabel '%s' ensureDistributed with '%s'", getFullName(), distributionPropertyColumn.getName())
            );
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
