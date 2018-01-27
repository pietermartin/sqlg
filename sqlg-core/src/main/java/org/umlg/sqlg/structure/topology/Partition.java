package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/13
 */
public class Partition implements TopologyInf {

    private static Logger logger = LoggerFactory.getLogger(Partition.class);
    protected SqlgGraph sqlgGraph;
    private String name;
    private String from;
    private String to;
    private String in;
    private AbstractLabel abstractLabel;
    protected boolean committed = true;

    private PartitionType partitionType = PartitionType.NONE;
    private String partitionExpression;

    private Partition parentPartition;
    protected Map<String, Partition> partitions = new HashMap<>();
    Map<String, Partition> uncommittedPartitions = new HashMap<>();
    Set<String> uncommittedRemovedPartitions = new HashSet<>();

    Partition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, String from, String to) {
        this.sqlgGraph = sqlgGraph;
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.from = from;
        this.to = to;
    }

    Partition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, String in) {
        this.sqlgGraph = sqlgGraph;
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.in = in;
    }

    private Partition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        this.sqlgGraph = sqlgGraph;
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.from = from;
        this.to = to;
        this.partitionType = partitionType;
        this.partitionExpression = partitionExpression;
    }

    private Partition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        this.sqlgGraph = sqlgGraph;
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.in = in;
        this.partitionType = partitionType;
        this.partitionExpression = partitionExpression;
    }

    private Partition(SqlgGraph sqlgGraph, Partition parentPartition, String name, String from, String to) {
        this.sqlgGraph = sqlgGraph;
        this.name = name;
        this.from = from;
        this.to = to;
        this.parentPartition = parentPartition;
    }

    private Partition(SqlgGraph sqlgGraph, Partition parentPartition, String name, String in) {
        this.sqlgGraph = sqlgGraph;
        this.name = name;
        this.in = in;
        this.parentPartition = parentPartition;
    }

    private Partition(
            SqlgGraph sqlgGraph,
            Partition parentPartition,
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        this.sqlgGraph = sqlgGraph;
        this.name = name;
        this.from = from;
        this.to = to;
        this.parentPartition = parentPartition;
        this.partitionType = partitionType;
        this.partitionExpression = partitionExpression;
    }

    private Partition(
            SqlgGraph sqlgGraph,
            Partition parentPartition,
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        this.sqlgGraph = sqlgGraph;
        this.name = name;
        this.in = in;
        this.parentPartition = parentPartition;
        this.partitionType = partitionType;
        this.partitionExpression = partitionExpression;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public String getIn() {
        return in;
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    @Override
    public String getName() {
        return this.name;
    }

    public AbstractLabel getAbstractLabel() {
        if (this.abstractLabel == null) {
            return this.parentPartition.getAbstractLabel();
        } else {
            return this.abstractLabel;
        }
    }

    public Partition getParentPartition() {
        return parentPartition;
    }

    @Override
    public void remove(boolean preserveData) {
        if (this.abstractLabel != null) {
            this.abstractLabel.removePartition(this, preserveData);
        } else {
            this.parentPartition.removePartition(this, preserveData);
        }
    }

    private void removePartition(Partition partition, boolean preserveData) {
        this.getAbstractLabel().getSchema().getTopology().lock();
        for (Partition partition1 : partition.getPartitions().values()) {
            partition.removePartition(partition1, preserveData);
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
            this.getAbstractLabel().getSchema().getTopology().fire(partition, "", TopologyChangeAction.DELETE);
        }
    }

    /**
     * Create a range partition on an {@link AbstractLabel}
     *
     * @param sqlgGraph
     * @param abstractLabel
     * @param name
     * @param from
     * @param to
     * @return
     */
    static Partition createRangePartition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, String from, String to) {
        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createRangePartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, from, to);
        partition.createRangePartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(sqlgGraph, abstractLabel, name, from, to);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, from, to);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a list partition on an {@link AbstractLabel}
     *
     * @param sqlgGraph
     * @param abstractLabel
     * @param name
     * @param in
     * @return
     */
    static Partition createListPartition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, String in) {
        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createListPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, in);
        partition.createListPartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(sqlgGraph, abstractLabel, name, in);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, in);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a range partition on an {@link AbstractLabel} that will itself be sub-partitioned
     *
     * @param sqlgGraph
     * @param abstractLabel
     * @param name
     * @param from
     * @param to
     * @param partitionType
     * @param partitionExpression
     * @return
     */
    static Partition createRangePartitionWithSubPartition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createRangePartitionWithSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Preconditions.checkState(abstractLabel.isRangePartition());
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, from, to, partitionType, partitionExpression);
        partition.createRangePartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(sqlgGraph, abstractLabel, name, from, to, partitionType, partitionExpression);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, from, to, partitionType, partitionExpression);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a list partition that will itself be sub-partitioned.
     *
     * @param sqlgGraph
     * @param abstractLabel
     * @param name
     * @param in
     * @param partitionType
     * @param partitionExpression
     * @return
     */
    static Partition createListPartitionWithSubPartition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createListSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, in, partitionType, partitionExpression);
        partition.createListPartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(sqlgGraph, abstractLabel, name, in, partitionType, partitionExpression);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, in);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a range partition on an existing {@link Partition}
     *
     * @param sqlgGraph
     * @param parentPartition
     * @param name
     * @param from
     * @param to
     * @return
     */
    private static Partition createRangeSubPartition(SqlgGraph sqlgGraph, Partition parentPartition, String name, String from, String to) {
        Preconditions.checkArgument(!parentPartition.getAbstractLabel().getSchema().isSqlgSchema(), "createRangeSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, parentPartition, name, from, to);
        partition.createRangePartitionOnDb();
        TopologyManager.addSubPartition(sqlgGraph, partition, name, from, to);
        partition.committed = false;
        return partition;
    }

    /**
     * Create a range partition on an existing {@link Partition}
     *
     * @param sqlgGraph
     * @param parentPartition
     * @param name
     * @param from
     * @param to
     * @return
     */
    private static Partition createRangeSubPartitionWithPartition(
            SqlgGraph sqlgGraph,
            Partition parentPartition,
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        Preconditions.checkArgument(!parentPartition.getAbstractLabel().getSchema().isSqlgSchema(), "createRangeSubPartitionWithPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, parentPartition, name, from, to, partitionType, partitionExpression);
        partition.createRangePartitionOnDb();
        TopologyManager.addSubPartition(sqlgGraph, partition, name, from, to);
        partition.committed = false;
        return partition;
    }

    private static Partition createListSubPartitionWithPartition(
            SqlgGraph sqlgGraph,
            Partition parentPartition,
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        Preconditions.checkArgument(!parentPartition.getAbstractLabel().getSchema().isSqlgSchema(), "createRangeSubPartitionWithPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, parentPartition, name, in, partitionType, partitionExpression);
        partition.createListPartitionOnDb();
        TopologyManager.addSubPartition(sqlgGraph, partition, name, in);
        partition.committed = false;
        return partition;
    }

    /**
     * Create a list partition on an existing {@link Partition}
     *
     * @param sqlgGraph
     * @param parentPartition
     * @param name
     * @param in
     * @return
     */
    private static Partition createListSubPartition(SqlgGraph sqlgGraph, Partition parentPartition, String name, String in) {
        Preconditions.checkArgument(!parentPartition.getAbstractLabel().getSchema().isSqlgSchema(), "createPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, parentPartition, name, in);
        partition.createListPartitionOnDb();
        TopologyManager.addSubPartition(sqlgGraph, partition, name, in);
        partition.committed = false;
        return partition;
    }


    private void createRangePartitionOnDb() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        sql.append(" PARTITION OF ");
        if (this.parentPartition == null) {
            Preconditions.checkState(this.abstractLabel != null, "If Partition.parentPartition is null it must have an abstractLabel.");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.abstractLabel.getSchema().getName()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.abstractLabel.getPrefix() + this.abstractLabel.getLabel()));
        } else {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parentPartition.getAbstractLabel().getSchema().getName()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parentPartition.name));
        }
        sql.append(" FOR VALUES FROM (");
        sql.append(this.from);
        sql.append(") TO (");
        sql.append(this.to);
        sql.append(")");
        if (!this.partitionType.isNone()) {
            sql.append("\nPARTITION BY ");
            sql.append(this.partitionType.name());
            sql.append("(");
            sql.append(this.partitionExpression);
            sql.append(")");
        }
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createListPartitionOnDb() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        sql.append(" PARTITION OF ");
        if (this.parentPartition == null) {
            Preconditions.checkState(this.abstractLabel != null, "If Partition.parentPartition is null it must have an abstractLabel.");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.abstractLabel.getSchema().getName()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.abstractLabel.getPrefix() + this.abstractLabel.getLabel()));
        } else {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parentPartition.getAbstractLabel().getSchema().getName()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.parentPartition.name));
        }
        sql.append(" FOR VALUES IN (");
        sql.append(this.in);
        sql.append(")");
        if (!this.partitionType.isNone()) {
            sql.append("\nPARTITION BY ");
            sql.append(this.partitionType.name());
            sql.append("(");
            sql.append(this.partitionExpression);
            sql.append(")");
        }
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void delete() {
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void detach() {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.abstractLabel.getPrefix() + this.abstractLabel.getName()));
        sql.append(" DETACH PARTITION ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void afterCommit() {
        Preconditions.checkState(this.getAbstractLabel().getSchema().getTopology().isSqlWriteLockHeldByCurrentThread(), "Partition.afterCommit must hold the write lock");
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
    }

    void afterRollback() {
        Preconditions.checkState(this.getAbstractLabel().getSchema().getTopology().isSqlWriteLockHeldByCurrentThread(), "Partition.afterCommit must hold the write lock");
        this.uncommittedRemovedPartitions.clear();
        for (Iterator<Map.Entry<String, Partition>> it = this.partitions.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Partition> entry = it.next();
            entry.getValue().afterRollback();
        }
    }

    public ObjectNode toNotifyJson() {
        ObjectNode partitionObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        partitionObjectNode.put("name", this.name);
        partitionObjectNode.put("from", this.from);
        partitionObjectNode.put("to", this.to);
        return partitionObjectNode;
    }

    public static Partition fromNotifyJson(AbstractLabel abstractLabel, JsonNode partitionNode) {
        return new Partition(
                abstractLabel.getSchema().getSqlgGraph(),
                abstractLabel,
                partitionNode.get("name").asText(),
                partitionNode.get("from").asText(),
                partitionNode.get("to").asText()
        );
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    protected JsonNode toJson() {
        return toNotifyJson();
    }

    public Partition ensureRangePartitionExist(String name, String from, String to) {
        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Objects.requireNonNull(from, "Sub-partition's \"from\" must not be null");
        Objects.requireNonNull(to, "Sub-partition's \"to\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.RANGE, "ensureRangePartitionExists(String name, String from, String to) can only be called for a RANGE partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (!partitionOptional.isPresent()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            if (!partitionOptional.isPresent()) {
                return this.createRangePartition(name, from, to);
            } else {
                return partitionOptional.get();
            }
        } else {
            return partitionOptional.get();
        }
    }

    public Partition ensureListPartitionExist(String name, String in) {
        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Objects.requireNonNull(in, "Sub-partition's \"in\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.LIST, "ensureRangePartitionExists(String name, String in) can only be called for a LIST partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (!partitionOptional.isPresent()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            if (!partitionOptional.isPresent()) {
                return this.createListPartition(name, in);
            } else {
                return partitionOptional.get();
            }
        } else {
            return partitionOptional.get();
        }
    }

    public Partition ensureRangePartitionWithSubPartitionExist(
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Objects.requireNonNull(from, "Sub-partition's \"from\" must not be null");
        Objects.requireNonNull(to, "Sub-partition's \"to\" must not be null");
        Preconditions.checkState(partitionType == PartitionType.RANGE, "ensureRangePartitionWithSubPartitionExist(String name, String from, String to, PartitionType partitionType, String partitionExpression) can only be called with a RANGE partition. Found %s", partitionType.name());
        Objects.requireNonNull(partitionExpression, "Sub-partition's \"partitionExpression\" must not be null");

        Optional<Partition> partitionOptional = this.getPartition(name);
        if (!partitionOptional.isPresent()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            if (!partitionOptional.isPresent()) {
                return this.createRangePartitionWithSubPartition(name, from, to, partitionType, partitionExpression);
            } else {
                return partitionOptional.get();
            }
        } else {
            return partitionOptional.get();
        }
    }

    public Partition ensureListPartitionWithSubPartitionExist(
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Objects.requireNonNull(in, "Sub-partition's \"in\" must not be null");
        Preconditions.checkState(partitionType == PartitionType.LIST, "ensureListPartitionWithSubPartitionExist(String name, String in, PartitionType partitionType, String partitionExpression) can only be called with a LIST partition. Found %s", partitionType.name());
        Objects.requireNonNull(partitionExpression, "Sub-partition's \"partitionExpression\" must not be null");

        Optional<Partition> partitionOptional = this.getPartition(name);
        if (!partitionOptional.isPresent()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            if (!partitionOptional.isPresent()) {
                return this.createListPartitionWithSubPartition(name, in, partitionType, partitionExpression);
            } else {
                return partitionOptional.get();
            }
        } else {
            return partitionOptional.get();
        }
    }

    private Partition createRangePartition(String name, String from, String to) {
        Preconditions.checkState(!this.getAbstractLabel().getSchema().isSqlgSchema(), "createSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createRangeSubPartition(this.sqlgGraph, this, name, from, to);
        this.uncommittedPartitions.put(name, partition);
        this.getAbstractLabel().getSchema().getTopology().fire(partition, "", TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createListPartition(String name, String in) {
        Preconditions.checkState(!this.getAbstractLabel().getSchema().isSqlgSchema(), "createSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createListSubPartition(this.sqlgGraph, this, name, in);
        this.uncommittedPartitions.put(name, partition);
        this.getAbstractLabel().getSchema().getTopology().fire(partition, "", TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createRangePartitionWithSubPartition(
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        Preconditions.checkState(!this.getAbstractLabel().getSchema().isSqlgSchema(), "createRangePartitionWithSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createRangeSubPartitionWithPartition(
                this.sqlgGraph,
                this,
                name,
                from,
                to,
                partitionType,
                partitionExpression);
        this.uncommittedPartitions.put(name, partition);
        this.getAbstractLabel().getSchema().getTopology().fire(partition, "", TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createListPartitionWithSubPartition(
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        Preconditions.checkState(!this.getAbstractLabel().getSchema().isSqlgSchema(), "createListPartitionWithSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createListSubPartitionWithPartition(
                this.sqlgGraph,
                this,
                name,
                in,
                partitionType,
                partitionExpression);
        this.uncommittedPartitions.put(name, partition);
        this.getAbstractLabel().getSchema().getTopology().fire(partition, "", TopologyChangeAction.CREATE);
        return partition;
    }

    public Map<String, Partition> getPartitions() {
        Map<String, Partition> result = new HashMap<>(this.partitions);
        if (this.getAbstractLabel().getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedPartitions);
            for (String s : this.uncommittedRemovedPartitions) {
                result.remove(s);
            }
        }
        return result;
    }

    public Optional<Partition> getPartition(String name) {
        if (getAbstractLabel().getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() && this.uncommittedRemovedPartitions.contains(name)) {
            return Optional.empty();
        }
        Partition result = null;
        if (this.getAbstractLabel().getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result = this.uncommittedPartitions.get(name);
        }
        if (result == null) {
            result = this.partitions.get(name);
        }
        return Optional.ofNullable(result);
    }

    Partition addPartition(Vertex partitionVertex) {
        Preconditions.checkState(this.getAbstractLabel().getSchema().getTopology().isSqlWriteLockHeldByCurrentThread());
        VertexProperty<String> from = partitionVertex.property(SQLG_SCHEMA_PARTITION_FROM);
        VertexProperty<String> to = partitionVertex.property(SQLG_SCHEMA_PARTITION_TO);
        VertexProperty<String> in = partitionVertex.property(SQLG_SCHEMA_PARTITION_IN);
        Partition partition;
        if (from.isPresent()) {
            Preconditions.checkState(to.isPresent());
            Preconditions.checkState(!in.isPresent());
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                    from.value(),
                    to.value());
        } else {
            Preconditions.checkState(in.isPresent());
            Preconditions.checkState(!to.isPresent());
            partition = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                    in.value());
        }
        this.partitions.put(partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME), partition);
        return partition;
    }
}
