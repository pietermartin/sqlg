package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.util.ThreadLocalMap;
import org.umlg.sqlg.util.ThreadLocalSet;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/01/13
 */
@SuppressWarnings("DuplicatedCode")
public class Partition implements TopologyInf {

    private static final Logger logger = LoggerFactory.getLogger(Partition.class);
    private final SqlgGraph sqlgGraph;
    private final String name;
    private String from;
    private String to;
    private String in;
    private Integer modulus;
    private Integer remainder;
    private AbstractLabel abstractLabel;
    private boolean committed = true;

    private final PartitionType partitionType;
    private final String partitionExpression;

    private Partition parentPartition;
    private final Map<String, Partition> partitions = new ConcurrentHashMap<>();
    private final Map<String, Partition> uncommittedPartitions = new ThreadLocalMap<>();
    private final Set<String> uncommittedRemovedPartitions = new ThreadLocalSet<>();

    public Partition(
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

    public Partition(
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

    public Partition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            Integer modulus,
            Integer remainder,
            PartitionType partitionType,
            String partitionExpression) {

        this.sqlgGraph = sqlgGraph;
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.modulus = modulus;
        this.remainder = remainder;
        this.partitionType = partitionType;
        this.partitionExpression = partitionExpression;
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

    private Partition(
            SqlgGraph sqlgGraph,
            Partition parentPartition,
            String name,
            Integer modulus,
            Integer remainder,
            PartitionType partitionType,
            String partitionExpression) {

        this.sqlgGraph = sqlgGraph;
        this.name = name;
        this.modulus = modulus;
        this.remainder = remainder;
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

    public Integer getModulus() {
        return modulus;
    }

    public Integer getRemainder() {
        return remainder;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public String getPartitionExpression() {
        return partitionExpression;
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
            this.getAbstractLabel().getSchema().getTopology().fire(partition, partition, TopologyChangeAction.DELETE);
        }
    }

    /**
     * Create a range partition on an {@link AbstractLabel}
     *
     * @param sqlgGraph     The graph
     * @param abstractLabel The abstractLabel to partition
     * @param name          The name of the partition
     * @param from          The range partition 'from' clause.
     * @param to            The range partition 'to' clause.
     * @return The newly created partition
     */
    static Partition createRangePartition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, String from, String to) {
        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createRangePartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, from, to, PartitionType.NONE, null);
        partition.createRangePartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(
                    sqlgGraph,
                    abstractLabel.getSchema().getName(),
                    abstractLabel.getName(),
                    name,
                    from,
                    to,
                    PartitionType.NONE,
                    null);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, from, to, PartitionType.NONE, null);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a hash partition on an {@link AbstractLabel}
     *
     * @param sqlgGraph     The graph
     * @param abstractLabel The abstractLabel to partition
     * @param name          The name of the partition
     * @param modulus       The hash modulus
     * @param remainder     The hash remainder
     * @return The newly created partition
     */
    static Partition createHashPartition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, int modulus, int remainder) {
        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createHashPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, modulus, remainder, PartitionType.NONE, null);
        partition.createHashPartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(
                    sqlgGraph,
                    abstractLabel.getSchema().getName(),
                    abstractLabel.getName(),
                    name,
                    modulus,
                    remainder,
                    PartitionType.NONE,
                    null);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, modulus, remainder, PartitionType.NONE, null);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a list partition on an {@link AbstractLabel}
     *
     * @param sqlgGraph     The graph
     * @param abstractLabel The abstractLabel to partition
     * @param name          The name of the partition
     * @param in            The list partition's 'in' clause
     * @return The newly created partition
     */
    static Partition createListPartition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, String in) {
        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createListPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Preconditions.checkState(abstractLabel.isListPartition());
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, in, PartitionType.NONE, null);
        partition.createListPartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(
                    sqlgGraph,
                    abstractLabel.getSchema().getName(),
                    abstractLabel.getName(),
                    name,
                    in,
                    PartitionType.NONE,
                    null);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, in, PartitionType.NONE, null);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a range partition on an {@link AbstractLabel} that will itself be sub-partitioned
     *
     * @param sqlgGraph           The graph
     * @param abstractLabel       The abstractLabel to partition
     * @param name                The name of the partition
     * @param from                The range partition 'from' clause.
     * @param to                  The range partition 'to' clause.
     * @param subPartitionType    The type of the sub partition, LIST, RANGE or HASH
     * @param partitionExpression The partition expression as passed directly into the sql
     * @return The newly created partition
     */
    static Partition createRangePartitionWithSubPartition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            String from,
            String to,
            PartitionType subPartitionType,
            String partitionExpression) {

        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createRangePartitionWithSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Preconditions.checkState(abstractLabel.isRangePartition());
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, from, to, subPartitionType, partitionExpression);
        partition.createRangePartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(
                    sqlgGraph,
                    abstractLabel.getSchema().getName(),
                    abstractLabel.getName(),
                    name,
                    from,
                    to,
                    subPartitionType,
                    partitionExpression);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, from, to, subPartitionType, partitionExpression);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a list partition that will itself be sub-partitioned.
     *
     * @param sqlgGraph           The graph
     * @param abstractLabel       The abstractLabel to partition
     * @param name                The name of the partition
     * @param in                  The list partition's 'in' clause
     * @param subPartitionType    The type of the sub partition, LIST, RANGE or HASH
     * @param partitionExpression The partition expression as passed directly into the sql
     * @return The newly created partition
     */
    static Partition createListPartitionWithSubPartition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            String in,
            PartitionType subPartitionType,
            String partitionExpression) {

        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createListSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Preconditions.checkState(abstractLabel.isListPartition());
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, in, subPartitionType, partitionExpression);
        partition.createListPartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(
                    sqlgGraph,
                    abstractLabel.getSchema().getName(),
                    abstractLabel.getName(),
                    name,
                    in,
                    subPartitionType,
                    partitionExpression);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, in, subPartitionType, partitionExpression);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a hash partition that will itself be sub-partitioned.
     *
     * @param sqlgGraph           The graph
     * @param abstractLabel       The abstractLabel to partition
     * @param name                The name of the partition
     * @param modulus             The hash partition's 'modulus' clause
     * @param remainder           The hash partition's 'remainder' clause
     * @param subPartitionType    The type of the sub partition, LIST, RANGE or HASH
     * @param partitionExpression The partition expression as passed directly into the sql
     * @return The newly created partition
     */
    static Partition createHashPartitionWithSubPartition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            Integer modulus,
            Integer remainder,
            PartitionType subPartitionType,
            String partitionExpression) {

        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createListSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Preconditions.checkState(abstractLabel.isHashPartition());
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, modulus, remainder, subPartitionType, partitionExpression);
        partition.createHashPartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(
                    sqlgGraph,
                    abstractLabel.getSchema().getName(),
                    abstractLabel.getName(),
                    name,
                    modulus,
                    remainder,
                    subPartitionType,
                    partitionExpression);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, modulus, remainder, subPartitionType, partitionExpression);
        }
        partition.committed = false;
        return partition;
    }

    /**
     * Create a range partition on an existing {@link Partition}
     */
    private static Partition createRangeSubPartition(SqlgGraph sqlgGraph, Partition parentPartition, String name, String from, String to) {
        Preconditions.checkArgument(!parentPartition.getAbstractLabel().getSchema().isSqlgSchema(), "createRangeSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, parentPartition, name, from, to, PartitionType.NONE, null);
        partition.createRangePartitionOnDb();
        TopologyManager.addSubPartition(sqlgGraph, partition);
        partition.committed = false;
        return partition;
    }

    /**
     * Create a range partition on an existing {@link Partition}
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
        TopologyManager.addSubPartition(sqlgGraph, partition);
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

        Preconditions.checkArgument(!parentPartition.getAbstractLabel().getSchema().isSqlgSchema(), "createListSubPartitionWithPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, parentPartition, name, in, partitionType, partitionExpression);
        partition.createListPartitionOnDb();
        TopologyManager.addSubPartition(sqlgGraph, partition);
        partition.committed = false;
        return partition;
    }

    /**
     * Create a list partition on an existing {@link Partition}
     */
    private static Partition createListSubPartition(SqlgGraph sqlgGraph, Partition parentPartition, String name, String in) {
        Preconditions.checkArgument(!parentPartition.getAbstractLabel().getSchema().isSqlgSchema(), "createPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, parentPartition, name, in, PartitionType.NONE, null);
        partition.createListPartitionOnDb();
        TopologyManager.addSubPartition(sqlgGraph, partition);
        partition.committed = false;
        return partition;
    }

    private static Partition createHashSubPartition(SqlgGraph sqlgGraph, Partition parentPartition, String name, Integer modulus, Integer remainder) {
        Preconditions.checkArgument(!parentPartition.getAbstractLabel().getSchema().isSqlgSchema(), "createPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, parentPartition, name, modulus, remainder, PartitionType.NONE, null);
        partition.createHashPartitionOnDb();
        TopologyManager.addSubPartition(sqlgGraph, partition);
        partition.committed = false;
        return partition;
    }

    private void createRangePartitionOnDb() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getAbstractLabel().getSchema().getName()));
        sql.append(".");
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
        //Only leaf partitions can have indexes on the foreign key.
        if (this.partitionType.isNone()) {
            AbstractLabel abstractLabel = getAbstractLabel();
            if (abstractLabel instanceof EdgeLabel) {
                sql.append(foreignKeyIndexSql());
            }
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
        if (this.partitionType.isNone()) {
            for (Index index : getAbstractLabel().getIndexes().values()) {
                //Append the partition name to the index name.
                String indexName = index.getName() + "_" + this.getName();
                if (indexName.length() > this.sqlgGraph.getSqlDialect().getMaximumIndexNameLength()) {
                    indexName = Index.generateName(this.sqlgGraph.getSqlDialect());
                }
                index.createIndex(this.sqlgGraph, SchemaTable.of(getAbstractLabel().getSchema().getName(), this.getName()), indexName);
            }
        }
    }

    private String foreignKeyIndexSql() {
        Preconditions.checkState(this.partitionType.isNone(), "Only leaf partitions can have indexes.");
        Preconditions.checkState(this.getAbstractLabel() instanceof EdgeLabel);
        SqlDialect sqlDialect = this.sqlgGraph.getSqlDialect();
        String schema = this.getAbstractLabel().getSchema().getName();
        String tableName = this.getName();
        EdgeLabel edgeLabel = (EdgeLabel) this.getAbstractLabel();
        StringBuilder sql = new StringBuilder();
        Set<EdgeRole> inEdgeRoles = edgeLabel.getInEdgeRoles();
        for (EdgeRole inEdgeRole : inEdgeRoles) {
            sql.append("\nCREATE INDEX");
            if (sqlDialect.requiresIndexName()) {
                sql.append(" ");
                sql.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(
                        SchemaTable.of(schema, tableName).withOutPrefix(),
                        EDGE_PREFIX,
                        "_idx",
                        Collections.singletonList(
                                inEdgeRole.getVertexLabel().getSchema().getName() + "_" + inEdgeRole.getVertexLabel().getLabel() + Topology.IN_VERTEX_COLUMN_END
                        ))));
            }
            sql.append(" ON ");
            sql.append(sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            if (inEdgeRole.getVertexLabel().hasIDPrimaryKey()) {
                sql.append(sqlDialect.maybeWrapInQoutes(inEdgeRole.getVertexLabel().getSchema().getName() + "." + inEdgeRole.getVertexLabel().getLabel() + Topology.IN_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : inEdgeRole.getVertexLabel().getIdentifiers()) {
                    sql.append(sqlDialect.maybeWrapInQoutes(inEdgeRole.getVertexLabel().getSchema().getName() + "." + inEdgeRole.getVertexLabel().getLabel() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                    if (i++ < inEdgeRole.getVertexLabel().getIdentifiers().size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(");");
        }
        Set<EdgeRole> outEdgeRoles = edgeLabel.getOutEdgeRoles();
        for (EdgeRole outEdgeRole : outEdgeRoles) {
            sql.append("\nCREATE INDEX");
            if (sqlDialect.requiresIndexName()) {
                sql.append(" ");
                sql.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(
                        SchemaTable.of(schema, tableName).withOutPrefix(),
                        EDGE_PREFIX,
                        "_idx",
                        Collections.singletonList(
                                outEdgeRole.getVertexLabel().getSchema().getName() + "_" + outEdgeRole.getVertexLabel().getLabel() + Topology.OUT_VERTEX_COLUMN_END
                        ))));
            }
            sql.append(" ON ");
            sql.append(sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            if (outEdgeRole.getVertexLabel().hasIDPrimaryKey()) {
                sql.append(sqlDialect.maybeWrapInQoutes(outEdgeRole.getVertexLabel().getSchema().getName() + "." + outEdgeRole.getVertexLabel().getLabel() + Topology.OUT_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : outEdgeRole.getVertexLabel().getIdentifiers()) {
                    sql.append(sqlDialect.maybeWrapInQoutes(outEdgeRole.getVertexLabel().getSchema().getName() + "." + outEdgeRole.getVertexLabel().getLabel() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                    if (i++ < outEdgeRole.getVertexLabel().getIdentifiers().size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(");");
        }
        return sql.toString();
    }

    private void createListPartitionOnDb() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getAbstractLabel().getSchema().getName()));
        sql.append(".");
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
        //Only leaf partitions can have indexes.
        if (this.partitionType.isNone()) {
            AbstractLabel abstractLabel = getAbstractLabel();
            if (abstractLabel instanceof EdgeLabel) {
                sql.append(foreignKeyIndexSql());
            }
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
        if (this.partitionType.isNone()) {
            for (Index index : getAbstractLabel().getIndexes().values()) {
                //Append the partition name to the index name.
                String indexName = index.getName() + "_" + this.getName();
                if (indexName.length() > this.sqlgGraph.getSqlDialect().getMaximumIndexNameLength()) {
                    indexName = Index.generateName(this.sqlgGraph.getSqlDialect());
                }
                index.createIndex(this.sqlgGraph, SchemaTable.of(getAbstractLabel().getSchema().getName(), this.getName()), indexName);
            }
        }
    }

    private void createHashPartitionOnDb() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getAbstractLabel().getSchema().getName()));
        sql.append(".");
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
        sql.append(" FOR VALUES WITH (MODULUS ");
        sql.append(this.modulus);
        sql.append(", REMAINDER ");
        sql.append(this.remainder);
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
        //Only leaf partitions can have indexes.
        if (this.partitionType.isNone()) {
            AbstractLabel abstractLabel = getAbstractLabel();
            if (abstractLabel instanceof EdgeLabel) {
                sql.append(foreignKeyIndexSql());
            }
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
        if (this.partitionType.isNone()) {
            for (Index index : getAbstractLabel().getIndexes().values()) {
                //Append the partition name to the index name.
                String indexName = index.getName() + "_" + this.getName();
                if (indexName.length() > this.sqlgGraph.getSqlDialect().getMaximumIndexNameLength()) {
                    indexName = Index.generateName(this.sqlgGraph.getSqlDialect());
                }
                index.createIndex(this.sqlgGraph, SchemaTable.of(getAbstractLabel().getSchema().getName(), this.getName()), indexName);
            }
        }
    }

    void delete() {
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getAbstractLabel().getSchema().getName()));
        sql.append(".");
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
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.getAbstractLabel().getSchema().getName()));
        sql.append(".");
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
        Preconditions.checkState(this.getAbstractLabel().getSchema().getTopology().isSchemaChanged(), "Partition.afterCommit must have schemaChanged = true");
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
    }

    void afterRollback() {
        Preconditions.checkState(this.getAbstractLabel().getSchema().getTopology().isSchemaChanged(), "Partition.afterRollback must have schemaChanged = true");
        this.uncommittedRemovedPartitions.clear();
        for (Map.Entry<String, Partition> entry : this.partitions.entrySet()) {
            entry.getValue().afterRollback();
        }
    }

    public Optional<ObjectNode> toUncommittedPartitionNotifyJson() {
        return toNotifyJson(false);
    }

    public Optional<ObjectNode> toCommittedPartitionNotifyJson() {
        return toNotifyJson(true);
    }

    private Optional<ObjectNode> toNotifyJson(boolean committed) {
        boolean foundSomething = false;
        ObjectNode partitionObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        partitionObjectNode.put("name", this.name);
        partitionObjectNode.put("from", this.from);
        partitionObjectNode.put("to", this.to);
        partitionObjectNode.put("in", this.in);
        partitionObjectNode.put("modulus", this.modulus);
        partitionObjectNode.put("remainder", this.remainder);
        partitionObjectNode.put("partitionType", this.partitionType.name());
        partitionObjectNode.put("partitionExpression", this.partitionExpression);
        ArrayNode uncommittedPartitions = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (Partition partition : this.uncommittedPartitions.values()) {
            foundSomething = true;
            Optional<ObjectNode> json = partition.toNotifyJson(false);
            json.ifPresent(uncommittedPartitions::add);
        }
        if (uncommittedPartitions.size() > 0) {
            partitionObjectNode.set("uncommittedPartitions", uncommittedPartitions);
        }
        ArrayNode committedPartitions = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (Partition partition : this.partitions.values()) {
            if (!this.uncommittedRemovedPartitions.contains(partition.getName())) {
                Optional<ObjectNode> json = partition.toNotifyJson(true);
                if (json.isPresent()) {
                    foundSomething = true;
                    committedPartitions.add(json.get());
                }
            }
        }
        if (committedPartitions.size() > 0) {
            partitionObjectNode.set("partitions", committedPartitions);
        }
        ArrayNode uncommittedRemovedPartitions = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (String removedPartition : this.uncommittedRemovedPartitions) {
            uncommittedRemovedPartitions.add(removedPartition);
        }
        if (uncommittedRemovedPartitions.size() > 0) {
            foundSomething = true;
            partitionObjectNode.set("uncommittedRemovedPartitions", uncommittedRemovedPartitions);
        }
        if (!committed || foundSomething) {
            return Optional.of(partitionObjectNode);
        } else {
            return Optional.empty();
        }
    }

    public Optional<ObjectNode> toJson() {
        ObjectNode partitionObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        partitionObjectNode.put("name", this.name);
        partitionObjectNode.put("from", this.from);
        partitionObjectNode.put("to", this.to);
        partitionObjectNode.put("in", this.in);
        partitionObjectNode.put("modulus", this.modulus);
        partitionObjectNode.put("remainder", this.remainder);
        partitionObjectNode.put("partitionType", this.partitionType.name());
        partitionObjectNode.put("partitionExpression", this.partitionExpression);
        ArrayNode uncommittedPartitions = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (Partition partition : this.uncommittedPartitions.values()) {
            Optional<ObjectNode> json = partition.toJson();
            json.ifPresent(uncommittedPartitions::add);
        }
        if (uncommittedPartitions.size() > 0) {
            partitionObjectNode.set("uncommittedPartitions", uncommittedPartitions);
        }
        ArrayNode committedPartitions = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (Partition partition : this.partitions.values()) {
            if (!this.uncommittedRemovedPartitions.contains(partition.getName())) {
                Optional<ObjectNode> json = partition.toJson();
                json.ifPresent(committedPartitions::add);
            }
        }
        if (committedPartitions.size() > 0) {
            partitionObjectNode.set("partitions", committedPartitions);
        }
        ArrayNode uncommittedRemovedPartitions = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (String removedPartition : this.uncommittedRemovedPartitions) {
            uncommittedRemovedPartitions.add(removedPartition);
        }
        if (uncommittedRemovedPartitions.size() > 0) {
            partitionObjectNode.set("uncommittedRemovedPartitions", uncommittedRemovedPartitions);
        }
        return Optional.of(partitionObjectNode);
    }

    static Partition fromUncommittedPartitionNotifyJson(AbstractLabel abstractLabel, JsonNode partitionNode) {
        Partition p;
        if (!partitionNode.get("from").asText().equals("null")) {
            Preconditions.checkState(!partitionNode.get("to").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("in").asText().equals("null"));
            p = new Partition(
                    abstractLabel.getSchema().getSqlgGraph(),
                    abstractLabel,
                    partitionNode.get("name").asText(),
                    partitionNode.get("from").asText(),
                    partitionNode.get("to").asText(),
                    PartitionType.from(partitionNode.get("partitionType").asText()),
                    partitionNode.get("partitionExpression").asText().equals("null") ? null : partitionNode.get("partitionExpression").asText()
            );
        } else if (!partitionNode.get("in").asText().equals("null")) {
            Preconditions.checkState(partitionNode.get("from").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("to").asText().equals("null"));
            Preconditions.checkState(!partitionNode.get("in").asText().equals("null"));
            p = new Partition(
                    abstractLabel.getSchema().getSqlgGraph(),
                    abstractLabel,
                    partitionNode.get("name").asText(),
                    partitionNode.get("in").asText(),
                    PartitionType.from(partitionNode.get("partitionType").asText()),
                    partitionNode.get("partitionExpression").asText().equals("null") ? null : partitionNode.get("partitionExpression").asText()
            );
        } else {
            Preconditions.checkState(!partitionNode.get("modulus").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("from").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("to").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("in").asText().equals("null"));
            p = new Partition(
                    abstractLabel.getSchema().getSqlgGraph(),
                    abstractLabel,
                    partitionNode.get("name").asText(),
                    partitionNode.get("modulus").asInt(),
                    partitionNode.get("remainder").asInt(),
                    PartitionType.from(partitionNode.get("partitionType").asText()),
                    partitionNode.get("partitionExpression").asText().equals("null") ? null : partitionNode.get("partitionExpression").asText()
            );

        }
        ArrayNode uncommittedPartitions = (ArrayNode) partitionNode.get("uncommittedPartitions");
        if (uncommittedPartitions != null) {
            for (JsonNode uncommittedPartition : uncommittedPartitions) {
                p.fromUncommittedPartitionNotifyJson(uncommittedPartition);
            }
        }
        ArrayNode removedPartitions = (ArrayNode) partitionNode.get("removedPartitions");
        if (removedPartitions != null) {
            p.fromNotifyJsonRemove(removedPartitions);
        }
        return p;
    }

    private void fromUncommittedPartitionNotifyJson(JsonNode partitionNode) {
        Partition p;
        if (!partitionNode.get("from").asText().equals("null")) {
            Preconditions.checkState(!partitionNode.get("to").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("in").asText().equals("null"));
            p = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionNode.get("name").asText(),
                    partitionNode.get("from").asText(),
                    partitionNode.get("to").asText(),
                    PartitionType.from(partitionNode.get("partitionType").asText()),
                    partitionNode.get("partitionExpression").asText().equals("null") ? null : partitionNode.get("partitionExpression").asText()
            );
        } else if (!partitionNode.get("in").asText().equals("null")) {
            Preconditions.checkState(partitionNode.get("from").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("to").asText().equals("null"));
            p = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionNode.get("name").asText(),
                    partitionNode.get("in").asText(),
                    PartitionType.from(partitionNode.get("partitionType").asText()),
                    partitionNode.get("partitionExpression").asText().equals("null") ? null : partitionNode.get("partitionExpression").asText()
            );
        } else {
            Preconditions.checkState(partitionNode.get("from").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("to").asText().equals("null"));
            Preconditions.checkState(partitionNode.get("in").asText().equals("null"));
            p = new Partition(
                    this.sqlgGraph,
                    this,
                    partitionNode.get("name").asText(),
                    partitionNode.get("modulus").asInt(),
                    partitionNode.get("remainder").asInt(),
                    PartitionType.from(partitionNode.get("partitionType").asText()),
                    partitionNode.get("partitionExpression").asText().equals("null") ? null : partitionNode.get("partitionExpression").asText()
            );
        }
        this.partitions.put(p.getName(), p);

        ArrayNode uncommittedPartitions = (ArrayNode) partitionNode.get("uncommittedPartitions");
        if (uncommittedPartitions != null) {
            for (JsonNode uncommittedPartition : uncommittedPartitions) {
                p.fromUncommittedPartitionNotifyJson(uncommittedPartition);
            }
        }

        ArrayNode removedPartitions = (ArrayNode) partitionNode.get("removedPartitions");
        if (removedPartitions != null) {
            p.fromNotifyJsonRemove(removedPartitions);
        }
    }

    private void fromNotifyJsonRemove(ArrayNode partitionNode) {
        for (JsonNode jsonNode : partitionNode) {
            this.partitions.remove(jsonNode.toString());
        }
    }

    void fromNotifyJson(JsonNode partitionNode, boolean fire) {
        ArrayNode partitionsNode = (ArrayNode) partitionNode.get("partitions");
        if (partitionsNode != null) {
            for (JsonNode jsonNode : partitionsNode) {
                Optional<Partition> optionalPartition = getPartition(jsonNode.get("name").asText());
                Preconditions.checkState(optionalPartition.isPresent(), "committed partition %s on partition %s must be present",
                        jsonNode.get("name").asText(),
                        this.getName());
                Partition committedPartition = optionalPartition.get();
                committedPartition.fromNotifyJson(jsonNode, fire);
            }
        }
        ArrayNode uncommittedPartitionsNode = (ArrayNode) partitionNode.get("uncommittedPartitions");
        if (uncommittedPartitionsNode != null) {
            for (JsonNode jsonNode : uncommittedPartitionsNode) {
                this.fromUncommittedPartitionNotifyJson(jsonNode);
            }
        }
        ArrayNode uncommittedRemovedPartitions = (ArrayNode) partitionNode.get("uncommittedRemovedPartitions");
        if (uncommittedRemovedPartitions != null) {
            for (JsonNode jsonNode : uncommittedRemovedPartitions) {
                String pName = jsonNode.asText();
                Partition old = this.partitions.remove(pName);
                if (fire && old != null) {
                    this.getAbstractLabel().getSchema().getTopology().fire(old, old, TopologyChangeAction.DELETE);
                }
            }
        }

    }

    @Override
    public String toString() {
        return this.name;
    }

    public void ensureRangePartitionExists(String name, String from, String to) {
        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Objects.requireNonNull(from, "Sub-partition's \"from\" must not be null");
        Objects.requireNonNull(to, "Sub-partition's \"to\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.RANGE, "ensureRangePartitionExists(String name, String from, String to) can only be called for a RANGE partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            partitionOptional.orElseGet(() -> this.createRangePartition(name, from, to));
        }
    }

    public void ensureListPartitionExists(String name, String in) {
        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Objects.requireNonNull(in, "Sub-partition's \"in\" must not be null");
        Preconditions.checkState(this.partitionType == PartitionType.LIST, "ensureListPartitionExists(String name, String in) can only be called for a LIST partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            partitionOptional.orElseGet(() -> this.createListPartition(name, in));
        }
    }

    public void ensureHashPartitionExists(String name, Integer modulus, Integer remainder) {
        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Preconditions.checkState(modulus > 0, "hash partition's modulus must be > 0.");
        Preconditions.checkState(remainder >= 0, "hash partition's modulus must be > 0.");
        Preconditions.checkState(this.partitionType == PartitionType.HASH, "ensureHashPartitionExists(String name, String in) can only be called for a HASH partitioned VertexLabel. Found %s", this.partitionType.name());
        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            partitionOptional.orElseGet(() -> this.createHashPartition(name, modulus, remainder));
        }
    }

    public Partition ensureRangePartitionWithSubPartitionExists(
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Objects.requireNonNull(from, "Sub-partition's \"from\" must not be null");
        Objects.requireNonNull(to, "Sub-partition's \"to\" must not be null");
        Preconditions.checkState(partitionType != PartitionType.NONE, "ensureRangePartitionWithSubPartitionExists(String name, String from, String to, PartitionType partitionType, String partitionExpression) may not be called with partitionType NONE partition");
        Objects.requireNonNull(partitionExpression, "Sub-partition's \"partitionExpression\" must not be null");

        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createRangePartitionWithSubPartition(name, from, to, partitionType, partitionExpression));
        } else {
            return partitionOptional.get();
        }
    }

    public Partition ensureListPartitionWithSubPartitionExists(
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        Objects.requireNonNull(name, "Sub-partition's \"name\" must not be null");
        Objects.requireNonNull(in, "Sub-partition's \"in\" must not be null");
        Preconditions.checkState(partitionType == PartitionType.LIST, "ensureListPartitionWithSubPartitionExists(String name, String in, PartitionType partitionType, String partitionExpression) can only be called with a LIST partition. Found %s", partitionType.name());
        Objects.requireNonNull(partitionExpression, "Sub-partition's \"partitionExpression\" must not be null");

        Optional<Partition> partitionOptional = this.getPartition(name);
        if (partitionOptional.isEmpty()) {
            this.getAbstractLabel().getSchema().getTopology().lock();
            partitionOptional = this.getPartition(name);
            return partitionOptional.orElseGet(() -> this.createListPartitionWithSubPartition(name, in, partitionType, partitionExpression));
        } else {
            return partitionOptional.get();
        }
    }

    private Partition createRangePartition(String name, String from, String to) {
        Preconditions.checkState(!this.getAbstractLabel().getSchema().isSqlgSchema(), "createSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createRangeSubPartition(this.sqlgGraph, this, name, from, to);
        this.uncommittedPartitions.put(name, partition);
        this.getAbstractLabel().getSchema().getTopology().fire(partition, null, TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createListPartition(String name, String in) {
        Preconditions.checkState(!this.getAbstractLabel().getSchema().isSqlgSchema(), "createSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createListSubPartition(this.sqlgGraph, this, name, in);
        this.uncommittedPartitions.put(name, partition);
        this.getAbstractLabel().getSchema().getTopology().fire(partition, null, TopologyChangeAction.CREATE);
        return partition;
    }

    private Partition createHashPartition(String name, Integer modulus, Integer remainder) {
        Preconditions.checkState(!this.getAbstractLabel().getSchema().isSqlgSchema(), "createSubPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        this.uncommittedPartitions.remove(name);
        Partition partition = Partition.createHashSubPartition(this.sqlgGraph, this, name, modulus, remainder);
        this.uncommittedPartitions.put(name, partition);
        this.getAbstractLabel().getSchema().getTopology().fire(partition, null, TopologyChangeAction.CREATE);
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
        this.getAbstractLabel().getSchema().getTopology().fire(partition, null, TopologyChangeAction.CREATE);
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
        this.getAbstractLabel().getSchema().getTopology().fire(partition, null, TopologyChangeAction.CREATE);
        return partition;
    }

    public Map<String, Partition> getPartitions() {
        Map<String, Partition> result = new HashMap<>(this.partitions);
        if (this.getAbstractLabel().getSchema().getTopology().isSchemaChanged()) {
            result.putAll(this.uncommittedPartitions);
            for (String s : this.uncommittedRemovedPartitions) {
                result.remove(s);
            }
        }
        return result;
    }

    public Optional<Partition> getPartition(String name) {
        if (getAbstractLabel().getSchema().getTopology().isSchemaChanged() && this.uncommittedRemovedPartitions.contains(name)) {
            return Optional.empty();
        }
        Partition result = null;
        if (this.getAbstractLabel().getSchema().getTopology().isSchemaChanged()) {
            result = this.uncommittedPartitions.get(name);
        }
        if (result == null) {
            result = this.partitions.get(name);
        }
        if (result == null) {
            for (Partition partition : this.uncommittedPartitions.values()) {
                Optional<Partition> p = partition.getPartition(name);
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

    Partition addPartition(Vertex partitionVertex) {
        Preconditions.checkState(this.getAbstractLabel().getSchema().getTopology().isSchemaChanged());
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
            if (partitionExpression.isPresent()) {
                PartitionType partitionType1 = PartitionType.from(partitionType.value());
                Preconditions.checkState(!partitionType1.isNone());
                partition = new Partition(
                        this.sqlgGraph,
                        this,
                        partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                        from.value(),
                        to.value(),
                        partitionType1,
                        partitionExpression.value());
            } else {
                PartitionType partitionType1 = PartitionType.from(partitionType.value());
                Preconditions.checkState(partitionType1.isNone());
                partition = new Partition(
                        this.sqlgGraph,
                        this,
                        partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                        from.value(),
                        to.value(),
                        partitionType1,
                        null);

            }
        } else if (in.isPresent()) {
            Preconditions.checkState(!to.isPresent());
            Preconditions.checkState(!from.isPresent());
            if (partitionExpression.isPresent()) {
                PartitionType partitionType1 = PartitionType.from(partitionType.value());
                Preconditions.checkState(!partitionType1.isNone());
                partition = new Partition(
                        this.sqlgGraph,
                        this,
                        partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                        in.value(),
                        partitionType1,
                        partitionExpression.value());
            } else {
                PartitionType partitionType1 = PartitionType.from(partitionType.value());
                Preconditions.checkState(partitionType1.isNone());
                partition = new Partition(
                        this.sqlgGraph,
                        this,
                        partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                        in.value(),
                        partitionType1,
                        null);
            }
        } else {
            Preconditions.checkState(modulus.isPresent());
            Preconditions.checkState(remainder.isPresent());
            if (partitionExpression.isPresent()) {
                PartitionType partitionType1 = PartitionType.from(partitionType.value());
                Preconditions.checkState(!partitionType1.isNone());
                partition = new Partition(
                        this.sqlgGraph,
                        this,
                        partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                        modulus.value(),
                        remainder.value(),
                        partitionType1,
                        partitionExpression.value());
            } else {
                PartitionType partitionType1 = PartitionType.from(partitionType.value());
                Preconditions.checkState(partitionType1.isNone());
                partition = new Partition(
                        this.sqlgGraph,
                        this,
                        partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME),
                        modulus.value(),
                        remainder.value(),
                        partitionType1,
                        null);
            }
        }
        this.partitions.put(partitionVertex.value(SQLG_SCHEMA_PARTITION_NAME), partition);
        return partition;
    }

    public void createIndexOnLeafPartitions(Index index) {
        if (this.partitionType.isNone()) {
            //Append the partition name to the index name.
            String indexName = index.getName() + "_" + this.getName();
            if (indexName.length() > this.sqlgGraph.getSqlDialect().getMaximumIndexNameLength()) {
                indexName = Index.generateName(this.sqlgGraph.getSqlDialect());
            }
            index.createIndex(this.sqlgGraph, SchemaTable.of(getAbstractLabel().getSchema().getName(), this.getName()), indexName);
        } else {
            for (Partition partition : this.partitions.values()) {
                partition.createIndexOnLeafPartitions(index);
            }
        }
    }

}
