package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyInf;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

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
    private AbstractLabel abstractLabel;
    protected boolean committed = true;

    Partition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, String from, String to) {
        this.sqlgGraph = sqlgGraph;
        this.abstractLabel = abstractLabel;
        this.name = name;
        this.from = from;
        this.to = to;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
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
        return abstractLabel;
    }

    @Override
    public void remove(boolean preserveData) {
        this.abstractLabel.removePartition(this, preserveData);
    }

    public static Partition createPartition(SqlgGraph sqlgGraph, AbstractLabel abstractLabel, String name, String from, String to) {
        Preconditions.checkArgument(!abstractLabel.getSchema().isSqlgSchema(), "createPartition may not be called for \"%s\"", Topology.SQLG_SCHEMA);
        Partition partition = new Partition(sqlgGraph, abstractLabel, name, from, to);
        partition.createPartitionOnDb();
        if (abstractLabel instanceof VertexLabel) {
            TopologyManager.addVertexLabelPartition(sqlgGraph, abstractLabel, name, from, to);
        } else {
            TopologyManager.addEdgeLabelPartition(sqlgGraph, abstractLabel, name, from, to);
        }
        partition.committed = false;
        return partition;
    }

    private void createPartitionOnDb() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        sql.append(" PARTITION OF ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.abstractLabel.getSchema().getName()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.abstractLabel.getPrefix() + this.abstractLabel.getLabel()));
        sql.append(" FOR VALUES FROM (");
        sql.append(this.from);
        sql.append(") TO (");
        sql.append(this.to);
        sql.append(")");
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
}
