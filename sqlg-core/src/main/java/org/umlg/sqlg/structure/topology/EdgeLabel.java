package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.TopologyChangeAction;

import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class EdgeLabel extends AbstractLabel {

    private static Logger logger = LoggerFactory.getLogger(EdgeLabel.class);
    //This just won't stick in my brain.
    //hand (out) ----<label>---- finger (in)
    Set<VertexLabel> outVertexLabels = new HashSet<>();
    Set<VertexLabel> inVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedOutVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedInVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedRemovedInVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedRemovedOutVertexLabels = new HashSet<>();

    private Topology topology;

    static EdgeLabel loadSqlgSchemaEdgeLabel(
            String edgeLabelName,
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyType> properties) {

        //edges are created in the out vertex's schema.
        return new EdgeLabel(true, edgeLabelName, outVertexLabel, inVertexLabel, properties, new ListOrderedSet<>());
    }

    static EdgeLabel createEdgeLabel(
            String edgeLabelName,
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyType> properties) {

        return createEdgeLabel(edgeLabelName, outVertexLabel, inVertexLabel, properties, new ListOrderedSet<>());
    }

    static EdgeLabel createEdgeLabel(
            String edgeLabelName,
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyType> properties,
            ListOrderedSet<String> identifiers) {

        Preconditions.checkState(!inVertexLabel.getSchema().isSqlgSchema(), "You may not create an edge to %s", Topology.SQLG_SCHEMA);
        //edges are created in the out vertex's schema.
        EdgeLabel edgeLabel = new EdgeLabel(false, edgeLabelName, outVertexLabel, inVertexLabel, properties, identifiers);
        edgeLabel.createEdgeTableOnDb(outVertexLabel, inVertexLabel, properties, identifiers);
        edgeLabel.committed = false;
        return edgeLabel;
    }

    static EdgeLabel createPartitionedEdgeLabel(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            final Map<String, PropertyType> properties,
            final ListOrderedSet<String> identifiers,
            final PartitionType partitionType,
            final String partitionExpression) {

        Preconditions.checkState(!inVertexLabel.getSchema().isSqlgSchema(), "You may not create an edge to %s", Topology.SQLG_SCHEMA);
        //edges are created in the out vertex's schema.
        EdgeLabel edgeLabel = new EdgeLabel(
                false,
                edgeLabelName,
                outVertexLabel,
                inVertexLabel,
                properties,
                identifiers,
                partitionType,
                partitionExpression);
        edgeLabel.createEdgeTableOnDb(outVertexLabel, inVertexLabel, properties, identifiers);
        edgeLabel.committed = false;
        return edgeLabel;
    }

    static EdgeLabel loadFromDb(Topology topology, String edgeLabelName) {
        return new EdgeLabel(topology, edgeLabelName);
    }

    static EdgeLabel loadFromDb(Topology topology, String edgeLabelName, PartitionType partitionType, String partitionExpression) {
        return new EdgeLabel(topology, edgeLabelName, partitionType, partitionExpression);
    }

    private EdgeLabel(
            final boolean forSqlgSchema,
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            final Map<String, PropertyType> properties,
            final ListOrderedSet<String> identifiers,
            final PartitionType partitionType,
            final String partitionExpression) {

        super(outVertexLabel.getSchema().getSqlgGraph(), edgeLabelName, properties, identifiers, partitionType, partitionExpression);
        if (forSqlgSchema) {
            this.outVertexLabels.add(outVertexLabel);
            this.inVertexLabels.add(inVertexLabel);
        } else {
            this.uncommittedOutVertexLabels.add(outVertexLabel);
            this.uncommittedInVertexLabels.add(inVertexLabel);
        }
        // this is a topology edge label, the columns exist
        if (forSqlgSchema) {
            for (PropertyColumn pc : this.uncommittedProperties.values()) {
                pc.setCommitted(true);
                this.properties.put(pc.getName(), pc);
            }
            this.uncommittedProperties.clear();
        }
        this.topology = outVertexLabel.getSchema().getTopology();
    }

    private EdgeLabel(
            boolean forSqlgSchema,
            String edgeLabelName,
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyType> properties,
            ListOrderedSet<String> identifiers) {

        super(outVertexLabel.getSchema().getSqlgGraph(), edgeLabelName, properties, identifiers);
        if (forSqlgSchema) {
            this.outVertexLabels.add(outVertexLabel);
            this.inVertexLabels.add(inVertexLabel);
        } else {
            this.uncommittedOutVertexLabels.add(outVertexLabel);
            this.uncommittedInVertexLabels.add(inVertexLabel);
        }
        // this is a topology edge label, the columns exist
        if (forSqlgSchema) {
            for (PropertyColumn pc : this.uncommittedProperties.values()) {
                pc.setCommitted(true);
                this.properties.put(pc.getName(), pc);
            }
            this.uncommittedProperties.clear();
        }
        this.topology = outVertexLabel.getSchema().getTopology();
    }

    EdgeLabel(Topology topology, String edgeLabelName) {
        super(topology.getSqlgGraph(), edgeLabelName, Collections.emptyMap(), new ListOrderedSet<>());
        this.topology = topology;
    }

    EdgeLabel(Topology topology, String edgeLabelName, PartitionType partitionType, String partitionExpression) {
        super(topology.getSqlgGraph(), edgeLabelName, partitionType, partitionExpression);
        this.topology = topology;
    }

    @Override
    public Schema getSchema() {
        if (!this.outVertexLabels.isEmpty()) {
            VertexLabel vertexLabel = this.outVertexLabels.iterator().next();
            return vertexLabel.getSchema();
        } else if (this.topology.isSqlWriteLockHeldByCurrentThread() && !this.uncommittedOutVertexLabels.isEmpty()) {
            VertexLabel vertexLabel = this.uncommittedOutVertexLabels.iterator().next();
            return vertexLabel.getSchema();
        } else {
            throw new IllegalStateException("BUG: no outVertexLabels present when getSchema() is called");
        }
    }

    @Override
    public void ensurePropertiesExist(Map<String, PropertyType> columns) {
        for (Map.Entry<String, PropertyType> column : columns.entrySet()) {
            if (!this.properties.containsKey(column.getKey())) {
                Preconditions.checkState(!this.getSchema().isSqlgSchema(), "schema may not be %s", SQLG_SCHEMA);
                if (!this.uncommittedProperties.containsKey(column.getKey())) {
                    this.getSchema().getTopology().lock();
                    if (!getProperty(column.getKey()).isPresent()) {
                        TopologyManager.addEdgeColumn(this.sqlgGraph, this.getSchema().getName(), EDGE_PREFIX + getLabel(), column, new ListOrderedSet<>());
                        addColumn(this.getSchema().getName(), EDGE_PREFIX + getLabel(), ImmutablePair.of(column.getKey(), column.getValue()));
                        PropertyColumn propertyColumn = new PropertyColumn(this, column.getKey(), column.getValue());
                        propertyColumn.setCommitted(false);
                        this.uncommittedProperties.put(column.getKey(), propertyColumn);
                        this.getSchema().getTopology().fire(propertyColumn, "", TopologyChangeAction.CREATE);
                    }
                }
            }
        }
    }

    private void createEdgeTableOnDb(
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers) {

        if (!this.partitionType.isNone() && identifiers.isEmpty()) {
            throw new IllegalStateException("Partitioned table must have identifiers.");
        }

        String schema = outVertexLabel.getSchema().getName();
        String tableName = EDGE_PREFIX + getLabel();

        SqlDialect sqlDialect = this.sqlgGraph.getSqlDialect();
        sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(sqlDialect.createTableStatement());
        sql.append(sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(tableName));
        if (this.partitionType.isNone() && identifiers.isEmpty()) {
            sql.append("(\n\t");
            sql.append(sqlDialect.maybeWrapInQoutes("ID"));
            sql.append(" ");
            if (this.partitionType.isNone()) {
                sql.append(sqlDialect.getAutoIncrementPrimaryKeyConstruct());
            } else {
                sql.append(sqlDialect.getAutoIncrement());
            }
            if (columns.size() > 0) {
                sql.append(", ");
            }
        } else {
            sql.append("(\n\t");
        }
        buildColumns(this.sqlgGraph, identifiers, columns, sql);
        if (this.partitionType.isNone() && !identifiers.isEmpty()) {
            sql.append(",\n\tPRIMARY KEY(");
            int count = 1;
            for (String identifier : identifiers) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                if (count++ < identifiers.size()) {
                    sql.append(", ");
                }
            }
            sql.append(")");
        }

        sql.append(",");
        if (inVertexLabel.hasIDPrimaryKey()) {
            sql.append("\n\t");
            sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
            sql.append(" ");
            sql.append(sqlDialect.getForeignKeyTypeDefinition());
        } else {
            sql.append("\n\t");
            int i = 1;
            for (String identifier : inVertexLabel.getIdentifiers()) {
                PropertyColumn propertyColumn = inVertexLabel.getProperty(identifier).orElseThrow(
                        () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                );
                if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                    i++;
                } else {
                    PropertyType propertyType = propertyColumn.getPropertyType();
                    String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    for (String sqlDefinition : propertyTypeToSqlDefinition) {
                        if (count > 1) {
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + Topology.IN_VERTEX_COLUMN_END)).append(" ").append(sqlDefinition);
                        } else {
                            //The first column existVertexLabel no postfix
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END)).append(" ").append(sqlDefinition);
                        }
                        if (count++ < propertyTypeToSqlDefinition.length) {
                            sql.append(", ");
                        }
                    }
                    if (outVertexLabel.isDistributed()) {
                        if (i++ < inVertexLabel.getIdentifiers().size() - 1) {
                            sql.append(", ");
                        }
                    } else {
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
        }
        sql.append(",");
        if (outVertexLabel.hasIDPrimaryKey()) {
            sql.append("\n\t");
            sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
            sql.append(" ");
            sql.append(sqlDialect.getForeignKeyTypeDefinition());
        } else {
            int i = 1;
            sql.append("\n\t");

            for (String identifier : outVertexLabel.getIdentifiers()) {
                PropertyColumn propertyColumn = outVertexLabel.getProperty(identifier).orElseThrow(
                        () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                );
                if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                    i++;
                } else {
                    PropertyType propertyType = propertyColumn.getPropertyType();
                    String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    for (String sqlDefinition : propertyTypeToSqlDefinition) {
                        if (count > 1) {
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + Topology.OUT_VERTEX_COLUMN_END)).append(" ").append(sqlDefinition);
                        } else {
                            //The first column existVertexLabel no postfix
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END)).append(" ").append(sqlDefinition);
                        }
                        if (count++ < propertyTypeToSqlDefinition.length) {
                            sql.append(", ");
                        }
                    }
                    if (outVertexLabel.isDistributed()) {
                        if (i++ < outVertexLabel.getIdentifiers().size() - 1) {
                            sql.append(", ");
                        }
                    } else {
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }

        }

        //foreign key definition start
        if (partitionType.isNone() && this.sqlgGraph.getTopology().isImplementingForeignKeys()) {
            sql.append(",\n\t");
            sql.append("FOREIGN KEY (");
            if (inVertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    } else {
                        sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
            sql.append(") REFERENCES ");
            sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName()));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + inVertexLabel.getLabel()));
            sql.append(" (");
            if (inVertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlDialect.maybeWrapInQoutes("ID"));
            } else {
                int i = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    } else {
                        sql.append(sqlDialect.maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
            sql.append(") ");
            if (sqlDialect.supportsDeferrableForeignKey()) {
                sql.append("DEFERRABLE");
            }
            sql.append(",\n\tFOREIGN KEY (");
            if (outVertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    } else {
                        sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
            sql.append(") REFERENCES ");
            sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName()));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + outVertexLabel.getLabel()));
            sql.append(" (");
            if (outVertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlDialect.maybeWrapInQoutes("ID"));
            } else {
                int i = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    } else {
                        sql.append(sqlDialect.maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
            sql.append(") ");
            if (sqlDialect.supportsDeferrableForeignKey()) {
                sql.append("DEFERRABLE");
            }
            if (sqlDialect.needForeignKeyIndex() && sqlDialect.isIndexPartOfCreateTable()) {
                //This is true for Cockroachdb
                sql.append(", INDEX (");
                sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName() + "." + inVertexLabel.getLabel() + Topology.IN_VERTEX_COLUMN_END));
                sql.append("), INDEX (");
                sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName() + "." + outVertexLabel.getLabel() + Topology.OUT_VERTEX_COLUMN_END));
                sql.append(")");
            }
        }
        //foreign key definition end

        sql.append("\n)");
        if (!partitionType.isNone()) {
            sql.append(" PARTITION BY ");
            sql.append(this.partitionType.name());
            sql.append(" (");
            sql.append(this.partitionExpression);
            sql.append(")");
        }

        if (sqlDialect.needsSemicolon()) {
            sql.append(";");
        }

        if (partitionType.isNone() && sqlDialect.needForeignKeyIndex() && !sqlDialect.isIndexPartOfCreateTable()) {
            sql.append("\n\tCREATE INDEX");
            if (sqlDialect.requiresIndexName()) {
                sql.append(" ");
                sql.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(
                        SchemaTable.of(schema, tableName).withOutPrefix(),
                        EDGE_PREFIX,
                        "_idx",
                        Collections.singletonList(
                                inVertexLabel.getSchema().getName() + "_" + inVertexLabel.getLabel() + Topology.IN_VERTEX_COLUMN_END
                        ))));
            }
            sql.append(" ON ");
            sql.append(sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            if (inVertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    } else {
                        sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
            sql.append(");");

            sql.append("\n\tCREATE INDEX");
            if (sqlDialect.requiresIndexName()) {
                sql.append(" ");
                sql.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(
                        SchemaTable.of(schema, tableName).withOutPrefix(),
                        EDGE_PREFIX,
                        "_idx",
                        Collections.singletonList(
                                outVertexLabel.getSchema().getName() + "_" + outVertexLabel.getLabel() + Topology.OUT_VERTEX_COLUMN_END
                        ))));
            }
            sql.append(" ON ");
            sql.append(sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            if (outVertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    } else {
                        sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
            sql.append(");");
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
        Preconditions.checkState(this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread(), "EdgeLabel.afterCommit must hold the write lock");
        super.afterCommit();
        for (Iterator<VertexLabel> it = this.uncommittedInVertexLabels.iterator(); it.hasNext(); ) {
            VertexLabel vertexLabel = it.next();
            this.inVertexLabels.add(vertexLabel);
            it.remove();
        }
        for (Iterator<VertexLabel> it = this.uncommittedRemovedInVertexLabels.iterator(); it.hasNext(); ) {
            VertexLabel vertexLabel = it.next();
            this.inVertexLabels.remove(vertexLabel);
            it.remove();
        }
        for (Iterator<VertexLabel> it = this.uncommittedOutVertexLabels.iterator(); it.hasNext(); ) {
            VertexLabel vertexLabel = it.next();
            this.outVertexLabels.add(vertexLabel);
            it.remove();
        }
        for (Iterator<VertexLabel> it = this.uncommittedRemovedOutVertexLabels.iterator(); it.hasNext(); ) {
            VertexLabel vertexLabel = it.next();
            this.outVertexLabels.remove(vertexLabel);
            it.remove();
        }
    }

    void afterRollbackInEdges(VertexLabel vertexLabel) {
        Preconditions.checkState(this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread(), "EdgeLabel.afterRollback must hold the write lock");
        super.afterRollback();
        this.uncommittedInVertexLabels.remove(vertexLabel);
    }

    void afterRollbackOutEdges(VertexLabel vertexLabel) {
        Preconditions.checkState(this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread(), "EdgeLabel.afterRollback must hold the write lock");
        super.afterRollback();
        this.uncommittedOutVertexLabels.remove(vertexLabel);
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    private boolean foreignKeysContains(Direction direction, VertexLabel vertexLabel) {
        switch (direction) {
            case OUT:
                if (this.outVertexLabels.contains(vertexLabel)) {
                    return true;
                }
                break;
            case IN:
                if (this.inVertexLabels.contains(vertexLabel)) {
                    return true;
                }
                break;
            case BOTH:
                throw new IllegalStateException("foreignKeysContains may not be called for Direction.BOTH");
        }
        if (this.topology.isSqlWriteLockHeldByCurrentThread()) {
            switch (direction) {
                case OUT:
                    if (this.uncommittedOutVertexLabels.contains(vertexLabel)) {
                        return true;
                    }
                    break;
                case IN:
                    if (this.uncommittedInVertexLabels.contains(vertexLabel)) {
                        return true;
                    }
                    break;
                case BOTH:
                    throw new IllegalStateException("foreignKeysContains may not be called for Direction.BOTH");
            }

        }
        return false;
    }

    Set<ForeignKey> getAllEdgeForeignKeys() {
        Set<ForeignKey> result = new HashSet<>();
        for (VertexLabel vertexLabel : this.getInVertexLabels()) {
            if (!this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() || !this.uncommittedRemovedInVertexLabels.contains(vertexLabel)) {
                if (vertexLabel.hasIDPrimaryKey()) {
                    result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
                } else {
                    ForeignKey foreignKey = new ForeignKey();
                    for (String identifier : vertexLabel.getIdentifiers()) {
                        if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                            foreignKey.add(vertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END);
                        }
                    }
                    result.add(foreignKey);
                }
            }
        }
        for (VertexLabel vertexLabel : this.getOutVertexLabels()) {
            if (!this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() || !this.uncommittedRemovedOutVertexLabels.contains(vertexLabel)) {
                if (vertexLabel.hasIDPrimaryKey()) {
                    result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
                } else {
                    ForeignKey foreignKey = new ForeignKey();
                    for (String identifier : vertexLabel.getIdentifiers()) {
                        if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                            foreignKey.add(vertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END);
                        }

                    }
                    result.add(foreignKey);
                }
            }
        }
        return result;
    }

    Set<ForeignKey> getUncommittedEdgeForeignKeys() {
        Set<ForeignKey> result = new HashSet<>();
        //noinspection Duplicates
        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            for (VertexLabel vertexLabel : this.uncommittedInVertexLabels) {
                if (!this.uncommittedRemovedInVertexLabels.contains(vertexLabel)) {
                    if (vertexLabel.hasIDPrimaryKey()) {
                        result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
                    } else {
                        ForeignKey foreignKey = new ForeignKey();
                        for (String identifier : vertexLabel.getIdentifiers()) {
                            if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                                foreignKey.add(vertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END);
                            }
                        }
                        result.add(foreignKey);
                    }
                }
            }
            for (VertexLabel vertexLabel : this.uncommittedOutVertexLabels) {
                if (!this.uncommittedRemovedOutVertexLabels.contains(vertexLabel)) {
                    if (vertexLabel.hasIDPrimaryKey()) {
                        result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
                    } else {
                        ForeignKey foreignKey = new ForeignKey();
                        for (String identifier : vertexLabel.getIdentifiers()) {
                            if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                                foreignKey.add(vertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END);
                            }
                        }
                        result.add(foreignKey);
                    }
                }
            }
        }
        return result;
    }

    boolean isValid() {
        return this.outVertexLabels.size() > 0 || this.uncommittedOutVertexLabels.size() > 0;
    }

    public Set<VertexLabel> getOutVertexLabels() {
        Set<VertexLabel> result = new HashSet<>(this.outVertexLabels);
        if (isValid() && this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedOutVertexLabels);
            result.removeAll(this.uncommittedRemovedOutVertexLabels);
        }
        return Collections.unmodifiableSet(result);
    }

    public Set<VertexLabel> getInVertexLabels() {
        Set<VertexLabel> result = new HashSet<>(this.inVertexLabels);
        if (isValid() && this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedInVertexLabels);
            result.removeAll(this.uncommittedRemovedInVertexLabels);
        }
        return Collections.unmodifiableSet(result);
    }

    public Set<EdgeRole> getOutEdgeRoles() {
        Set<EdgeRole> result = new HashSet<>();
        for (VertexLabel lbl : this.outVertexLabels) {
//            if (!this.uncommittedOutVertexLabels.contains(lbl)) {
            result.add(new EdgeRole(lbl, this, Direction.OUT, true));
//            }
        }

        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            for (VertexLabel lbl : this.uncommittedOutVertexLabels) {
//                if (!this.uncommittedOutVertexLabels.contains(lbl)) {
                result.add(new EdgeRole(lbl, this, Direction.OUT, false));
//                }
            }
        }
        return Collections.unmodifiableSet(result);
    }

    public Set<EdgeRole> getInEdgeRoles() {
        Set<EdgeRole> result = new HashSet<>();
        for (VertexLabel lbl : this.inVertexLabels) {
//            if (!this.uncommittedInVertexLabels.contains(lbl)) {
            result.add(new EdgeRole(lbl, this, Direction.IN, true));
//            }
        }

        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            for (VertexLabel lbl : this.uncommittedInVertexLabels) {
//                if (!this.uncommittedInVertexLabels.contains(lbl)) {
                result.add(new EdgeRole(lbl, this, Direction.IN, false));
//                }
            }
        }
        return Collections.unmodifiableSet(result);
    }

    public void ensureEdgeVertexLabelExist(Direction direction, VertexLabel vertexLabel) {
        //if the direction is OUT then the vertexLabel must be in the same schema as the edgeLabel (this)
        if (direction == Direction.OUT) {
            Preconditions.checkState(vertexLabel.getSchema().equals(getSchema()), "For Direction.OUT the VertexLabel must be in the same schema as the edge. Found %s and %s", vertexLabel.getSchema().getName(), getSchema().getName());
        }
        SchemaTable foreignKey = SchemaTable.of(vertexLabel.getSchema().getName(), vertexLabel.getLabel() + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END));
        if (!foreignKeysContains(direction, vertexLabel)) {
            //Make sure the current thread/transaction owns the lock
            Schema schema = this.getSchema();
            schema.getTopology().lock();
            if (!foreignKeysContains(direction, vertexLabel)) {
                TopologyManager.addLabelToEdge(this.sqlgGraph, this.getSchema().getName(), EDGE_PREFIX + getLabel(), direction == Direction.IN, foreignKey);
                if (direction == Direction.IN) {
                    this.uncommittedInVertexLabels.add(vertexLabel);
                    vertexLabel.addToUncommittedInEdgeLabels(schema, this);
                } else {
                    this.uncommittedOutVertexLabels.add(vertexLabel);
                    vertexLabel.addToUncommittedOutEdgeLabels(schema, this);
                }
                SchemaTable vertexSchemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), vertexLabel.getLabel());
                addEdgeForeignKey(schema.getName(), EDGE_PREFIX + getLabel(), foreignKey, vertexSchemaTable);
                this.getSchema().getTopology().fire(this, vertexSchemaTable.toString(), TopologyChangeAction.ADD_IN_VERTEX_LABELTO_EDGE);
            }
        }
    }

    private void addEdgeForeignKey(String schema, String table, SchemaTable foreignKey, SchemaTable otherVertex) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "BUG: ensureEdgeVertexLabelExist may not be called for %s", SQLG_SCHEMA);
        String sqlStr = this.sqlgGraph.getSqlDialect().addColumnStatement(schema, table,
                foreignKey.getSchema() + "." + foreignKey.getTable(), this.sqlgGraph.getSqlDialect().getForeignKeyTypeDefinition());
        if (logger.isDebugEnabled()) {
            logger.debug(sqlStr);
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sqlStr)) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        StringBuilder sql = new StringBuilder();
        //foreign key definition start
        if (this.sqlgGraph.getTopology().isImplementingForeignKeys()) {
            sql.append(" ALTER TABLE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sql.append(" ADD CONSTRAINT ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table + "_" + foreignKey.getSchema() + "." + foreignKey.getTable() + "_fkey"));
            sql.append(" FOREIGN KEY (");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
            sql.append(") REFERENCES ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(otherVertex.getSchema()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + otherVertex.getTable()));
            sql.append(" (");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            if (this.sqlgGraph.getSqlDialect().supportsDeferrableForeignKey()) {
                sql.append(") DEFERRABLE");
            } else {
                sql.append(")");
            }
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        sql.setLength(0);
        if (this.sqlgGraph.getSqlDialect().needForeignKeyIndex()) {
            sql.append("\nCREATE INDEX");
            if (this.sqlgGraph.getSqlDialect().requiresIndexName()) {
                sql.append(" ");
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                        this.sqlgGraph.getSqlDialect().indexName(
                                SchemaTable.of(schema, table).withOutPrefix(),
                                EDGE_PREFIX,
                                "_idx",
                                Collections.singletonList(foreignKey.getSchema() + "_" + foreignKey.getTable())
                        )));
            }
            sql.append(" ON ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sql.append(" (");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
            sql.append(")");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void addToOutVertexLabel(VertexLabel vertexLabel) {
        this.outVertexLabels.add(vertexLabel);
    }

    void addToInVertexLabel(VertexLabel vertexLabel) {
        this.inVertexLabels.add(vertexLabel);
    }

    @Override
    public int hashCode() {
        //Edges are unique per out vertex schemas.
        //An edge must have at least one out vertex so take it to get the schema.
        VertexLabel vertexLabel;
        if (!this.outVertexLabels.isEmpty()) {
            vertexLabel = this.outVertexLabels.iterator().next();
        } else {
            vertexLabel = this.uncommittedOutVertexLabels.iterator().next();
        }
        return (vertexLabel.getSchema().getName() + this.getLabel()).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        if (!(other instanceof EdgeLabel)) {
            return false;
        }
        EdgeLabel otherEdgeLabel = (EdgeLabel) other;
        if (isValid()) {
            if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedInVertexLabels.isEmpty()) {
                VertexLabel vertexLabel = this.uncommittedOutVertexLabels.iterator().next();
                VertexLabel otherVertexLabel = otherEdgeLabel.uncommittedOutVertexLabels.iterator().next();
                return vertexLabel.getSchema().equals(otherVertexLabel.getSchema()) && otherEdgeLabel.getLabel().equals(this.getLabel());
            } else {
                VertexLabel vertexLabel = this.outVertexLabels.iterator().next();
                VertexLabel otherVertexLabel = otherEdgeLabel.outVertexLabels.iterator().next();
                return vertexLabel.getSchema().equals(otherVertexLabel.getSchema()) && otherEdgeLabel.getLabel().equals(this.getLabel());
            }
        }
        return otherEdgeLabel.getLabel().equals(this.getLabel());
    }

    boolean deepEquals(EdgeLabel otherEdgeLabel) {
        Preconditions.checkState(this.equals(otherEdgeLabel), "equals must have passed before calling deepEquals");

        //check every out and in edge
        for (VertexLabel outVertexLabel : this.outVertexLabels) {
            boolean ok = false;
            for (VertexLabel otherOutVertexLabel : otherEdgeLabel.outVertexLabels) {
                if (outVertexLabel.equals(otherOutVertexLabel)) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                return false;
            }
        }
        for (VertexLabel inVertexLabel : this.inVertexLabels) {
            boolean ok = false;
            for (VertexLabel otherInVertexLabel : otherEdgeLabel.inVertexLabels) {
                if (inVertexLabel.equals(otherInVertexLabel)) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected JsonNode toJson() {
        ObjectNode edgeLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        if (isValid()) {
            edgeLabelNode.put("schema", getSchema().getName());
        }
        edgeLabelNode.put("label", getLabel());
        edgeLabelNode.set("properties", super.toJson());

        ArrayNode outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (VertexLabel outVertexLabel : this.outVertexLabels) {
            ObjectNode outVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
            outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
            outVertexLabelArrayNode.add(outVertexLabelObjectNode);
        }
        edgeLabelNode.set("outVertexLabels", outVertexLabelArrayNode);

        ArrayNode inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (VertexLabel inVertexLabel : this.inVertexLabels) {
            ObjectNode inVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
            inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
            inVertexLabelArrayNode.add(inVertexLabelObjectNode);
        }
        edgeLabelNode.set("inVertexLabels", inVertexLabelArrayNode);

        if (isValid() && this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread()) {
            outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel outVertexLabel : this.uncommittedOutVertexLabels) {
                ObjectNode outVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
                outVertexLabelArrayNode.add(outVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedOutVertexLabels", outVertexLabelArrayNode);

            inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel inVertexLabel : this.uncommittedInVertexLabels) {
                ObjectNode inVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
                inVertexLabelArrayNode.add(inVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedInVertexLabels", inVertexLabelArrayNode);
        }

        return edgeLabelNode;
    }

    @Override
    protected Optional<JsonNode> toNotifyJson() {

        boolean foundSomething = false;
        ObjectNode edgeLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        edgeLabelNode.put("schema", getSchema().getName());
        edgeLabelNode.put("label", getLabel());
        edgeLabelNode.put("partitionType", this.partitionType.name());
        edgeLabelNode.put("partitionExpression", this.partitionExpression);

        Optional<JsonNode> abstractLabelNode = super.toNotifyJson();
        if (abstractLabelNode.isPresent()) {
            foundSomething = true;
            edgeLabelNode.set("uncommittedProperties", abstractLabelNode.get().get("uncommittedProperties"));
            edgeLabelNode.set("uncommittedIdentifiers", abstractLabelNode.get().get("uncommittedIdentifiers"));
            edgeLabelNode.set("uncommittedIndexes", abstractLabelNode.get().get("uncommittedIndexes"));
            edgeLabelNode.set("uncommittedPartitions", abstractLabelNode.get().get("uncommittedPartitions"));
            edgeLabelNode.set("partitions", abstractLabelNode.get().get("partitions"));
            if (abstractLabelNode.get().get("uncommittedDistributionPropertyColumn") != null) {
                edgeLabelNode.set("uncommittedDistributionPropertyColumn", abstractLabelNode.get().get("uncommittedDistributionPropertyColumn"));
            }
            if (abstractLabelNode.get().get("uncommittedShardCount") != null) {
                edgeLabelNode.set("uncommittedShardCount", abstractLabelNode.get().get("uncommittedShardCount"));
            }
            if (abstractLabelNode.get().get("uncommittedDistributionColocateAbstractLabel") != null) {
                edgeLabelNode.set("uncommittedDistributionColocateAbstractLabel", abstractLabelNode.get().get("uncommittedDistributionColocateAbstractLabel"));
            }
            edgeLabelNode.set("uncommittedRemovedProperties", abstractLabelNode.get().get("uncommittedRemovedProperties"));
            edgeLabelNode.set("uncommittedRemovedPartitions", abstractLabelNode.get().get("uncommittedRemovedPartitions"));
            edgeLabelNode.set("uncommittedRemovedIndexes", abstractLabelNode.get().get("uncommittedRemovedIndexes"));
        }

        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedOutVertexLabels.isEmpty()) {
            foundSomething = true;
            ArrayNode outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel outVertexLabel : this.uncommittedOutVertexLabels) {
                ObjectNode outVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
                outVertexLabelArrayNode.add(outVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedOutVertexLabels", outVertexLabelArrayNode);
        }

        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedRemovedOutVertexLabels.isEmpty()) {
            foundSomething = true;
            ArrayNode outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel outVertexLabel : this.uncommittedRemovedOutVertexLabels) {
                ObjectNode outVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
                outVertexLabelArrayNode.add(outVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedRemovedOutVertexLabels", outVertexLabelArrayNode);
        }

        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedInVertexLabels.isEmpty()) {
            foundSomething = true;
            ArrayNode inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel inVertexLabel : this.uncommittedInVertexLabels) {
                ObjectNode inVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
                inVertexLabelArrayNode.add(inVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedInVertexLabels", inVertexLabelArrayNode);
        }

        if (this.getSchema().getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedRemovedInVertexLabels.isEmpty()) {
            foundSomething = true;
            ArrayNode inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel inVertexLabel : this.uncommittedRemovedInVertexLabels) {
                ObjectNode inVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
                inVertexLabelArrayNode.add(inVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedRemovedInVertexLabels", inVertexLabelArrayNode);
        }

        if (foundSomething) {
            return Optional.of(edgeLabelNode);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException {
        List<Topology.TopologyValidationError> validationErrors = new ArrayList<>();
        for (PropertyColumn propertyColumn : getProperties().values()) {
            List<Triple<String, Integer, String>> columns = this.sqlgGraph.getSqlDialect().getTableColumns(metadata, null, this.getSchema().getName(), "E_" + this.getLabel(), propertyColumn.getName());
            if (columns.isEmpty()) {
                validationErrors.add(new Topology.TopologyValidationError(propertyColumn));
            }
//            try (ResultSet propertyRs = metadata.getColumns(null, this.getSchema().getName(), "E_" + this.getLabel(), propertyColumn.getName())) {
//                if (!propertyRs.next()) {
//                    validationErrors.add(new Topology.TopologyValidationError(propertyColumn));
//                }
//            }
        }
        return validationErrors;
    }

    @Override
    public String getPrefix() {
        return EDGE_PREFIX;
    }

    @Override
    void removeProperty(PropertyColumn propertyColumn, boolean preserveData) {
        this.getSchema().getTopology().lock();
        if (!uncommittedRemovedProperties.contains(propertyColumn.getName())) {
            uncommittedRemovedProperties.add(propertyColumn.getName());
            TopologyManager.removeEdgeColumn(this.sqlgGraph, this.getSchema().getName(), EDGE_PREFIX + getLabel(), propertyColumn.getName());
            if (!preserveData) {
                removeColumn(this.getSchema().getName(), EDGE_PREFIX + getLabel(), propertyColumn.getName());
            }
            this.getSchema().getTopology().fire(propertyColumn, "", TopologyChangeAction.DELETE);
        }
    }

    @Override
    public void remove(boolean preserveData) {
        getSchema().removeEdgeLabel(this, preserveData);
    }

    /**
     * delete the table
     */
    void delete() {
        String schema = getSchema().getName();
        String tableName = EDGE_PREFIX + getLabel();

        SqlDialect sqlDialect = this.sqlgGraph.getSqlDialect();
        sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS ");
        sql.append(sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(tableName));
        if (sqlDialect.supportsCascade()) {
            sql.append(" CASCADE");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        if (sqlDialect.needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * delete a given column from the table
     *
     * @param column The column to delete.
     */
    private void deleteColumn(String column) {
        removeColumn(getSchema().getName(), EDGE_PREFIX + getLabel(), column);
    }

    /**
     * remove a vertex label from the out collection
     *
     * @param lbl          the vertex label
     * @param preserveData should we keep the sql data?
     */
    void removeOutVertexLabel(VertexLabel lbl, boolean preserveData) {
        this.uncommittedRemovedOutVertexLabels.add(lbl);
        if (!preserveData) {
            deleteColumn(lbl.getFullName() + Topology.OUT_VERTEX_COLUMN_END);
        }
    }

    /**
     * remove a vertex label from the in collection
     *
     * @param lbl          the vertex label
     * @param preserveData should we keep the sql data?
     */
    void removeInVertexLabel(VertexLabel lbl, boolean preserveData) {
        this.uncommittedRemovedInVertexLabels.add(lbl);
        if (!preserveData) {
            deleteColumn(lbl.getFullName() + Topology.IN_VERTEX_COLUMN_END);
        }
    }

    public void ensureDistributed(int shardCount, PropertyColumn distributionPropertyColumn) {
        ensureDistributed(shardCount, distributionPropertyColumn, getOutVertexLabels().iterator().next());
    }
}
