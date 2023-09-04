package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.SqlgUtil;
import org.umlg.sqlg.util.ThreadLocalSet;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class EdgeLabel extends AbstractLabel {

    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeLabel.class);
    //This just won't stick in my brain.
    //hand (out) ----<label>---- finger (in)
    final Set<EdgeRole> outEdgeRoles = ConcurrentHashMap.newKeySet();
    final Set<EdgeRole> inEdgeRoles = ConcurrentHashMap.newKeySet();
    final Set<EdgeRole> uncommittedOutEdgeRoles = new ThreadLocalSet<>();
    final Set<EdgeRole> uncommittedInEdgeRoles = new ThreadLocalSet<>();
    final Set<EdgeRole> uncommittedRemovedInEdgeRoles = new ThreadLocalSet<>();
    final Set<EdgeRole> uncommittedRemovedOutEdgeRoles = new ThreadLocalSet<>();

    private final Topology topology;

    static EdgeLabel loadSqlgSchemaEdgeLabel(
            String edgeLabelName,
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> properties) {

        //edges are created in the out vertex's schema.
        return new EdgeLabel(true, edgeLabelName, outVertexLabel, inVertexLabel, properties, new ListOrderedSet<>(), EdgeDefinition.of());
    }

    static EdgeLabel createEdgeLabel(
            String edgeLabelName,
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> properties,
            ListOrderedSet<String> identifiers,
            EdgeDefinition edgeDefinition) {

        Preconditions.checkState(!inVertexLabel.getSchema().isSqlgSchema(), "You may not create an edge to %s", Topology.SQLG_SCHEMA);
        //edges are created in the out vertex's schema.
        EdgeLabel edgeLabel = new EdgeLabel(false, edgeLabelName, outVertexLabel, inVertexLabel, properties, identifiers, edgeDefinition);
        edgeLabel.createEdgeTableOnDb(outVertexLabel, inVertexLabel, properties, identifiers, false, edgeDefinition);
        edgeLabel.committed = false;
        return edgeLabel;
    }

    static EdgeLabel createPartitionedEdgeLabel(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            final Map<String, PropertyDefinition> properties,
            final ListOrderedSet<String> identifiers,
            final PartitionType partitionType,
            final String partitionExpression,
            boolean isForeignKeyPartition,
            EdgeDefinition edgeDefinition) {

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
        edgeLabel.createEdgeTableOnDb(outVertexLabel, inVertexLabel, properties, identifiers, isForeignKeyPartition, edgeDefinition);
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
            final Map<String, PropertyDefinition> properties,
            final ListOrderedSet<String> identifiers,
            final PartitionType partitionType,
            final String partitionExpression) {

        super(outVertexLabel.getSchema().getSqlgGraph(), edgeLabelName, properties, identifiers, partitionType, partitionExpression);
        if (forSqlgSchema) {
            this.outEdgeRoles.add(new EdgeRole(outVertexLabel, this, Direction.OUT, true, Multiplicity.of(0, -1)));
            this.inEdgeRoles.add(new EdgeRole(inVertexLabel, this, Direction.IN, true, Multiplicity.of(0, -1)));
        } else {
            this.uncommittedOutEdgeRoles.add(new EdgeRole(outVertexLabel, this, Direction.OUT, false, Multiplicity.of(0, -1)));
            this.uncommittedInEdgeRoles.add(new EdgeRole(inVertexLabel, this, Direction.IN, false, Multiplicity.of(0, -1)));
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
            Map<String, PropertyDefinition> properties,
            ListOrderedSet<String> identifiers,
            EdgeDefinition edgeDefinition) {

        super(outVertexLabel.getSchema().getSqlgGraph(), edgeLabelName, properties, identifiers);
        if (forSqlgSchema) {
            this.outEdgeRoles.add(new EdgeRole(outVertexLabel, this, Direction.OUT, true, edgeDefinition.outMultiplicity()));
            this.inEdgeRoles.add(new EdgeRole(inVertexLabel, this, Direction.IN, true, edgeDefinition.inMultiplicity()));
        } else {
            this.uncommittedOutEdgeRoles.add(new EdgeRole(outVertexLabel, this, Direction.OUT, false, edgeDefinition.outMultiplicity()));
            this.uncommittedInEdgeRoles.add(new EdgeRole(inVertexLabel, this, Direction.IN, false, edgeDefinition.inMultiplicity()));
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

    EdgeLabel(SqlgGraph sqlgGraph, String edgeLabelName, Map<String, PropertyDefinition> properties, ListOrderedSet<String> identifiers) {
        super(sqlgGraph, edgeLabelName, properties, identifiers);
        this.topology = sqlgGraph.getTopology();
    }

    EdgeLabel(Topology topology, String edgeLabelName) {
        super(topology.getSqlgGraph(), edgeLabelName, Collections.emptyMap(), new ListOrderedSet<>());
        this.topology = topology;
    }

    EdgeLabel(Topology topology, String edgeLabelName, PartitionType partitionType, String partitionExpression) {
        super(topology.getSqlgGraph(), edgeLabelName, partitionType, partitionExpression);
        this.topology = topology;
    }

    private EdgeLabel(Topology topology, String edgeLabelName, boolean isForeignEdgeLabel) {
        super(topology.getSqlgGraph(), edgeLabelName, isForeignEdgeLabel);
        Preconditions.checkState(isForeignEdgeLabel);
        this.topology = topology;
    }

    @Override
    public Schema getSchema() {
        if (!this.outEdgeRoles.isEmpty()) {
            VertexLabel vertexLabel = this.outEdgeRoles.iterator().next().getVertexLabel();
            return vertexLabel.getSchema();
        } else if (this.topology.isSchemaChanged() && !this.uncommittedOutEdgeRoles.isEmpty()) {
            VertexLabel vertexLabel = this.uncommittedOutEdgeRoles.iterator().next().getVertexLabel();
            return vertexLabel.getSchema();
        } else {
            throw new IllegalStateException("BUG: no outVertexLabels present when getSchema() is called");
        }
    }

    @Override
    public Topology getTopology() {
        return this.topology;
    }

//        @Override
    public void ensurePropertiesExist(Map<String, PropertyDefinition> columns) {
        for (Map.Entry<String, PropertyDefinition> column : columns.entrySet()) {
            PropertyDefinition incomingPropertyDefinition = column.getValue();
            PropertyColumn propertyColumn = this.properties.get(column.getKey());
            if (propertyColumn == null) {
                Preconditions.checkState(!this.getSchema().isSqlgSchema(), "schema may not be %s", SQLG_SCHEMA);
                this.sqlgGraph.getSqlDialect().validateColumnName(column.getKey());
                propertyColumn = this.uncommittedProperties.get(column.getKey());
                if (propertyColumn == null) {
                    this.getSchema().getTopology().startSchemaChange(
                            String.format("EdgeLabel '%s' ensurePropertiesExist with '%s'", getFullName(), columns.keySet().stream().reduce((a,b) -> a + "," + b).orElse(""))
                    );
                    if (getProperty(column.getKey()).isEmpty()) {
                        TopologyManager.addEdgeColumn(
                                this.sqlgGraph,
                                this.getSchema().getName(),
                                EDGE_PREFIX + getLabel(),
                                column,
                                new ListOrderedSet<>()
                        );
                        addColumn(
                                this.getSchema().getName(),
                                EDGE_PREFIX + getLabel(),
                                ImmutablePair.of(column.getKey(), column.getValue())
                        );
                        propertyColumn = new PropertyColumn(this, column.getKey(), column.getValue());
                        propertyColumn.setCommitted(false);
                        this.uncommittedProperties.put(column.getKey(), propertyColumn);
                        this.getSchema().getTopology().fire(propertyColumn, null, TopologyChangeAction.CREATE, true);
                    }
                } else {
                    //Set the proper definition in the map;
                    if (!this.sqlgGraph.tx().isInStreamingBatchMode()) {
                        SqlgUtil.validateIncomingPropertyType(
                                getFullName() + "." + column.getKey(),
                                incomingPropertyDefinition,
                                getFullName() + "." + propertyColumn.getName(),
                                propertyColumn.getPropertyDefinition()
                        );
                    }
                    columns.put(column.getKey(), propertyColumn.getPropertyDefinition());
                }
            } else {
                //Set the proper definition in the map;
                SqlgUtil.validateIncomingPropertyType(
                        getFullName() + "." + column.getKey(),
                        incomingPropertyDefinition,
                        getFullName() + "." + propertyColumn.getName(),
                        propertyColumn.getPropertyDefinition()
                );
                columns.put(column.getKey(), propertyColumn.getPropertyDefinition());
            }
        }
    }

    private void createEdgeTableOnDb(
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            boolean isForeignKeyPartition,
            EdgeDefinition edgeDefinition) {

        String schema = outVertexLabel.getSchema().getName();
        String tableName = EDGE_PREFIX + getLabel();

        SqlDialect sqlDialect = this.sqlgGraph.getSqlDialect();
        sqlDialect.assertTableName(tableName);
        StringBuilder createTableSql = new StringBuilder(sqlDialect.createTableStatement());
        createTableSql.append(sqlDialect.maybeWrapInQoutes(schema));
        createTableSql.append(".");
        createTableSql.append(sqlDialect.maybeWrapInQoutes(tableName));
        if (identifiers.isEmpty()) {
            createTableSql.append("(\n\t");
            createTableSql.append(sqlDialect.maybeWrapInQoutes("ID"));
            createTableSql.append(" ");
            if (this.partitionType.isNone()) {
                createTableSql.append(sqlDialect.getAutoIncrementPrimaryKeyConstruct());
            } else {
                createTableSql.append(sqlDialect.getAutoIncrement());
            }
            if (columns.size() > 0) {
                createTableSql.append(", ");
            }
        } else {
            createTableSql.append("(\n\t");
        }
        buildColumns(this.sqlgGraph, identifiers, columns, createTableSql);
        if (!isForeignKeyPartition && !identifiers.isEmpty()) {
            createTableSql.append(",\n\tPRIMARY KEY(");
            int count = 1;
            for (String identifier : identifiers) {
                createTableSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                if (count++ < identifiers.size()) {
                    createTableSql.append(", ");
                }
            }
            createTableSql.append(")");
        }

        createTableSql.append(",");
        if (inVertexLabel.hasIDPrimaryKey()) {
            createTableSql.append("\n\t");
            createTableSql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
            createTableSql.append(" ");
            createTableSql.append(sqlDialect.getForeignKeyTypeDefinition());
        } else {
            createTableSql.append("\n\t");
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
                            createTableSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + Topology.IN_VERTEX_COLUMN_END)).append(" ").append(sqlDefinition);
                        } else {
                            //The first column existVertexLabel no postfix
                            createTableSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END)).append(" ").append(sqlDefinition);
                        }
                        if (count++ < propertyTypeToSqlDefinition.length) {
                            createTableSql.append(", ");
                        }
                    }
                    if (outVertexLabel.isDistributed()) {
                        if (i++ < inVertexLabel.getIdentifiers().size() - 1) {
                            createTableSql.append(", ");
                        }
                    } else {
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    }
                }
            }
        }
        createTableSql.append(",");
        if (outVertexLabel.hasIDPrimaryKey()) {
            createTableSql.append("\n\t");
            createTableSql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
            createTableSql.append(" ");
            createTableSql.append(sqlDialect.getForeignKeyTypeDefinition());
        } else {
            int i = 1;
            createTableSql.append("\n\t");

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
                            createTableSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + Topology.OUT_VERTEX_COLUMN_END)).append(" ").append(sqlDefinition);
                        } else {
                            //The first column existVertexLabel no postfix
                            createTableSql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END)).append(" ").append(sqlDefinition);
                        }
                        if (count++ < propertyTypeToSqlDefinition.length) {
                            createTableSql.append(", ");
                        }
                    }
                    if (outVertexLabel.isDistributed()) {
                        if (i++ < outVertexLabel.getIdentifiers().size() - 1) {
                            createTableSql.append(", ");
                        }
                    } else {
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    }
                }
            }

        }

        //foreign key definition start
        if (inVertexLabel.getPartitionType().isNone() &&
                outVertexLabel.getPartitionType().isNone() &&
                this.partitionType.isNone() &&
                this.sqlgGraph.getTopology().isImplementingForeignKeys()) {

            createTableSql.append(",\n\t");
            createTableSql.append("FOREIGN KEY (");
            if (inVertexLabel.hasIDPrimaryKey()) {
                createTableSql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        createTableSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    } else {
                        createTableSql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    }
                }
            }
            createTableSql.append(") REFERENCES ");
            createTableSql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName()));
            createTableSql.append(".");
            createTableSql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + inVertexLabel.getLabel()));
            createTableSql.append(" (");
            if (inVertexLabel.hasIDPrimaryKey()) {
                createTableSql.append(sqlDialect.maybeWrapInQoutes("ID"));
            } else {
                int i = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        createTableSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    } else {
                        createTableSql.append(sqlDialect.maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    }
                }
            }
            createTableSql.append(") ");
            if (sqlDialect.supportsDeferrableForeignKey()) {
                createTableSql.append("DEFERRABLE");
            }
            createTableSql.append(",\n\tFOREIGN KEY (");
            if (outVertexLabel.hasIDPrimaryKey()) {
                createTableSql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        createTableSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    } else {
                        createTableSql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    }
                }
            }
            createTableSql.append(") REFERENCES ");
            createTableSql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName()));
            createTableSql.append(".");
            createTableSql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + outVertexLabel.getLabel()));
            createTableSql.append(" (");
            if (outVertexLabel.hasIDPrimaryKey()) {
                createTableSql.append(sqlDialect.maybeWrapInQoutes("ID"));
            } else {
                int i = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        createTableSql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    } else {
                        createTableSql.append(sqlDialect.maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createTableSql.append(", ");
                        }
                    }
                }
            }
            createTableSql.append(") ");
            if (sqlDialect.supportsDeferrableForeignKey()) {
                createTableSql.append("DEFERRABLE");
            }
            if (sqlDialect.needForeignKeyIndex() && sqlDialect.isIndexPartOfCreateTable()) {
                //This is true for Cockroachdb
                createTableSql.append(", INDEX (");
                createTableSql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName() + "." + inVertexLabel.getLabel() + Topology.IN_VERTEX_COLUMN_END));
                createTableSql.append("), INDEX (");
                createTableSql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName() + "." + outVertexLabel.getLabel() + Topology.OUT_VERTEX_COLUMN_END));
                createTableSql.append(")");
            }
        }
        //foreign key definition end

        createTableSql.append("\n)");
        if (!partitionType.isNone()) {
            createTableSql.append(" PARTITION BY ");
            createTableSql.append(this.partitionType.name());
            createTableSql.append(" (");
            createTableSql.append(this.partitionExpression);
            createTableSql.append(")");
        }

        if (sqlDialect.needsSemicolon()) {
            createTableSql.append(";");
        }

        StringBuilder createIndexIn = new StringBuilder();
        StringBuilder createIndexOut = new StringBuilder();
        Multiplicity outMultiplicity = edgeDefinition.outMultiplicity();
        Multiplicity inMultiplicity = edgeDefinition.inMultiplicity();
        if (this.partitionType.isNone() && sqlDialect.needForeignKeyIndex() && !sqlDialect.isIndexPartOfCreateTable() ||
                (this.partitionType.isNone() && isMultiplicityOneToOne(outMultiplicity, inMultiplicity))) {

            createIndexIn.append("\n\tCREATE ");
            if (isMultiplicityOneToOne(outMultiplicity, inMultiplicity)) {
                createIndexIn.append("UNIQUE ");
            }
            createIndexIn.append("INDEX");
            if (sqlDialect.requiresIndexName()) {
                createIndexIn.append(" ");
                createIndexIn.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(
                        SchemaTable.of(schema, tableName).withOutPrefix(),
                        EDGE_PREFIX,
                        "_idx",
                        Collections.singletonList(
                                inVertexLabel.getSchema().getName() + "_" + inVertexLabel.getLabel() + Topology.IN_VERTEX_COLUMN_END
                        ))));
            }
            createIndexIn.append(" ON ");
            createIndexIn.append(sqlDialect.maybeWrapInQoutes(schema));
            createIndexIn.append(".");
            createIndexIn.append(sqlDialect.maybeWrapInQoutes(tableName));
            createIndexIn.append(" (");
            if (inVertexLabel.hasIDPrimaryKey()) {
                createIndexIn.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        createIndexIn.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createIndexIn.append(", ");
                        }
                    } else {
                        createIndexIn.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createIndexIn.append(", ");
                        }
                    }
                }
            }
            createIndexIn.append(");");

            createIndexOut.append("\n\tCREATE ");
            outMultiplicity = edgeDefinition.outMultiplicity();
            inMultiplicity = edgeDefinition.inMultiplicity();
            if (isMultiplicityOneToOne(outMultiplicity, inMultiplicity)) {
                createIndexOut.append("UNIQUE ");
            }
            createIndexOut.append("INDEX");
            if (sqlDialect.requiresIndexName()) {
                createIndexOut.append(" ");
                createIndexOut.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(
                        SchemaTable.of(schema, tableName).withOutPrefix(),
                        EDGE_PREFIX,
                        "_idx",
                        Collections.singletonList(
                                outVertexLabel.getSchema().getName() + "_" + outVertexLabel.getLabel() + Topology.OUT_VERTEX_COLUMN_END
                        ))));
            }
            createIndexOut.append(" ON ");
            createIndexOut.append(sqlDialect.maybeWrapInQoutes(schema));
            createIndexOut.append(".");
            createIndexOut.append(sqlDialect.maybeWrapInQoutes(tableName));
            createIndexOut.append(" (");
            if (outVertexLabel.hasIDPrimaryKey()) {
                createIndexOut.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        createIndexOut.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createIndexOut.append(", ");
                        }
                    } else {
                        createIndexOut.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createIndexOut.append(", ");
                        }
                    }
                }
            }
            createIndexOut.append(");");
        }

        //Multiplicity unique constraints
        StringBuilder createIndexInAndOut = new StringBuilder();
        if ((outMultiplicity.isOne() && inMultiplicity.isMany() && inMultiplicity.unique()) ||
                (outMultiplicity.isMany() && inMultiplicity.isOne() && outMultiplicity.unique()) ||
                (outMultiplicity.isMany() && inMultiplicity.isMany() && outMultiplicity.unique() && inMultiplicity.unique())) {

            //Create 1 unique constraints over both foreign key columns
            createIndexInAndOut.append("\n\tCREATE UNIQUE INDEX");
            if (sqlDialect.requiresIndexName()) {
                createIndexInAndOut.append(" ");
                createIndexInAndOut.append(sqlDialect.maybeWrapInQoutes(sqlDialect.indexName(
                        SchemaTable.of(schema, tableName).withOutPrefix(),
                        EDGE_PREFIX,
                        "_idx",
                        List.of(
                                outVertexLabel.getSchema().getName() + "_" + outVertexLabel.getLabel() + Topology.OUT_VERTEX_COLUMN_END,
                                inVertexLabel.getSchema().getName() + "_" + inVertexLabel.getLabel() + Topology.IN_VERTEX_COLUMN_END
                        ))));
            }
            createIndexInAndOut.append(" ON ");
            createIndexInAndOut.append(sqlDialect.maybeWrapInQoutes(schema));
            createIndexInAndOut.append(".");
            createIndexInAndOut.append(sqlDialect.maybeWrapInQoutes(tableName));
            createIndexInAndOut.append(" (");
            if (outVertexLabel.hasIDPrimaryKey()) {
                createIndexInAndOut.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
                createIndexInAndOut.append(", ");
            } else {
                int i = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        createIndexInAndOut.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createIndexInAndOut.append(", ");
                        }
                    } else {
                        createIndexInAndOut.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            createIndexInAndOut.append(", ");
                        }
                    }
                }
                createIndexInAndOut.append(", ");
            }
            if (inVertexLabel.hasIDPrimaryKey()) {
                createIndexInAndOut.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
            } else {
                int i = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    if (inVertexLabel.isDistributed() && inVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        createIndexInAndOut.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createIndexInAndOut.append(", ");
                        }
                    } else {
                        createIndexInAndOut.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            createIndexInAndOut.append(", ");
                        }
                    }
                }
            }
            createIndexInAndOut.append(");");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(createTableSql.toString());
            if (createIndexIn.length() > 0) {
                LOGGER.debug(createIndexIn.toString());
            }
            if (createIndexOut.length() > 0) {
                LOGGER.debug(createIndexOut.toString());
            }
            if (createIndexInAndOut.length() > 0) {
                LOGGER.debug(createIndexInAndOut.toString());
            }
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql.toString());
            if (createIndexIn.length() > 0) {
                stmt.execute(createIndexIn.toString());
            }
            if (createIndexOut.length() > 0) {
                stmt.execute(createIndexOut.toString());
            }
            if (createIndexInAndOut.length() > 0) {
                stmt.execute(createIndexInAndOut.toString());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isMultiplicityOneToOne(Multiplicity outMultiplicity, Multiplicity inMultiplicity) {
        return outMultiplicity.isOne() && inMultiplicity.isOne();
    }

    void afterCommit() {
        Preconditions.checkState(this.topology.isSchemaChanged(), "EdgeLabel.afterCommit must have schemaChanged as true");
        super.afterCommit();
        for (Iterator<EdgeRole> it = this.uncommittedInEdgeRoles.iterator(); it.hasNext(); ) {
            EdgeRole inEdgeRole = it.next();
            this.inEdgeRoles.add(inEdgeRole);
            it.remove();
        }
        for (Iterator<EdgeRole> it = this.uncommittedRemovedInEdgeRoles.iterator(); it.hasNext(); ) {
            EdgeRole inEdgeRole = it.next();
            this.inEdgeRoles.remove(inEdgeRole);
            it.remove();
        }
        for (Iterator<EdgeRole> it = this.uncommittedOutEdgeRoles.iterator(); it.hasNext(); ) {
            EdgeRole outEdgeRole = it.next();
            this.outEdgeRoles.add(outEdgeRole);
            it.remove();
        }
        for (Iterator<EdgeRole> it = this.uncommittedRemovedOutEdgeRoles.iterator(); it.hasNext(); ) {
            EdgeRole outEdgeRole = it.next();
            this.outEdgeRoles.remove(outEdgeRole);
            it.remove();
        }
    }

    void afterRollbackInEdges(VertexLabel vertexLabel) {
        Preconditions.checkState(this.topology.isSchemaChanged(), "EdgeLabel.afterCommit must have schemaChanged as true");
        super.afterRollback();
        this.uncommittedInEdgeRoles.remove(vertexLabel);
        this.uncommittedRemovedInEdgeRoles.remove(vertexLabel);
    }

    void afterRollbackOutEdges(VertexLabel vertexLabel) {
        Preconditions.checkState(this.topology.isSchemaChanged(), "EdgeLabel.afterCommit must have schemaChanged as true");
        super.afterRollback();
        this.uncommittedOutEdgeRoles.remove(vertexLabel);
        this.uncommittedRemovedOutEdgeRoles.remove(vertexLabel);
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean foreignKeysContains(Direction direction, VertexLabel vertexLabel, EdgeDefinition edgeDefinition) {
        EdgeRole outEdgeRole = new EdgeRole(vertexLabel, this, Direction.OUT, true, edgeDefinition.outMultiplicity());
        EdgeRole inEdgeRole = new EdgeRole(vertexLabel, this, Direction.IN, true, edgeDefinition.inMultiplicity());
        switch (direction) {
            case OUT:
                if (this.outEdgeRoles.contains(outEdgeRole)) {
                    return true;
                } else {
                    //Check if the EdgeRole exist with a different multiplicity.
                    for (EdgeRole edgeRole : this.outEdgeRoles) {
                        if (edgeRole.getVertexLabel().equals(vertexLabel) && this.equals(edgeRole.getEdgeLabel())) {
                            throw new RuntimeException("Different multiplicity");
                        }
                    }
                }
                break;
            case IN:
                if (this.inEdgeRoles.contains(inEdgeRole)) {
                    return true;
                } else {
                    //Check if the EdgeRole exist with a different multiplicity.
                    for (EdgeRole edgeRole : this.inEdgeRoles) {
                        if (edgeRole.getVertexLabel().equals(vertexLabel) && this.equals(edgeRole.getEdgeLabel())) {
                            throw new RuntimeException("Different multiplicity");
                        }
                    }

                }
                break;
            case BOTH:
                throw new IllegalStateException("foreignKeysContains may not be called for Direction.BOTH");
        }
        if (this.topology.isSchemaChanged()) {
            switch (direction) {
                case OUT:
                    if (this.uncommittedOutEdgeRoles.contains(outEdgeRole)) {
                        return true;
                    } else {
                        //Check if the EdgeRole exist with a different multiplicity.
                        for (EdgeRole edgeRole : this.uncommittedOutEdgeRoles) {
                            if (edgeRole.getVertexLabel().equals(vertexLabel) && this.equals(edgeRole.getEdgeLabel())) {
                                throw new RuntimeException(String.format("EdgeRole [%s][%s][%s] already exists with a multiplicity of %s", this.getLabel(), direction.name(), vertexLabel.getLabel(), edgeRole.getMultiplicity().toString()));
                            }
                        }
                    }
                    break;
                case IN:
                    if (this.uncommittedInEdgeRoles.contains(inEdgeRole)) {
                        return true;
                    } else {
                        //Check if the EdgeRole exist with a different multiplicity.
                        for (EdgeRole edgeRole : this.uncommittedInEdgeRoles) {
                            if (edgeRole.getVertexLabel().equals(vertexLabel) && this.equals(edgeRole.getEdgeLabel())) {
                                throw new RuntimeException(String.format("EdgeRole [%s][%s][%s] already exists with a multiplicity of %s", this.getLabel(), direction.name(), vertexLabel.getLabel(), edgeRole.getMultiplicity().toString()));
                            }
                        }
                    }
                    break;
            }
        }
        return false;
    }

    Set<ForeignKey> getAllEdgeForeignKeys() {
        Set<ForeignKey> result = new HashSet<>();
        for (VertexLabel vertexLabel : this.getInVertexLabels()) {
            if (!this.topology.isSchemaChanged() || !this.uncommittedRemovedInEdgeRoles.contains(vertexLabel)) {
                if (vertexLabel.hasIDPrimaryKey()) {
                    result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
                } else {
                    ForeignKey foreignKey = new ForeignKey();
                    for (String identifier : vertexLabel.getIdentifiers()) {
                        if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                            foreignKey.add(vertexLabel.getFullName(), identifier, Topology.IN_VERTEX_COLUMN_END);
                        }
                    }
                    result.add(foreignKey);
                }
            }
        }
        for (VertexLabel vertexLabel : this.getOutVertexLabels()) {
            if (!this.topology.isSchemaChanged() || !this.uncommittedRemovedOutEdgeRoles.contains(vertexLabel)) {
                if (vertexLabel.hasIDPrimaryKey()) {
                    result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
                } else {
                    ForeignKey foreignKey = new ForeignKey();
                    for (String identifier : vertexLabel.getIdentifiers()) {
                        if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                            foreignKey.add(vertexLabel.getFullName(), identifier, Topology.OUT_VERTEX_COLUMN_END);
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
        if (this.topology.isSchemaChanged()) {
            //noinspection Duplicates
            for (EdgeRole inEdgeRole : this.uncommittedInEdgeRoles) {
                VertexLabel vertexLabel = inEdgeRole.getVertexLabel();
                if (!this.uncommittedRemovedInEdgeRoles.contains(inEdgeRole)) {
                    if (vertexLabel.hasIDPrimaryKey()) {
                        result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
                    } else {
                        ForeignKey foreignKey = new ForeignKey();
                        for (String identifier : vertexLabel.getIdentifiers()) {
                            if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                                foreignKey.add(vertexLabel.getFullName(), identifier, Topology.IN_VERTEX_COLUMN_END);
                            }
                        }
                        result.add(foreignKey);
                    }
                }
            }
            for (EdgeRole outEdgeRole : this.uncommittedOutEdgeRoles) {
                VertexLabel vertexLabel = outEdgeRole.getVertexLabel();
                if (!this.uncommittedRemovedOutEdgeRoles.contains(outEdgeRole)) {
                    if (vertexLabel.hasIDPrimaryKey()) {
                        result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
                    } else {
                        ForeignKey foreignKey = new ForeignKey();
                        for (String identifier : vertexLabel.getIdentifiers()) {
                            if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                                foreignKey.add(vertexLabel.getFullName(), identifier, Topology.OUT_VERTEX_COLUMN_END);
                            }
                        }
                        result.add(foreignKey);
                    }
                }
            }
        }
        return result;
    }

    Set<ForeignKey> getUncommittedRemovedEdgeForeignKeys() {
        Set<ForeignKey> result = new HashSet<>();
        if (this.topology.isSchemaChanged()) {
            //noinspection Duplicates
            for (EdgeRole inEdgeRole : this.uncommittedRemovedInEdgeRoles) {
                VertexLabel vertexLabel = inEdgeRole.getVertexLabel();
                if (vertexLabel.hasIDPrimaryKey()) {
                    result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
                } else {
                    ForeignKey foreignKey = new ForeignKey();
                    for (String identifier : vertexLabel.getIdentifiers()) {
                        if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                            foreignKey.add(vertexLabel.getFullName(), identifier, Topology.IN_VERTEX_COLUMN_END);
                        }
                    }
                    result.add(foreignKey);
                }
            }
            for (EdgeRole outEdgeRole : this.uncommittedRemovedOutEdgeRoles) {
                VertexLabel vertexLabel = outEdgeRole.getVertexLabel();
                if (vertexLabel.hasIDPrimaryKey()) {
                    result.add(ForeignKey.of(vertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
                } else {
                    ForeignKey foreignKey = new ForeignKey();
                    for (String identifier : vertexLabel.getIdentifiers()) {
                        if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                            foreignKey.add(vertexLabel.getFullName(), identifier, Topology.OUT_VERTEX_COLUMN_END);
                        }
                    }
                    result.add(foreignKey);
                }
            }
        }
        return result;
    }

    boolean isValid() {
        return !this.outEdgeRoles.isEmpty() || !this.uncommittedOutEdgeRoles.isEmpty();
    }

    public Set<VertexLabel> getOutVertexLabels() {
        Set<VertexLabel> result = new HashSet<>();
        Set<EdgeRole> outEdgeRoles = getOutEdgeRoles();
        for (EdgeRole outEdgeRole : outEdgeRoles) {
            result.add(outEdgeRole.getVertexLabel());
        }
        return Collections.unmodifiableSet(result);
    }

    public Set<EdgeRole> getOutEdgeRoles() {
        Set<EdgeRole> result = new HashSet<>(this.outEdgeRoles);
        if (this.topology.isSchemaChanged() && isValid()) {
            result.addAll(this.uncommittedOutEdgeRoles);
            result.removeAll(this.uncommittedRemovedOutEdgeRoles);
        }
        return Collections.unmodifiableSet(result);
    }

    public EdgeRole getOutEdgeRoles(VertexLabel vertexLabel) {
        //Optimized to not call getOutEdgeRoles()
        for (EdgeRole outEdgeRole : this.outEdgeRoles) {
            if (outEdgeRole.getVertexLabel().equals(vertexLabel)) {
                return outEdgeRole;
            }
        }
        Set<EdgeRole> result = new HashSet<>(this.uncommittedOutEdgeRoles);
        result.removeAll(this.uncommittedRemovedOutEdgeRoles);
        for (EdgeRole uncommittedOutEdgeRole : result) {
            if (uncommittedOutEdgeRole.getVertexLabel().equals(vertexLabel)) {
                return uncommittedOutEdgeRole;
            }
        }
        return null;
    }

    public EdgeRole getInEdgeRoles(VertexLabel vertexLabel) {
        //Optimized to not call getInEdgeRoles()
        for (EdgeRole inEdgeRole : this.inEdgeRoles) {
            if (inEdgeRole.getVertexLabel().equals(vertexLabel)) {
                return inEdgeRole;
            }
        }
        Set<EdgeRole> result = new HashSet<>(this.uncommittedInEdgeRoles);
        result.removeAll(this.uncommittedRemovedInEdgeRoles);
        for (EdgeRole uncommittedInEdgeRole : result) {
            if (uncommittedInEdgeRole.getVertexLabel().equals(vertexLabel)) {
                return uncommittedInEdgeRole;
            }
        }
        return null;
    }

    public Set<VertexLabel> getInVertexLabels() {
        Set<VertexLabel> result = new HashSet<>();
        for (EdgeRole inEdgeRole : getInEdgeRoles()) {
            result.add(inEdgeRole.getVertexLabel());
        }
        return Collections.unmodifiableSet(result);
    }

    public Set<EdgeRole> getInEdgeRoles() {
        Set<EdgeRole> result = new HashSet<>(this.inEdgeRoles);
        if (this.topology.isSchemaChanged() && isValid()) {
            result.addAll(this.uncommittedInEdgeRoles);
            result.removeAll(this.uncommittedRemovedInEdgeRoles);
        }
        return Collections.unmodifiableSet(result);
    }

    void ensureEdgeVertexLabelExist(Direction direction, VertexLabel vertexLabel, EdgeDefinition edgeDefinition) {
        //if the direction is OUT then the vertexLabel must be in the same schema as the edgeLabel (this)
        if (direction == Direction.OUT) {
            Preconditions.checkState(vertexLabel.getSchema().equals(getSchema()), "For Direction.OUT the VertexLabel must be in the same schema as the edge. Found %s and %s", vertexLabel.getSchema().getName(), getSchema().getName());
        }
        if (!foreignKeysContains(direction, vertexLabel, edgeDefinition)) {
            //Make sure the current thread/transaction owns the lock
            Schema schema = this.getSchema();
            schema.getTopology().startSchemaChange(
                    String.format("EdgeLabel '%s' ensureEdgeVertexLabelExist with '%s', '%s'", getFullName(), direction.name(), vertexLabel.getName())
            );
            if (!foreignKeysContains(direction, vertexLabel, edgeDefinition)) {
                SchemaTable foreignKeySchemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), vertexLabel.getLabel());
                TopologyManager.addLabelToEdge(
                        this.sqlgGraph,
                        this.getSchema().getName(),
                        EDGE_PREFIX + getLabel(),
                        direction == Direction.IN,
                        foreignKeySchemaTable,
                        edgeDefinition
                );
                if (direction == Direction.IN) {
                    EdgeRole edgeRole = new EdgeRole(vertexLabel, this, Direction.IN, false, edgeDefinition.inMultiplicity());
                    this.uncommittedInEdgeRoles.add(edgeRole);
                    vertexLabel.addToUncommittedInEdgeRoles(schema, edgeRole);
                } else {
                    EdgeRole edgeRole = new EdgeRole(vertexLabel, this, Direction.OUT, false, edgeDefinition.outMultiplicity());
                    this.uncommittedOutEdgeRoles.add(edgeRole);
                    vertexLabel.addToUncommittedOutEdgeRoles(schema, edgeRole);
                }
                //addEdgeForeignKey is not creating foreignKeys for user supplied ids.
                //TODO investigate user supplied id foreignKeys
                SchemaTable foreignKey = SchemaTable.of(vertexLabel.getSchema().getName(), vertexLabel.getLabel() + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END));
                addEdgeForeignKey(schema.getName(), EDGE_PREFIX + getLabel(), vertexLabel, direction, foreignKey);
                this.getSchema().getTopology().fire(this, vertexLabel, TopologyChangeAction.ADD_IN_VERTEX_LABEL_TO_EDGE, true);
            }
        }
    }

    private void addEdgeForeignKey(String schema, String table, VertexLabel foreignVertexLabel, Direction direction, SchemaTable foreignKey) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "BUG: ensureEdgeVertexLabelExist may not be called for %s", SQLG_SCHEMA);
        List<String> addEdgeSqls = new ArrayList<>();
        if (foreignVertexLabel.hasIDPrimaryKey()) {
            addEdgeSqls.add(
                    this.sqlgGraph.getSqlDialect().addColumnStatement(
                            schema,
                            table,
                            foreignVertexLabel.getSchema().getName() + "." + foreignVertexLabel.getLabel() + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END),
                            this.sqlgGraph.getSqlDialect().getForeignKeyTypeDefinition()
                    )
            );
        } else {
            for (String identifier : foreignVertexLabel.getIdentifiers()) {
                PropertyColumn propertyColumn = foreignVertexLabel.getProperty(identifier).orElseThrow(
                        () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                );
                PropertyType propertyType = propertyColumn.getPropertyType();
                String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                int count = 1;
                for (String foreignKeyType : propertyTypeToSqlDefinition) {
                    if (count > 1) {
                        addEdgeSqls.add(
                                this.sqlgGraph.getSqlDialect().addColumnStatement(
                                        schema,
                                        table,
                                        foreignVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END),
                                        foreignKeyType
                                )
                        );
                    } else {
                        //The first column existVertexLabel no postfix
                        addEdgeSqls.add(
                                this.sqlgGraph.getSqlDialect().addColumnStatement(
                                        schema,
                                        table,
                                        foreignVertexLabel.getFullName() + "." + identifier + (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END),
                                        foreignKeyType
                                )
                        );
                    }
                    count++;
                }
            }

        }
        if (LOGGER.isDebugEnabled()) {
            for (String addEdgeSql : addEdgeSqls) {
                LOGGER.debug(addEdgeSql);
            }
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        for (String addEdgeSql : addEdgeSqls) {
            try (PreparedStatement preparedStatement = conn.prepareStatement(addEdgeSql)) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        StringBuilder sql = new StringBuilder();
        //foreign key definition start
        if (foreignVertexLabel.hasIDPrimaryKey() && this.sqlgGraph.getTopology().isImplementingForeignKeys()) {
            sql.append(" ALTER TABLE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sql.append(" ADD CONSTRAINT ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table + "_" + foreignKey.getSchema() + "." + foreignKey.getTable() + "_fkey"));
            sql.append(" FOREIGN KEY (");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
            sql.append(") REFERENCES ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignVertexLabel.getSchema().getName()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + foreignVertexLabel.getLabel()));
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql.toString());
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
            if (foreignVertexLabel.hasIDPrimaryKey()) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
            } else {
                int countIdentifier = 1;
                for (String identifier : foreignVertexLabel.getIdentifiers()) {
                    PropertyColumn propertyColumn = foreignVertexLabel.getProperty(identifier).orElseThrow(
                            () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                    );
                    PropertyType propertyType = propertyColumn.getPropertyType();
                    String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    for (String ignore : propertyTypeToSqlDefinition) {
                        if (count > 1) {
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    foreignVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END))
                            );
                        } else {
                            //The first column existVertexLabel no postfix
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                    foreignVertexLabel.getFullName() + "." + identifier + (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END))
                            );
                        }
                        count++;
                    }
                    if (countIdentifier++ < foreignVertexLabel.getIdentifiers().size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(")");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void addToOutEdgeRole(EdgeRole outEdgeRole) {
        this.outEdgeRoles.add(outEdgeRole);
    }

    void addToInEdgeRole(EdgeRole inEdgeRole) {
        this.inEdgeRoles.add(inEdgeRole);
    }

    @Override
    public int hashCode() {
        //Edges are unique per out vertex schemas.
        //An edge must have at least one out vertex so take it to get the schema.
        VertexLabel vertexLabel;
        if (!this.outEdgeRoles.isEmpty()) {
            vertexLabel = this.outEdgeRoles.iterator().next().getVertexLabel();
        } else {
            vertexLabel = this.uncommittedOutEdgeRoles.iterator().next().getVertexLabel();
        }
        return (vertexLabel.getSchema().getName() + this.getLabel()).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        if (!(other instanceof EdgeLabel otherEdgeLabel)) {
            return false;
        }
        if (isValid()) {
            if (this.topology.isSchemaChanged() && !this.uncommittedOutEdgeRoles.isEmpty()) {
                VertexLabel vertexLabel = this.uncommittedOutEdgeRoles.iterator().next().getVertexLabel();
                VertexLabel otherVertexLabel = otherEdgeLabel.uncommittedOutEdgeRoles.iterator().next().getVertexLabel();
                return vertexLabel.getSchema().equals(otherVertexLabel.getSchema()) && otherEdgeLabel.getLabel().equals(this.getLabel());
            } else {
                VertexLabel vertexLabel = this.outEdgeRoles.iterator().next().getVertexLabel();
                VertexLabel otherVertexLabel = otherEdgeLabel.outEdgeRoles.iterator().next().getVertexLabel();
                return vertexLabel.getSchema().equals(otherVertexLabel.getSchema()) && otherEdgeLabel.getLabel().equals(this.getLabel());
            }
        }
        return otherEdgeLabel.getLabel().equals(this.getLabel());
    }

//    boolean deepEquals(EdgeLabel otherEdgeLabel) {
//        Preconditions.checkState(this.equals(otherEdgeLabel), "equals must have passed before calling deepEquals");
//
//        //check every out and in edge
//        for (VertexLabel outVertexLabel : this.outEdgeRoles) {
//            boolean ok = false;
//            for (VertexLabel otherOutVertexLabel : otherEdgeLabel.outEdgeRoles) {
//                if (outVertexLabel.equals(otherOutVertexLabel)) {
//                    ok = true;
//                    break;
//                }
//            }
//            if (!ok) {
//                return false;
//            }
//        }
//        for (VertexLabel inVertexLabel : this.inEdgeRoles) {
//            boolean ok = false;
//            for (VertexLabel otherInVertexLabel : otherEdgeLabel.inEdgeRoles) {
//                if (inVertexLabel.equals(otherInVertexLabel)) {
//                    ok = true;
//                    break;
//                }
//            }
//            if (!ok) {
//                return false;
//            }
//        }
//        return true;
//    }

    @Override
    protected JsonNode toJson() {
        ObjectNode edgeLabelNode = Topology.OBJECT_MAPPER.createObjectNode();
        if (isValid()) {
            edgeLabelNode.put("schema", getSchema().getName());
        }
        edgeLabelNode.put("label", getLabel());
        edgeLabelNode.set("properties", super.toJson());

        ArrayNode outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        SortedSet<VertexLabel> vertexLabels = new TreeSet<>(Comparator.comparing(AbstractLabel::getName));
        vertexLabels.addAll(this.outEdgeRoles.stream().map(EdgeRole::getVertexLabel).collect(Collectors.toSet()));
        for (VertexLabel outVertexLabel : vertexLabels) {
            ObjectNode outVertexLabelObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
            outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
            outVertexLabelArrayNode.add(outVertexLabelObjectNode);
        }
        edgeLabelNode.set("outVertexLabels", outVertexLabelArrayNode);

        ArrayNode inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        vertexLabels = new TreeSet<>(Comparator.comparing(AbstractLabel::getName));
        vertexLabels.addAll(this.inEdgeRoles.stream().map(EdgeRole::getVertexLabel).collect(Collectors.toSet()));
        for (VertexLabel inVertexLabel : vertexLabels) {
            ObjectNode inVertexLabelObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
            inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
            inVertexLabelArrayNode.add(inVertexLabelObjectNode);
        }
        edgeLabelNode.set("inVertexLabels", inVertexLabelArrayNode);

        if (this.topology.isSchemaChanged() && isValid()) {
            outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            vertexLabels = new TreeSet<>(Comparator.comparing(AbstractLabel::getName));
            vertexLabels.addAll(this.uncommittedOutEdgeRoles.stream().map(EdgeRole::getVertexLabel).collect(Collectors.toSet()));
            for (VertexLabel outVertexLabel : vertexLabels) {
                ObjectNode outVertexLabelObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
                outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
                outVertexLabelArrayNode.add(outVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedOutVertexLabels", outVertexLabelArrayNode);

            inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            vertexLabels = new TreeSet<>(Comparator.comparing(AbstractLabel::getName));
            vertexLabels.addAll(this.uncommittedInEdgeRoles.stream().map(EdgeRole::getVertexLabel).collect(Collectors.toSet()));
            for (VertexLabel inVertexLabel : vertexLabels) {
                ObjectNode inVertexLabelObjectNode = Topology.OBJECT_MAPPER.createObjectNode();
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
        ObjectNode edgeLabelNode = Topology.OBJECT_MAPPER.createObjectNode();
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

        if (this.topology.isSchemaChanged() && !this.uncommittedOutEdgeRoles.isEmpty()) {
            foundSomething = true;
        }

//        if (this.topology.isSchemaChanged() && !this.uncommittedRemovedOutEdgeRoles.isEmpty()) {
//            foundSomething = true;
//        }

        if (this.topology.isSchemaChanged() && !this.uncommittedInEdgeRoles.isEmpty()) {
            foundSomething = true;
        }

//        if (this.topology.isSchemaChanged() && !this.uncommittedRemovedInEdgeRoles.isEmpty()) {
//            foundSomething = true;
//        }

        if (foundSomething) {
            return Optional.of(edgeLabelNode);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) {
        List<Topology.TopologyValidationError> validationErrors = new ArrayList<>();
        for (PropertyColumn propertyColumn : getProperties().values()) {
            List<Triple<String, Integer, String>> columns = this.sqlgGraph.getSqlDialect().getTableColumns(metadata, null, this.getSchema().getName(), "E_" + this.getLabel(), propertyColumn.getName());
            if (columns.isEmpty()) {
                validationErrors.add(new Topology.TopologyValidationError(propertyColumn));
            }
        }
        return validationErrors;
    }

    @Override
    public String getPrefix() {
        return EDGE_PREFIX;
    }

    @Override
    void removeProperty(PropertyColumn propertyColumn, boolean preserveData) {
        this.getSchema().getTopology().startSchemaChange(
                String.format("EdgeLabel '%s' removeProperty with '%s'", getFullName(), propertyColumn.getName())
        );
        if (!uncommittedRemovedProperties.contains(propertyColumn.getName())) {
            uncommittedRemovedProperties.add(propertyColumn.getName());
            TopologyManager.removeEdgeColumn(this.sqlgGraph, this.getSchema().getName(), EDGE_PREFIX + getLabel(), propertyColumn.getName());
            if (!preserveData) {
                removeColumn(this.getSchema().getName(), EDGE_PREFIX + getLabel(), propertyColumn.getName());
            }
            this.getSchema().getTopology().fire(propertyColumn, propertyColumn, TopologyChangeAction.DELETE, true);
        }
    }

    @Override
    void updatePropertyDefinition(PropertyColumn propertyColumn, PropertyDefinition propertyDefinition) {
        Preconditions.checkState(!sqlgGraph.getSqlDialect().isMariaDb(), "updatePropertyDefinition is not supported for mariadb. Dropping constraints is complicated so will only do if if requested.");
        PropertyDefinition currentPropertyDefinition = propertyColumn.getPropertyDefinition();
        Preconditions.checkState(currentPropertyDefinition.propertyType().equals(propertyDefinition.propertyType()),
                "PropertyType must be the same for updatePropertyDefinition. Original: '%s', Updated: '%s' '%s'", currentPropertyDefinition.propertyType(), propertyDefinition.propertyType()
        );
        this.getSchema().getTopology().startSchemaChange(
                String.format("EdgeLabel '%s' updatePropertyDefinition with '%s' '%s'", getFullName(), propertyColumn.getName(), propertyDefinition.toString())
        );
        String name = propertyColumn.getName();
        if (!this.uncommittedUpdatedProperties.containsKey(name)) {
            PropertyColumn copy = new PropertyColumn(this, name, propertyDefinition);
            this.uncommittedUpdatedProperties.put(name, copy);
            TopologyManager.updateEdgeLabelPropertyColumn(
                    this.sqlgGraph,
                    getSchema().getName(),
                    EDGE_PREFIX + getLabel(),
                    name,
                    propertyDefinition
            );
            internalUpdatePropertyDefinition(propertyColumn, propertyDefinition, currentPropertyDefinition, name, copy);
        }
    }

    @Override
    void renameProperty(String name, PropertyColumn propertyColumn) {
        this.getSchema().getTopology().startSchemaChange(
                String.format("EdgeLabel '%s' renameProperty with '%s' '%s'", getFullName(), name, propertyColumn.getName())
        );
        String oldName = propertyColumn.getName();
        Pair<String, String> namePair = Pair.of(oldName, name);
        if (!this.uncommittedRemovedProperties.contains(name)) {
            this.uncommittedRemovedProperties.add(oldName);
            PropertyColumn copy = new PropertyColumn(this, name, propertyColumn.getPropertyDefinition());
            this.uncommittedProperties.put(name, copy);
            TopologyManager.renameEdgeLabelPropertyColumn(this.sqlgGraph, getSchema().getName(), EDGE_PREFIX + getLabel(), oldName, name);
            renameColumn(getSchema().getName(), EDGE_PREFIX + getLabel(), oldName, name);
            if (this.getIdentifiers().contains(oldName)) {
                Preconditions.checkState(!this.renamedIdentifiers.contains(namePair), "BUG! renamedIdentifiers may not yet contain '%s'", oldName);
                this.renamedIdentifiers.add(namePair);
            }
            this.getSchema().getTopology().fire(copy, copy, TopologyChangeAction.DELETE, true);
            this.getSchema().getTopology().fire(propertyColumn, copy, TopologyChangeAction.CREATE, true);
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
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
    void removeOutVertexLabel(VertexLabel lbl, boolean dropEdges, boolean preserveData) {
        if (dropEdges) {
            this.sqlgGraph.traversal().V().hasLabel(lbl.getSchema().getName() + "." + lbl.getLabel())
                    .outE(getLabel())
                    .drop()
                    .iterate();
        }
        List<EdgeRole> edgeRolesToRemove = getOutEdgeRoles().stream().filter(er -> er.getVertexLabel().equals(lbl) && er.getEdgeLabel().equals(this)).toList();
        Preconditions.checkState(edgeRolesToRemove.size() == 1);
        this.uncommittedRemovedOutEdgeRoles.add(edgeRolesToRemove.get(0));
        TopologyManager.removeOutEdgeRole(this.sqlgGraph, this, lbl);
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
    void removeInVertexLabel(VertexLabel lbl, boolean dropEdges, boolean preserveData) {
        //If this code executes via a VertexLabel.remove or Schema.remove there is no need to drop the edges and
        // dropping the VertexLabel will already drop the edges.
        if (dropEdges) {
            this.sqlgGraph.traversal().V().hasLabel(lbl.getSchema().getName() + "." + lbl.getLabel())
                    .inE(getLabel())
                    .drop()
                    .iterate();
        }
        List<EdgeRole> edgeRolesToRemove = getInEdgeRoles().stream().filter(er -> er.getVertexLabel().equals(lbl) && er.getEdgeLabel().equals(this)).toList();
        Preconditions.checkState(edgeRolesToRemove.size() == 1);
        this.uncommittedRemovedInEdgeRoles.add(edgeRolesToRemove.get(0));
        TopologyManager.removeInEdgeRole(this.sqlgGraph, this, lbl);
        if (!preserveData) {
            deleteColumn(lbl.getFullName() + Topology.IN_VERTEX_COLUMN_END);
        }
    }

    public void ensureDistributed(int shardCount, PropertyColumn distributionPropertyColumn) {
        ensureDistributed(shardCount, distributionPropertyColumn, getOutVertexLabels().iterator().next());
    }

    EdgeLabel readOnlyCopy(Topology topology, Schema foreignSchema, Set<Schema> foreignSchemas) {
        EdgeLabel copy = new EdgeLabel(topology, this.label, true);
        for (EdgeRole outEdgeRole : this.outEdgeRoles) {
            Optional<VertexLabel> foreignOutVertexLabelOptional = foreignSchema.getVertexLabel(outEdgeRole.getVertexLabel().getLabel());
            Preconditions.checkState(foreignOutVertexLabelOptional.isPresent());
            VertexLabel foreignOutVertexLabel = foreignOutVertexLabelOptional.get();

            EdgeRole copyEdgeRole = new EdgeRole(foreignOutVertexLabel, copy, Direction.OUT, true, outEdgeRole.getMultiplicity());
            copy.outEdgeRoles.add(copyEdgeRole);
            foreignOutVertexLabel.outEdgeRoles.put(copy.getFullName(), copyEdgeRole);
        }
        for (EdgeRole inEdgeRole : this.inEdgeRoles) {
            Optional<VertexLabel> foreignInVertexLabelOptional = foreignSchemas.stream()
                    .filter(s -> s.getName().equals(inEdgeRole.getVertexLabel().getSchema().getName()) && s.getVertexLabel(inEdgeRole.getVertexLabel().getLabel()).isPresent())
                    .map(s -> s.getVertexLabel(inEdgeRole.getVertexLabel().getLabel()).orElseThrow())
                    .findAny();
            Preconditions.checkState(foreignInVertexLabelOptional.isPresent());
            VertexLabel foreignInVertexLabel = foreignInVertexLabelOptional.get();

            EdgeRole copyEdgeRole = new EdgeRole(foreignInVertexLabel, copy, Direction.IN, true, inEdgeRole.getMultiplicity());
            copy.inEdgeRoles.add(copyEdgeRole);
            foreignInVertexLabel.inEdgeRoles.put(copy.getFullName(), copyEdgeRole);
        }
        for (String property : this.properties.keySet()) {
            copy.properties.put(property, this.properties.get(property).readOnlyCopy(copy));
        }
        copy.identifiers.addAll(this.identifiers);
        return copy;
    }

    void renameOutForeignKeyIdentifier(String oldName, String newName, VertexLabel oldVertexLabel) {
        renameInOutForeignKeyIdentifier(oldName, newName, oldVertexLabel, Direction.OUT);
    }

    void renameInForeignKeyIdentifier(String oldName, String newName, VertexLabel oldVertexLabel) {
        renameInOutForeignKeyIdentifier(oldName, newName, oldVertexLabel, Direction.IN);
    }

    private void renameInOutForeignKeyIdentifier(String oldName, String newName, VertexLabel oldVertexLabel, Direction direction) {
        Preconditions.checkState(!oldVertexLabel.hasIDPrimaryKey());
        Optional<String> newIdentifierOptional = oldVertexLabel.getIdentifiers().stream().filter(i -> i.equals(newName)).findAny();
        Preconditions.checkState(newIdentifierOptional.isPresent());
        Preconditions.checkState(oldVertexLabel.renamedIdentifiers.stream().anyMatch(p -> p.getLeft().equals(oldName)));
        renameColumn(
                getSchema().getName(),
                EDGE_PREFIX + getLabel(),
                oldVertexLabel.getFullName() + "." + oldName + (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : IN_VERTEX_COLUMN_END),
                oldVertexLabel.getFullName() + "." + newName + (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : IN_VERTEX_COLUMN_END)
        );
    }

    void renameOutVertexLabel(VertexLabel renamedVertexLabel, VertexLabel oldVertexLabel) {
        this.uncommittedRemovedOutEdgeRoles.addAll(oldVertexLabel.getOutEdgeRoles().values());
        List<EdgeRole> edgeRoles = this.outEdgeRoles.stream().filter(
                edgeRole -> edgeRole.getVertexLabel().equals(oldVertexLabel) && edgeRole.getEdgeLabel().equals(this)
        ).toList();
        Preconditions.checkState(edgeRoles.size() == 1);
        EdgeRole edgeRole = edgeRoles.get(0);

        EdgeRole renamedEdgeRole = new EdgeRole(renamedVertexLabel, this, Direction.OUT, false, edgeRoles.get(0).getMultiplicity());
        this.uncommittedOutEdgeRoles.add(renamedEdgeRole);
        renamedVertexLabel.addToUncommittedOutEdgeRoles(renamedVertexLabel.getSchema(), renamedEdgeRole);
        if (oldVertexLabel.hasIDPrimaryKey()) {
            renameColumn(
                    getSchema().getName(),
                    EDGE_PREFIX + getLabel(),
                    oldVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END,
                    renamedVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END
            );
        } else {
            for (String identifier : oldVertexLabel.getIdentifiers()) {
                renameColumn(
                        getSchema().getName(),
                        EDGE_PREFIX + getLabel(),
                        oldVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END,
                        renamedVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END
                );
            }
        }
    }

    void renameInVertexLabel(VertexLabel renamedVertexLabel, VertexLabel oldVertexLabel) {
        List<EdgeRole> oldEdgeRoles = oldVertexLabel.getInEdgeRoles().values().stream().filter(edgeRole -> edgeRole.getEdgeLabel().equals(this)).toList();
        Preconditions.checkState(oldEdgeRoles.size() == 1);
        EdgeRole oldEdgeRole = oldEdgeRoles.get(0);
        this.uncommittedRemovedInEdgeRoles.add(oldEdgeRole);
        EdgeRole renamedEdgeRole = new EdgeRole(renamedVertexLabel, this, Direction.IN, false, oldEdgeRoles.get(0).getMultiplicity());
        this.uncommittedInEdgeRoles.add(renamedEdgeRole);
        renamedVertexLabel.addToUncommittedInEdgeRoles(renamedVertexLabel.getSchema(), renamedEdgeRole);
        if (oldVertexLabel.hasIDPrimaryKey()) {
            renameColumn(
                    getSchema().getName(),
                    EDGE_PREFIX + getLabel(),
                    oldVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END,
                    renamedVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END
            );
        } else {
            for (String identifier : oldVertexLabel.getIdentifiers()) {
                renameColumn(
                        getSchema().getName(),
                        EDGE_PREFIX + getLabel(),
                        oldVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END,
                        renamedVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END
                );
            }
        }
    }

    @Override
    public void rename(String label) {
        Objects.requireNonNull(label, "Given label must not be null");
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not be prefixed with \"%s\"", EDGE_PREFIX);
        Preconditions.checkState(!this.isForeignAbstractLabel, "'%s' is a read only foreign table!", label);
        this.getSchema().getTopology().startSchemaChange(
                String.format("EdgeLabel '%s' rename with '%s'", getFullName(), label)
        );
        this.getSchema().renameEdgeLabel(this, label);
    }

    static EdgeLabel renameEdgeLabel(
            SqlgGraph sqlgGraph,
            Schema schema,
            EdgeLabel oldEdgeLabel,
            String newLabel,
            Set<EdgeRole> oldOutEdgeRoles,
            Set<EdgeRole> oldInEdgeRoles,
            Map<String, PropertyDefinition> properties,
            ListOrderedSet<String> identifiers) {

        Preconditions.checkArgument(!schema.isSqlgSchema(), "renameEdgeLabel may not be called for \"%s\"", SQLG_SCHEMA);
        EdgeLabel edgeLabel = new EdgeLabel(
                sqlgGraph,
                newLabel,
                properties,
                identifiers
        );
        for (EdgeRole oldOutEdgeRole : oldOutEdgeRoles) {
            EdgeRole newOutEdgeRole = new EdgeRole(oldOutEdgeRole.getVertexLabel(), edgeLabel, oldOutEdgeRole.getDirection(), false, oldOutEdgeRole.getMultiplicity());
            edgeLabel.uncommittedOutEdgeRoles.add(newOutEdgeRole);
            newOutEdgeRole.getVertexLabel().addToUncommittedOutEdgeRoles(edgeLabel.getSchema(), newOutEdgeRole);
            newOutEdgeRole.getVertexLabel().removeOutEdge(oldEdgeLabel);
            schema.getTopology().fire(oldOutEdgeRole, oldOutEdgeRole, TopologyChangeAction.DELETE, true);
            schema.getTopology().fire(oldOutEdgeRole, newOutEdgeRole, TopologyChangeAction.CREATE, true);
        }
        for (EdgeRole oldInEdgeRole : oldInEdgeRoles) {
            EdgeRole newInEdgeRole = new EdgeRole(oldInEdgeRole.getVertexLabel(), edgeLabel, oldInEdgeRole.getDirection(), false, oldInEdgeRole.getMultiplicity());
            edgeLabel.uncommittedInEdgeRoles.add(newInEdgeRole);
            newInEdgeRole.getVertexLabel().addToUncommittedInEdgeRoles(edgeLabel.getSchema(), newInEdgeRole);
            newInEdgeRole.getVertexLabel().removeInEdge(oldEdgeLabel);
            schema.getTopology().fire(oldInEdgeRole, oldInEdgeRole, TopologyChangeAction.DELETE, true);
            schema.getTopology().fire(oldInEdgeRole, newInEdgeRole, TopologyChangeAction.CREATE, true);
        }
        edgeLabel.renameEdgeLabelOnDb(oldEdgeLabel.getLabel(), newLabel);
        TopologyManager.renameEdgeLabel(sqlgGraph, schema.getName(), EDGE_PREFIX + oldEdgeLabel.getLabel(), EDGE_PREFIX + newLabel);
        edgeLabel.committed = false;
        schema.getTopology().fire(oldEdgeLabel, oldEdgeLabel, TopologyChangeAction.DELETE, true);
        schema.getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE, true);
        return edgeLabel;

    }

    private void renameEdgeLabelOnDb(String oldLabel, String newLabel) {
        String sql = this.sqlgGraph.getSqlDialect().renameTable(
                getSchema().getName(),
                EDGE_PREFIX + oldLabel,
                EDGE_PREFIX + newLabel
        );
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

}
