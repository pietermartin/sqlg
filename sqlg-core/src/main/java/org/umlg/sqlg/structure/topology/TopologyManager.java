package org.umlg.sqlg.structure.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.structure.BatchManager;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Created by pieter on 2015/12/08.
 */
public class TopologyManager {


    private TopologyManager() {
    }

    public static void addGraph(SqlgGraph sqlgGraph, String version) {
        Connection conn = sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            LocalDateTime now = LocalDateTime.now();
            sqlgGraph.addVertex(
                    T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_GRAPH,
                    SQLG_SCHEMA_GRAPH_VERSION, version,
                    SQLG_SCHEMA_GRAPH_DB_VERSION, metadata.getDatabaseProductVersion(),
                    CREATED_ON, now,
                    UPDATED_ON, now
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Updates sqlg_schema.V_graph's version to the new version and returns the old version.
     *
     * @param sqlgGraph The graph.
     * @param version   The new version.
     * @return The old version.
     */
    public static String updateGraph(SqlgGraph sqlgGraph, String version) {
        Connection conn = sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> graphVertices = traversalSource.V().hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_GRAPH).toList();
            Preconditions.checkState(graphVertices.size() == 1, "BUG: There can only ever be one graph vertex, found %s", graphVertices.size());
            Vertex graph = graphVertices.get(0);
            String oldVersion = graph.value(SQLG_SCHEMA_GRAPH_VERSION);
            if (!oldVersion.equals(version)) {
                graph.property(SQLG_SCHEMA_GRAPH_VERSION, version);
                graph.property(SQLG_SCHEMA_GRAPH_DB_VERSION, metadata.getDatabaseProductVersion());
                graph.property(UPDATED_ON, LocalDateTime.now());
            }
            return oldVersion;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void addSchema(SqlgGraph sqlgGraph, String schema) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            sqlgGraph.addVertex(
                    T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                    "name", schema,
                    CREATED_ON, LocalDateTime.now()
            );
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void removeSchema(SqlgGraph sqlgGraph, String schema) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();

            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            if (schemas.size() > 0) {
                Vertex vs = schemas.get(0);
                traversalSource.V(vs)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
                        .drop().iterate();

                traversalSource.V(vs)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_VERTEX_INDEX_EDGE)
                        .drop().iterate();

                traversalSource.V(vs)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
                        .drop().iterate();

                traversalSource.V(vs)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                        .drop().iterate();
                traversalSource.V(vs)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .out(SQLG_SCHEMA_EDGE_INDEX_EDGE)
                        .drop().iterate();
                traversalSource.V(vs)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .drop().iterate();
                traversalSource.V(vs)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .drop().iterate();

                traversalSource.V(vs)
                        .drop().iterate();
            }

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addVertexLabel(SqlgGraph sqlgGraph, String schema, String tableName, Map<String, PropertyType> columns, ListOrderedSet<String> identifiers) {
        addVertexLabel(sqlgGraph, schema, tableName, columns, identifiers, PartitionType.NONE, null);
    }

    public static void addVertexLabel(
            SqlgGraph sqlgGraph,
            String schema,
            String tableName,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            //get the schema vertex
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema \"%s\" does not exist in Sqlg's topology. BUG!!!", schema);
            Preconditions.checkState(schemas.size() == 1, "Multiple \"%s\" found in Sqlg's topology. BUG!!!", schema);
            Preconditions.checkState(!tableName.startsWith(VERTEX_PREFIX));
            Vertex schemaVertex = schemas.get(0);

            sqlgGraph.tx().normalBatchModeOn();
            Vertex vertex;
            if (partitionExpression != null) {
                Preconditions.checkState(partitionType != PartitionType.NONE, "If the partitionExpression is not null then the PartitionType may not be NONE. Found %s", partitionType.name());
                vertex = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL,
                        SQLG_SCHEMA_VERTEX_LABEL_NAME, tableName,
                        SCHEMA_VERTEX_DISPLAY, schema + "." + VERTEX_PREFIX + tableName, //this is here for display when in pgadmin
                        Topology.CREATED_ON, LocalDateTime.now(),
                        Topology.SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE, partitionType.name(),
                        Topology.SQLG_SCHEMA_VERTEX_LABEL_PARTITION_EXPRESSION, partitionExpression);
            } else {
                Preconditions.checkState(partitionType == PartitionType.NONE, "If the partitionExpression is null then the PartitionType must be NONE. Found %s", partitionType.name());
                vertex = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL,
                        SQLG_SCHEMA_VERTEX_LABEL_NAME, tableName,
                        SCHEMA_VERTEX_DISPLAY, schema + "." + VERTEX_PREFIX + tableName, //this is here for display when in pgadmin
                        Topology.CREATED_ON, LocalDateTime.now(),
                        Topology.SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE, PartitionType.NONE.name());
            }
            schemaVertex.addEdge(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertex);
            for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {
                Vertex property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PROPERTY,
                        "name", columnEntry.getKey(),
                        "type", columnEntry.getValue().name(),
                        Topology.CREATED_ON, LocalDateTime.now()
                );
                vertex.addEdge(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, property);

                int i = 0;
                for (String identifier : identifiers) {
                    if (identifier.equals(columnEntry.getKey())) {
                        vertex.addEdge(SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE, property, SQLG_SCHEMA_VERTEX_IDENTIFIER_INDEX_EDGE, i);
                    }
                    i++;
                }

            }
            sqlgGraph.tx().flush();
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void renameVertexLabel(SqlgGraph sqlgGraph, String schema, String oldVertexLabel, String newVertexLabel) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(oldVertexLabel.startsWith(VERTEX_PREFIX), "prefixedTable must be for a vertex. prefixedTable = " + oldVertexLabel);
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertexLabelsToRename = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has(SQLG_SCHEMA_SCHEMA_NAME, schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(SQLG_SCHEMA_VERTEX_LABEL_NAME, oldVertexLabel.substring(VERTEX_PREFIX.length()))
                    .toList();
            Preconditions.checkState(vertexLabelsToRename.size() == 1, String.format("Expected exactly one VertexLabel in %s.%s. Found %d", schema, oldVertexLabel, vertexLabelsToRename.size()));
            vertexLabelsToRename.get(0).property(SQLG_SCHEMA_VERTEX_LABEL_NAME, newVertexLabel.substring(VERTEX_PREFIX.length()));
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void removeVertexLabel(SqlgGraph sqlgGraph, VertexLabel vertexLabel) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .has("name", vertexLabel.getSchema().getName())
                    .toList();
            if (schemas.size() > 0) {
                Vertex schemaVertex = schemas.get(0);
                List<Vertex> vertices = traversalSource.V(schemaVertex)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .has(SQLG_SCHEMA_VERTEX_LABEL_NAME, vertexLabel.getLabel())
                        .toList();
                if (vertices.size() > 0) {
                    Vertex vertex = vertices.get(0);
                    traversalSource.V(vertex)
                            .out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
                            .drop().iterate();
                    vertex.remove();
                }
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addVertexLabelPartition(
            SqlgGraph sqlgGraph,
            String schema,
            String abstractLabel,
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", abstractLabel)
                    .toList();
            if (vertices.size() == 0) {
                throw new IllegalStateException("Found no vertex for " + schema + "." + abstractLabel);
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException("Found more than one vertex for " + schema + "." + abstractLabel);
            }
            Vertex vertex = vertices.get(0);

            Vertex partition;
            if (partitionExpression != null) {
                Preconditions.checkState(!partitionType.isNone());
                partition = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_FROM, from,
                        SQLG_SCHEMA_PARTITION_TO, to,
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression,
                        CREATED_ON, LocalDateTime.now()
                );
            } else {
                Preconditions.checkState(partitionType.isNone());
                partition = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_FROM, from,
                        SQLG_SCHEMA_PARTITION_TO, to,
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        CREATED_ON, LocalDateTime.now()
                );
            }
            vertex.addEdge(SQLG_SCHEMA_VERTEX_PARTITION_EDGE, partition);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addVertexLabelPartition(
            SqlgGraph sqlgGraph,
            String schema,
            String abstractLabel,
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", abstractLabel)
                    .toList();
            if (vertices.size() == 0) {
                throw new IllegalStateException("Found no vertex for " + schema + "." + abstractLabel);
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException("Found more than one vertex for " + schema + "." + abstractLabel);
            }
            Vertex vertex = vertices.get(0);

            Vertex partition;
            if (partitionExpression != null) {
                Preconditions.checkState(!partitionType.isNone());
                partition = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_IN, in,
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression,
                        CREATED_ON, LocalDateTime.now()
                );
            } else {
                Preconditions.checkState(partitionType.isNone());
                partition = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_IN, in,
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        CREATED_ON, LocalDateTime.now()
                );
            }
            vertex.addEdge(SQLG_SCHEMA_VERTEX_PARTITION_EDGE, partition);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addVertexLabelPartition(
            SqlgGraph sqlgGraph,
            String schema,
            String abstractLabel,
            String name,
            Integer modulus,
            Integer remainder,
            PartitionType partitionType,
            String partitionExpression) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", abstractLabel)
                    .toList();
            if (vertices.size() == 0) {
                throw new IllegalStateException("Found no vertex for " + schema + "." + abstractLabel);
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException("Found more than one vertex for " + schema + "." + abstractLabel);
            }
            Vertex vertex = vertices.get(0);

            Vertex partition;
            if (partitionExpression != null) {
                Preconditions.checkState(!partitionType.isNone());
                partition = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_MODULUS, modulus,
                        SQLG_SCHEMA_PARTITION_REMAINDER, remainder,
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression,
                        CREATED_ON, LocalDateTime.now()
                );
            } else {
                Preconditions.checkState(partitionType.isNone());
                partition = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_MODULUS, modulus,
                        SQLG_SCHEMA_PARTITION_REMAINDER, remainder,
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        CREATED_ON, LocalDateTime.now()
                );
            }
            vertex.addEdge(SQLG_SCHEMA_VERTEX_PARTITION_EDGE, partition);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addEdgeLabelPartition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        addEdgeLabelPartition(
                sqlgGraph,
                abstractLabel.getSchema().getName(),
                abstractLabel.getName(),
                name,
                from,
                to,
                partitionType,
                partitionExpression);
    }

    public static void addEdgeLabelPartition(
            SqlgGraph sqlgGraph,
            String schema,
            String abstractLabel,
            String name,
            String from,
            String to,
            PartitionType partitionType,
            String partitionExpression) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .has("name", abstractLabel)
                    .toList();
            if (vertices.size() == 0) {
                throw new IllegalStateException("Found no vertex for " + schema + "." + abstractLabel);
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException("Found more than one vertex for " + schema + "." + abstractLabel);
            }
            Vertex vertex = vertices.get(0);

            Vertex property;
            if (partitionExpression != null) {
                Preconditions.checkState(!partitionType.isNone());
                property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_FROM, from,
                        SQLG_SCHEMA_PARTITION_TO, to,
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression,
                        CREATED_ON, LocalDateTime.now()
                );
            } else {
                Preconditions.checkState(partitionType.isNone());
                property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_FROM, from,
                        SQLG_SCHEMA_PARTITION_TO, to,
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        CREATED_ON, LocalDateTime.now()
                );
            }
            vertex.addEdge(SQLG_SCHEMA_EDGE_PARTITION_EDGE, property);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addEdgeLabelPartition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        addEdgeLabelPartition(
                sqlgGraph,
                abstractLabel.getSchema().getName(),
                abstractLabel.getName(),
                name,
                in,
                partitionType,
                partitionExpression
        );
    }

    public static void addEdgeLabelPartition(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            String name,
            Integer modulus,
            Integer remainder,
            PartitionType partitionType,
            String partitionExpression) {

        addEdgeLabelPartition(
                sqlgGraph,
                abstractLabel.getSchema().getName(),
                abstractLabel.getName(),
                name,
                modulus,
                remainder,
                partitionType,
                partitionExpression
        );
    }

    public static void addEdgeLabelPartition(
            SqlgGraph sqlgGraph,
            String schema,
            String abstractLabel,
            String name,
            String in,
            PartitionType partitionType,
            String partitionExpression) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .has("name", abstractLabel)
                    .toList();
            if (vertices.size() == 0) {
                throw new IllegalStateException("Found no vertex for " + schema + "." + abstractLabel);
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException("Found more than one vertex for " + schema + "." + abstractLabel);
            }
            Vertex vertex = vertices.get(0);

            Vertex property;
            if (partitionExpression != null) {
                Preconditions.checkState(!partitionType.isNone());
                property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_IN, in,
                        CREATED_ON, LocalDateTime.now(),
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression
                );
            } else {
                Preconditions.checkState(partitionType.isNone());
                property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_IN, in,
                        CREATED_ON, LocalDateTime.now(),
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name()
                );
            }
            vertex.addEdge(SQLG_SCHEMA_EDGE_PARTITION_EDGE, property);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    public static void addEdgeLabelPartition(
            SqlgGraph sqlgGraph,
            String schema,
            String abstractLabel,
            String name,
            Integer modulus,
            Integer remainder,
            PartitionType partitionType,
            String partitionExpression) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .has("name", abstractLabel)
                    .toList();
            if (vertices.size() == 0) {
                throw new IllegalStateException("Found no vertex for " + schema + "." + abstractLabel);
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException("Found more than one vertex for " + schema + "." + abstractLabel);
            }
            Vertex vertex = vertices.get(0);

            Vertex property;
            if (partitionExpression != null) {
                Preconditions.checkState(!partitionType.isNone());
                property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_MODULUS, modulus,
                        SQLG_SCHEMA_PARTITION_REMAINDER, remainder,
                        CREATED_ON, LocalDateTime.now(),
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                        SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression
                );
            } else {
                Preconditions.checkState(partitionType.isNone());
                property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                        SQLG_SCHEMA_PARTITION_NAME, name,
                        SQLG_SCHEMA_PARTITION_MODULUS, modulus,
                        SQLG_SCHEMA_PARTITION_REMAINDER, remainder,
                        CREATED_ON, LocalDateTime.now(),
                        SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name()
                );
            }
            vertex.addEdge(SQLG_SCHEMA_EDGE_PARTITION_EDGE, property);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    static void addEdgeLabel(
            SqlgGraph sqlgGraph,
            String schema,
            String prefixedTable,
            SchemaTable foreignKeyOut,
            SchemaTable foreignKeyIn,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers) {

        addEdgeLabel(sqlgGraph, schema, prefixedTable, foreignKeyOut, foreignKeyIn, columns, identifiers, PartitionType.NONE, null);
    }

    static void addEdgeLabel(
            SqlgGraph sqlgGraph,
            String schema,
            String prefixedTable,
            SchemaTable foreignKeyOut,
            SchemaTable foreignKeyIn,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression) {

        Vertex edgeVertex = addEdgeLabel(sqlgGraph, prefixedTable, columns, identifiers, partitionType, partitionExpression);

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema %s does not exist in Sqlg's topology. BUG!!!", schema);
            Preconditions.checkState(schemas.size() == 1, "Multiple %s found in Sqlg's topology. BUG!!!", schema);
            Vertex schemaVertex = schemas.get(0);

            List<Vertex> outVertices = traversalSource.V(schemaVertex)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyOut.getTable())
                    .toList();
            Preconditions.checkState(!outVertices.isEmpty(), "Schema %s does not contain vertex label %s ", schema, foreignKeyOut.getTable());
            Preconditions.checkState(outVertices.size() == 1, "Multiple out vertices %s found in Sqlg's topology. BUG!!!", foreignKeyOut.toString());
            Preconditions.checkState(prefixedTable.startsWith(EDGE_PREFIX));
            Vertex outVertex = outVertices.get(0);

            //Get the schema of the in vertex
            schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeyIn.getSchema())
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema %s does not exist in Sqlg's topology. BUG!!!", schema);
            Preconditions.checkState(schemas.size() == 1, "Multiple %s found in Sqlg's topology. BUG!!!", schema);
            Vertex schemaInVertex = schemas.get(0);

            List<Vertex> inVertices = traversalSource.V(schemaInVertex)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyIn.getTable())
                    .toList();
            Preconditions.checkState(!inVertices.isEmpty(), "In vertex %s does not exist in Sqlg's topology. BUG!!!", foreignKeyIn.toString());
            Preconditions.checkState(inVertices.size() == 1, "Multiple in vertices %s found in Sqlg's topology. BUG!!!", foreignKeyIn.toString());
            Vertex inVertex = inVertices.get(0);

            outVertex.addEdge(SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertex);
            inVertex.addEdge(SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertex);
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static Vertex addEdgeLabel(
            SqlgGraph sqlgGraph,
            String prefixedTable,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Vertex edgeVertex;
            if (partitionExpression != null) {
                Preconditions.checkState(partitionType != PartitionType.NONE, "If the partitionExpression is not null then the PartitionType may not be NONE. Found %s", partitionType.name());
                edgeVertex = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL,
                        "name", prefixedTable.substring(EDGE_PREFIX.length()),
                        CREATED_ON, LocalDateTime.now(),
                        Topology.SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE, partitionType.name(),
                        Topology.SQLG_SCHEMA_EDGE_LABEL_PARTITION_EXPRESSION, partitionExpression);
            } else {
                Preconditions.checkState(partitionType == PartitionType.NONE, "If the partitionExpression is null then the PartitionType must be NONE. Found %s", partitionType.name());
                edgeVertex = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL,
                        "name", prefixedTable.substring(EDGE_PREFIX.length()),
                        CREATED_ON, LocalDateTime.now(),
                        Topology.SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE, PartitionType.NONE.name());
            }

            for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {
                Vertex property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PROPERTY,
                        "name", columnEntry.getKey(),
                        "type", columnEntry.getValue().name(),
                        CREATED_ON, LocalDateTime.now()
                );
                edgeVertex.addEdge(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, property);
                //add in the identifiers if there are any.
                int i = 0;
                for (String identifier : identifiers) {
                    if (identifier.equals(columnEntry.getKey())) {
                        edgeVertex.addEdge(SQLG_SCHEMA_EDGE_IDENTIFIER_EDGE, property, SQLG_SCHEMA_EDGE_IDENTIFIER_INDEX_EDGE, i);
                    }
                    i++;
                }
            }
            return edgeVertex;
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void removeEdgeLabel(SqlgGraph sqlgGraph, EdgeLabel edge) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> edges =
                    traversalSource.V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                            .has("name", edge.getSchema().getName())
                            .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                            .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                            .has("name", edge.getName()).toList();
            if (edges.size() > 0) {
                Vertex edgeV = edges.get(0);

                traversalSource.V(edgeV)
                        .out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                        .drop()
                        .iterate();
                traversalSource.V(edgeV)
                        .drop()
                        .iterate();
            }

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void removeOutEdgeRole(SqlgGraph sqlgGraph, EdgeLabel edgeLabel, VertexLabel vertexLabel) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            String schema = edgeLabel.getSchema().getName();
            traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has(SQLG_SCHEMA_SCHEMA_NAME, vertexLabel.getSchema().getName())
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(SQLG_SCHEMA_VERTEX_LABEL_NAME, vertexLabel.getLabel())
                    .outE(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .drop()
                    .iterate();
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void removeInEdgeRole(SqlgGraph sqlgGraph, EdgeLabel edgeLabel, VertexLabel vertexLabel) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has(SQLG_SCHEMA_SCHEMA_NAME, vertexLabel.getSchema().getName())
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(SQLG_SCHEMA_VERTEX_LABEL_NAME, vertexLabel.getLabel())
                    .outE(SQLG_SCHEMA_IN_EDGES_EDGE)
                    .drop()
                    .iterate();
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addLabelToEdge(SqlgGraph sqlgGraph, String schema, String prefixedTable, boolean in, SchemaTable foreignKey) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema %s does not exist in Sqlg's topology. BUG!!!", schema);
            Preconditions.checkState(schemas.size() == 1, "Multiple %s found in Sqlg's topology. BUG!!!", schema);

            String foreignKeySchema = foreignKey.getSchema();
            schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeySchema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema %s does not exist in Sqlg's topology. BUG!!!", foreignKeySchema);
            Preconditions.checkState(schemas.size() == 1, "Multiple %s found in Sqlg's topology. BUG!!!", foreignKeySchema);
            Vertex foreignKeySchemaVertex = schemas.get(0);

            Preconditions.checkState(prefixedTable.startsWith(EDGE_PREFIX));
            List<Vertex> edgeVertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(EDGE_PREFIX.length())).as("a")
                    .in(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", schema)
                    .<Vertex>select("a")
                    .dedup()
                    .toList();
            Preconditions.checkState(!edgeVertices.isEmpty(), "Edge vertex '%s' does not exist in schema '%s'", prefixedTable.substring(EDGE_PREFIX.length()), schema);
            Preconditions.checkState(edgeVertices.size() == 1, "Multiple edge vertices %s found in Sqlg's topology. BUG!!!", foreignKey.toString());
            Vertex edgeVertex = edgeVertices.get(0);

            String foreignKeyVertexTable;
            if (in) {
//                foreignKeyVertexTable = foreignKey.getTable().substring(0, foreignKey.getTable().length() - Topology.IN_VERTEX_COLUMN_END.length());
                foreignKeyVertexTable = foreignKey.getTable();
            } else {
//                foreignKeyVertexTable = foreignKey.getTable().substring(0, foreignKey.getTable().length() - Topology.OUT_VERTEX_COLUMN_END.length());
                foreignKeyVertexTable = foreignKey.getTable();
            }
            List<Vertex> foreignKeyVertices = traversalSource.V(foreignKeySchemaVertex)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyVertexTable)
                    .toList();
            Preconditions.checkState(!foreignKeyVertices.isEmpty(), "Out vertex %s does not exist in Sqlg's topology. BUG!!!", foreignKey.toString());
            Preconditions.checkState(foreignKeyVertices.size() == 1, "Multiple out vertices %s found in Sqlg's topology. BUG!!!", foreignKey.toString());
            Preconditions.checkState(prefixedTable.startsWith(EDGE_PREFIX));
            Vertex foreignKeyVertex = foreignKeyVertices.get(0);

            if (in) {
                foreignKeyVertex.addEdge(SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertex);
            } else {
                foreignKeyVertex.addEdge(SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertex);
            }

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addLabelToEdge(SqlgGraph sqlgGraph, Vertex edgeVertex, String schema, String prefixedTable, boolean in, SchemaTable foreignKey) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema %s does not exist in Sqlg's topology. BUG!!!", schema);
            Preconditions.checkState(schemas.size() == 1, "Multiple %s found in Sqlg's topology. BUG!!!", schema);

            String foreignKeySchema = foreignKey.getSchema();
            schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeySchema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema %s does not exist in Sqlg's topology. BUG!!!", foreignKeySchema);
            Preconditions.checkState(schemas.size() == 1, "Multiple %s found in Sqlg's topology. BUG!!!", foreignKeySchema);
            Vertex foreignKeySchemaVertex = schemas.get(0);

            Preconditions.checkState(prefixedTable.startsWith(EDGE_PREFIX));

            String foreignKeyVertexTable;
            if (in) {
                foreignKeyVertexTable = foreignKey.getTable().substring(0, foreignKey.getTable().length() - Topology.IN_VERTEX_COLUMN_END.length());
            } else {
                foreignKeyVertexTable = foreignKey.getTable().substring(0, foreignKey.getTable().length() - Topology.OUT_VERTEX_COLUMN_END.length());
            }
            List<Vertex> foreignKeyVertices = traversalSource.V(foreignKeySchemaVertex)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyVertexTable)
                    .toList();
            Preconditions.checkState(!foreignKeyVertices.isEmpty(), "Out vertex %s does not exist in Sqlg's topology. BUG!!!", foreignKey.toString());
            Preconditions.checkState(foreignKeyVertices.size() == 1, "Multiple out vertices %s found in Sqlg's topology. BUG!!!", foreignKey.toString());
            Preconditions.checkState(prefixedTable.startsWith(EDGE_PREFIX));
            Vertex foreignKeyVertex = foreignKeyVertices.get(0);

            if (in) {
                foreignKeyVertex.addEdge(SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertex);
            } else {
                foreignKeyVertex.addEdge(SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertex);
            }

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addVertexColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map.Entry<String, PropertyType> column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(VERTEX_PREFIX), "prefixedTable must be for a vertex. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();

            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", prefixedTable.substring(VERTEX_PREFIX.length()))
                    .toList();
            if (vertices.size() == 0) {
                throw new IllegalStateException("Found no vertex for " + schema + "." + prefixedTable);
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException("Found more than one vertex for " + schema + "." + prefixedTable);
            }
            Vertex vertex = vertices.get(0);

            Vertex property = sqlgGraph.addVertex(
                    T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PROPERTY,
                    "name", column.getKey(),
                    "type", column.getValue().name(),
                    CREATED_ON, LocalDateTime.now()
            );
            vertex.addEdge(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, property);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    public static void removeVertexColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, String column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(VERTEX_PREFIX), "prefixedTable must be for a vertex. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();

            traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", prefixedTable.substring(VERTEX_PREFIX.length()))
                    .out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
                    .has("name", column)
                    .drop().iterate();

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    public static void renamePropertyColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, String column, String newName) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(VERTEX_PREFIX), "prefixedTable must be for a vertex. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();

            List<Vertex> propertiesToRename = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has(SQLG_SCHEMA_SCHEMA_NAME, schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has(SQLG_SCHEMA_VERTEX_LABEL_NAME, prefixedTable.substring(VERTEX_PREFIX.length()))
                    .out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
                    .has(SQLG_SCHEMA_PROPERTY_NAME, column)
                    .toList();
            Preconditions.checkState(propertiesToRename.size() == 1, String.format("Expected exactly one property in %s.%s.%s. Found %d", schema, propertiesToRename, column, propertiesToRename.size()));
            propertiesToRename.get(0).property(SQLG_SCHEMA_PROPERTY_NAME, newName);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void removeEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, String column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(EDGE_PREFIX), "prefixedTable must be for an edge. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();

            traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(EDGE_PREFIX.length()))
                    .as("a")
                    .in(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", schema)
                    .select("a")
                    .out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                    .has("name", column)
                    .drop().iterate();

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    public static void addIndex(SqlgGraph sqlgGraph, Index index) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            //get the abstractLabel's vertex
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> abstractLabelVertexes;

            AbstractLabel abstractLabel = index.getParentLabel();
            if (abstractLabel instanceof VertexLabel) {
                abstractLabelVertexes = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                        .has(SQLG_SCHEMA_SCHEMA_NAME, abstractLabel.getSchema().getName())
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .has("name", abstractLabel.getLabel())
                        .toList();
            } else {
                abstractLabelVertexes = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                        .has(SQLG_SCHEMA_SCHEMA_NAME, abstractLabel.getSchema().getName())
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .has("name", abstractLabel.getLabel())
                        .dedup()
                        .toList();
            }
            Preconditions.checkState(!abstractLabelVertexes.isEmpty(), "AbstractLabel %s.%s does not exists", abstractLabel.getSchema().getName(), abstractLabel.getLabel());
            Preconditions.checkState(abstractLabelVertexes.size() == 1, "BUG: multiple AbstractLabels found for %s.%s", abstractLabel.getSchema().getName(), abstractLabel.getLabel());
            Vertex abstractLabelVertex = abstractLabelVertexes.get(0);

            Vertex indexVertex = sqlgGraph.addVertex(
                    T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_INDEX,
                    SQLG_SCHEMA_INDEX_NAME, index.getName(),
                    SQLG_SCHEMA_INDEX_INDEX_TYPE, index.getIndexType().toString(),
                    CREATED_ON, LocalDateTime.now()
            );

            if (abstractLabel instanceof VertexLabel) {
                abstractLabelVertex.addEdge(SQLG_SCHEMA_VERTEX_INDEX_EDGE, indexVertex);
            } else {
                abstractLabelVertex.addEdge(SQLG_SCHEMA_EDGE_INDEX_EDGE, indexVertex);
            }
            int ix = 0;
            for (PropertyColumn property : index.getProperties()) {
                List<Vertex> propertyVertexes = traversalSource.V(abstractLabelVertex)
                        .out(abstractLabel instanceof VertexLabel ? SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE : SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                        .has("name", property.getName())
                        .toList();
                Preconditions.checkState(!propertyVertexes.isEmpty(), "Property %s for AbstractLabel %s.%s does not exists", property.getName(), abstractLabel.getSchema().getName(), abstractLabel.getLabel());
                Preconditions.checkState(propertyVertexes.size() == 1, "BUG: multiple Properties %s found for AbstractLabels found for %s.%s", property.getName(), abstractLabel.getSchema().getName(), abstractLabel.getLabel());
                Vertex propertyVertex = propertyVertexes.get(0);
                indexVertex.addEdge(SQLG_SCHEMA_INDEX_PROPERTY_EDGE, propertyVertex, SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE, ix++);
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void removeIndex(SqlgGraph sqlgGraph, Index index) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> abstractLabelVertexes;

            AbstractLabel abstractLabel = index.getParentLabel();
            if (abstractLabel instanceof VertexLabel) {
                abstractLabelVertexes = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                        .has(SQLG_SCHEMA_SCHEMA_NAME, abstractLabel.getSchema().getName())
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .has("name", abstractLabel.getLabel())
                        .toList();
            } else {
                abstractLabelVertexes = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                        .has(SQLG_SCHEMA_SCHEMA_NAME, abstractLabel.getSchema().getName())
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .has("name", abstractLabel.getLabel())
                        .dedup()
                        .toList();
            }
            if (abstractLabelVertexes.size() > 0) {
                Vertex v = abstractLabelVertexes.get(0);
                traversalSource.V(v.id())
                        .out(abstractLabel instanceof VertexLabel ? SQLG_SCHEMA_VERTEX_INDEX_EDGE : SQLG_SCHEMA_EDGE_INDEX_EDGE)
                        .has(SQLG_SCHEMA_INDEX_NAME, index.getName())
                        .drop()
                        .iterate();
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    /**
     * add an index from information schema
     *
     * @param sqlgGraph  the graph
     * @param schema     the schema name
     * @param label      the label name
     * @param vertex     is it a vertex or an edge label?
     * @param index      the index name
     * @param indexType  index type
     * @param properties the column names
     */
    public static void addIndex(SqlgGraph sqlgGraph, String schema, String label, boolean vertex, String index, IndexType indexType, List<String> properties) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            //get the abstractLabel's vertex
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> abstractLabelVertexes;
            if (vertex) {
                abstractLabelVertexes = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                        .has(SQLG_SCHEMA_SCHEMA_NAME, schema)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .has("name", label)
                        .toList();
            } else {
                abstractLabelVertexes = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                        .has(SQLG_SCHEMA_SCHEMA_NAME, schema)
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .has("name", label)
                        .dedup()
                        .toList();
            }
            Preconditions.checkState(!abstractLabelVertexes.isEmpty(), "AbstractLabel %s.%s does not exists", schema, label);
            Preconditions.checkState(abstractLabelVertexes.size() == 1, "BUG: multiple AbstractLabels found for %s.%s", schema, label);
            Vertex abstractLabelVertex = abstractLabelVertexes.get(0);

            boolean createdIndexVertex = false;
            Vertex indexVertex = null;
            int ix = 0;
            for (String property : properties) {

                List<Vertex> propertyVertexes = traversalSource.V(abstractLabelVertex)
                        .out(vertex ? SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE : SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                        .has("name", property)
                        .toList();

                //do not create indexes for properties that are not found.
                //TODO, Sqlg needs to get more sophisticated support for indexes, i.e. function indexes on a property etc.
                if (!createdIndexVertex && !propertyVertexes.isEmpty()) {
                    createdIndexVertex = true;
                    indexVertex = sqlgGraph.addVertex(
                            T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_INDEX,
                            SQLG_SCHEMA_INDEX_NAME, index,
                            SQLG_SCHEMA_INDEX_INDEX_TYPE, indexType.toString(),
                            CREATED_ON, LocalDateTime.now()
                    );
                    if (vertex) {
                        abstractLabelVertex.addEdge(SQLG_SCHEMA_VERTEX_INDEX_EDGE, indexVertex);
                    } else {
                        abstractLabelVertex.addEdge(SQLG_SCHEMA_EDGE_INDEX_EDGE, indexVertex);
                    }
                }
                if (!propertyVertexes.isEmpty()) {
                    Preconditions.checkState(propertyVertexes.size() == 1, "BUG: multiple Properties %s found for AbstractLabels found for %s.%s", property, schema, label);
                    Preconditions.checkState(indexVertex != null);
                    Vertex propertyVertex = propertyVertexes.get(0);
                    indexVertex.addEdge(SQLG_SCHEMA_INDEX_PROPERTY_EDGE, propertyVertex, SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE, ix);
                }
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map<String, PropertyType> column, ListOrderedSet<String> primaryKeys) {
        for (Map.Entry<String, PropertyType> stringPropertyTypeEntry : column.entrySet()) {
            addEdgeColumn(sqlgGraph, schema, prefixedTable, stringPropertyTypeEntry, primaryKeys);
        }
    }

    static void addEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map.Entry<String, PropertyType> column, ListOrderedSet<String> identifiers) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(EDGE_PREFIX), "prefixedTable must be for an edge. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            Set<Vertex> edges = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(EDGE_PREFIX.length()))
                    .as("a")
                    .in(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", schema)
                    .<Vertex>select("a")
                    .toSet();
            if (edges.size() == 0) {
                throw new IllegalStateException("Found no edge for " + prefixedTable);
            }
            if (edges.size() > 1) {
                throw new IllegalStateException("Found more than one edge for " + prefixedTable);
            }
            Vertex edge = edges.iterator().next();

            Vertex property = sqlgGraph.addVertex(
                    T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PROPERTY,
                    "name", column.getKey(),
                    "type", column.getValue().name(),
                    CREATED_ON, LocalDateTime.now()
            );
            edge.addEdge(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, property);
            int i = 0;
            for (String identifier : identifiers) {
                if (identifier.equals(column.getKey())) {
                    edge.addEdge(SQLG_SCHEMA_EDGE_IDENTIFIER_EDGE, property, SQLG_SCHEMA_EDGE_IDENTIFIER_INDEX_EDGE, i);
                }
                i++;
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    private static BatchManager.BatchModeType flushAndSetTxToNone(SqlgGraph sqlgGraph) {
        //topology elements can not be added in batch mode because on flushing the topology
        //needs to be queries and yet the elements are still in the cache.
        BatchManager.BatchModeType batchModeType = sqlgGraph.tx().getBatchModeType();
        if (sqlgGraph.tx().isInBatchMode()) {
            batchModeType = sqlgGraph.tx().getBatchModeType();
            sqlgGraph.tx().flush();
            sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NONE);
        }
        return batchModeType;
    }

    public static void removePartition(SqlgGraph sqlgGraph, Partition partition) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            AbstractLabel abstractLabel = partition.getAbstractLabel();
            List<Vertex> partitions;
            if (abstractLabel instanceof VertexLabel) {
                partitions = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                        .has("name", abstractLabel.getSchema().getName())
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .has(SQLG_SCHEMA_VERTEX_LABEL_NAME, abstractLabel.getName())
                        .repeat(__.out(SQLG_SCHEMA_VERTEX_PARTITION_EDGE, SQLG_SCHEMA_PARTITION_PARTITION_EDGE))
                        .until(__.has(SQLG_SCHEMA_PARTITION_NAME, partition.getName()))
                        .toList();

            } else {
                partitions = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                        .has("name", abstractLabel.getSchema().getName())
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .has(SQLG_SCHEMA_EDGE_LABEL_NAME, abstractLabel.getName())
                        .repeat(__.out(SQLG_SCHEMA_EDGE_PARTITION_EDGE, SQLG_SCHEMA_PARTITION_PARTITION_EDGE))
                        .until(__.has(SQLG_SCHEMA_PARTITION_NAME, partition.getName()))
                        .toList();
            }
            Preconditions.checkState(partitions.size() == 1);
            Vertex partitionVertex = partitions.get(0);
            partitionVertex.remove();
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    /**
     * Adds the partition to a partition. A new Vertex with label Partition is added and in linked to its parent with
     * the SQLG_SCHEMA_PARTITION_PARTITION_EDGE edge label.
     *
     * @param sqlgGraph The graph.
     */
    public static void addSubPartition(SqlgGraph sqlgGraph, Partition partition) {
        AbstractLabel abstractLabel = partition.getAbstractLabel();
        addSubPartition(
                sqlgGraph,
                partition.getParentPartition().getParentPartition() != null,
                abstractLabel instanceof VertexLabel,
                abstractLabel.getSchema().getName(),
                abstractLabel.getName(),
                partition.getParentPartition().getName(),
                partition.getName(),
                partition.getPartitionType(),
                partition.getPartitionExpression(),
                partition.getFrom(),
                partition.getTo(),
                partition.getIn(),
                partition.getModulus(),
                partition.getRemainder()
        );

    }

    public static void addSubPartition(
            SqlgGraph sqlgGraph,
            boolean isSubSubPartition,
            boolean isVertexLabel,
            String schema,
            String abstractLabel,
            String partitionParent,
            String partitionName,
            PartitionType partitionType,
            String partitionExpression,
            String from,
            String to,
            String in,
            Integer modulus,
            Integer remainder) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertices;
            if (isVertexLabel) {
                if (!isSubSubPartition) {
                    vertices = traversalSource.V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                            .has("name", schema)
                            .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                            .has("name", abstractLabel)
                            .out(SQLG_SCHEMA_VERTEX_PARTITION_EDGE)
                            .has("name", partitionParent)
                            .toList();
                } else {
                    vertices = traversalSource.V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                            .has("name", schema)
                            .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                            .has("name", abstractLabel)
                            .out(SQLG_SCHEMA_VERTEX_PARTITION_EDGE)
                            .repeat(__.out(SQLG_SCHEMA_PARTITION_PARTITION_EDGE))
                            .until(__.has("name", partitionParent))
                            .toList();
                }
            } else {
                if (!isSubSubPartition) {
                    vertices = traversalSource.V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                            .has("name", schema)
                            .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                            .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                            .has("name", abstractLabel)
                            .out(SQLG_SCHEMA_EDGE_PARTITION_EDGE)
                            .has("name", partitionParent)
                            .toList();
                } else {
                    vertices = traversalSource.V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                            .has("name", schema)
                            .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                            .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                            .has("name", abstractLabel)
                            .out(SQLG_SCHEMA_EDGE_PARTITION_EDGE)
                            .repeat(__.out(SQLG_SCHEMA_PARTITION_PARTITION_EDGE))
                            .until(__.has("name", partitionParent))
                            .toList();
                }
            }
            if (vertices.size() == 0) {
                throw new IllegalStateException(String.format("Found no vertex for %s.%s#%s", schema, abstractLabel, partitionParent));
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException(String.format("Found more than one vertex for %s.%s#%s", schema, abstractLabel, partitionParent));
            }
            Vertex vertex = vertices.get(0);

            Vertex subPartition;
            if (partitionExpression != null) {
                if (from != null) {
                    Preconditions.checkState(to != null);
                    subPartition = sqlgGraph.addVertex(
                            T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                            SQLG_SCHEMA_PARTITION_NAME, partitionName,
                            SQLG_SCHEMA_PARTITION_FROM, from,
                            SQLG_SCHEMA_PARTITION_TO, to,
                            SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                            SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression,
                            CREATED_ON, LocalDateTime.now()
                    );
                } else if (in != null) {
                    subPartition = sqlgGraph.addVertex(
                            T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                            SQLG_SCHEMA_PARTITION_NAME, partitionName,
                            SQLG_SCHEMA_PARTITION_IN, in,
                            SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                            SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression,
                            CREATED_ON, LocalDateTime.now()
                    );
                } else {
                    Preconditions.checkState(modulus > 0);
                    Preconditions.checkState(remainder >= 0);
                    subPartition = sqlgGraph.addVertex(
                            T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                            SQLG_SCHEMA_PARTITION_NAME, partitionName,
                            SQLG_SCHEMA_PARTITION_MODULUS, modulus,
                            SQLG_SCHEMA_PARTITION_REMAINDER, remainder,
                            SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                            SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, partitionExpression,
                            CREATED_ON, LocalDateTime.now()
                    );

                }
            } else {
                if (from != null) {
                    Preconditions.checkState(to != null);
                    subPartition = sqlgGraph.addVertex(
                            T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                            SQLG_SCHEMA_PARTITION_NAME, partitionName,
                            SQLG_SCHEMA_PARTITION_FROM, from,
                            SQLG_SCHEMA_PARTITION_TO, to,
                            SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                            CREATED_ON, LocalDateTime.now()
                    );
                } else if (in != null){
                    subPartition = sqlgGraph.addVertex(
                            T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                            SQLG_SCHEMA_PARTITION_NAME, partitionName,
                            SQLG_SCHEMA_PARTITION_IN, in,
                            SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                            CREATED_ON, LocalDateTime.now()
                    );
                } else {
                    Preconditions.checkState(modulus > 0);
                    Preconditions.checkState(remainder >= 0);
                    subPartition = sqlgGraph.addVertex(
                            T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PARTITION,
                            SQLG_SCHEMA_PARTITION_NAME, partitionName,
                            SQLG_SCHEMA_PARTITION_MODULUS, modulus,
                            SQLG_SCHEMA_PARTITION_REMAINDER, remainder,
                            SQLG_SCHEMA_PARTITION_PARTITION_TYPE, partitionType.name(),
                            CREATED_ON, LocalDateTime.now()
                    );
                }
            }
            vertex.addEdge(SQLG_SCHEMA_PARTITION_PARTITION_EDGE, subPartition);
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    public static void updateVertexLabelPartitionTypeAndExpression(
            SqlgGraph sqlgGraph,
            String schema,
            String name,
            PartitionType partitionType,
            String partitionExpression) {

        GraphTraversalSource traversalSource = sqlgGraph.topology();
        List<Vertex> vertexLabels = traversalSource
                .V().hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA).has(Topology.SQLG_SCHEMA_SCHEMA_NAME, schema)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, name)
                .toList();
        Preconditions.checkState(vertexLabels.size() == 1, "BUG: There can only ever be one VertexLabel vertex, found %s", vertexLabels.size());
        Vertex vertexLabel = vertexLabels.get(0);
        vertexLabel.property(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE, partitionType.name());
        vertexLabel.property(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_EXPRESSION, partitionExpression);
    }

    public static void updateEdgeLabelPartitionTypeAndExpression(
            SqlgGraph sqlgGraph,
            String schema,
            String name,
            PartitionType partitionType,
            String partitionExpression) {

        GraphTraversalSource traversalSource = sqlgGraph.topology();
        List<Vertex> edgeLabels = traversalSource
                .V().hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA).has(Topology.SQLG_SCHEMA_SCHEMA_NAME, schema)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .out(SQLG_SCHEMA_OUT_EDGES_EDGE)
                .has(Topology.SQLG_SCHEMA_EDGE_LABEL_NAME, name)
                .toList();
        Preconditions.checkState(edgeLabels.size() == 1, "BUG: There can only ever be one EdgeLabel vertex, found %s", edgeLabels.size());
        Vertex vertexLabel = edgeLabels.get(0);
        vertexLabel.property(SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE, partitionType.name());
        vertexLabel.property(SQLG_SCHEMA_EDGE_LABEL_PARTITION_EXPRESSION, partitionExpression);
    }

    static void distributeAbstractLabel(
            SqlgGraph sqlgGraph,
            AbstractLabel abstractLabel,
            int shardCount,
            PropertyColumn distributionPropertyColumn,
            AbstractLabel colocate) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            //get the vertex
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> vertices;
            if (abstractLabel instanceof VertexLabel) {
                vertices = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                        .has("name", abstractLabel.getSchema().getName())
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .has("name", abstractLabel.getName())
                        .toList();
            } else {
                vertices = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                        .has("name", abstractLabel.label)
                        .as("a")
                        .in(SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .has("name", abstractLabel.getSchema().getName())
                        .<Vertex>select("a")
                        .toList();
                if (vertices.size() == 0) {
                    throw new IllegalStateException(String.format("Found no edge for %s", abstractLabel.label));
                }
                if (vertices.size() > 1) {
                    throw new IllegalStateException(String.format("Found more than one edge for %s", abstractLabel.label));
                }
            }

            Preconditions.checkState(vertices.size() != 0, "Found no vertex for %s", abstractLabel.getFullName());
            Preconditions.checkState(vertices.size() == 1, "Found more than one vertex for %s", abstractLabel.getFullName());
            Vertex vertex = vertices.get(0);

            //get the distributionPropertyColumn's vertex
            List<Vertex> distributionColumnPropertyVertices = traversalSource.V(vertex)
                    .out((abstractLabel instanceof VertexLabel ? SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE : SQLG_SCHEMA_EDGE_PROPERTIES_EDGE))
                    .has(SQLG_SCHEMA_PROPERTY_NAME, distributionPropertyColumn.getName())
                    .toList();
            Preconditions.checkState(distributionColumnPropertyVertices.size() == 1, "VertexLabel %s must have one and only only one property with name %s", abstractLabel.getFullName(), distributionPropertyColumn.getName());
            Vertex distributionPropertyVertex = distributionColumnPropertyVertices.get(0);

            if (abstractLabel instanceof VertexLabel) {
                vertex.addEdge(Topology.SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLUMN_EDGE, distributionPropertyVertex);
            } else {
                vertex.addEdge(Topology.SQLG_SCHEMA_EDGE_DISTRIBUTION_COLUMN_EDGE, distributionPropertyVertex);
            }

            //get the colocate's vertex
            if (colocate != null) {
                List<Vertex> colocateVertices = traversalSource.V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                        .has("name", colocate.getSchema().getName())
                        .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .has("name", colocate.getName())
                        .toList();
                Preconditions.checkState(colocateVertices.size() == 1, "Did not find the colocate %s vertex", colocate.getFullName());
                Vertex colocateVertex = colocateVertices.get(0);
                if (abstractLabel instanceof VertexLabel) {
                    vertex.addEdge(Topology.SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLOCATE_EDGE, colocateVertex);
                } else {
                    vertex.addEdge(Topology.SQLG_SCHEMA_EDGE_DISTRIBUTION_COLOCATE_EDGE, colocateVertex);
                }
            }

            //set the shard_count
            if (shardCount > -1) {
                if (abstractLabel instanceof VertexLabel) {
                    vertex.property(SQLG_SCHEMA_VERTEX_LABEL_DISTRIBUTION_SHARD_COUNT, shardCount);
                } else {
                    vertex.property(SQLG_SCHEMA_EDGE_LABEL_DISTRIBUTION_SHARD_COUNT, shardCount);
                }
            }

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }
}
