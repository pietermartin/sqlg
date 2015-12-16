package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.strategy.TopologyStrategy;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Created by pieter on 2015/12/08.
 */
public class TopologyManager {

    static Vertex addSchema(SqlgGraph sqlgGraph, String schema) {
        return sqlgGraph.addVertex(
                T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA,
                "name", schema,
                "createdOn", LocalDateTime.now()
        );
    }

    static void addVertexLabel(SqlgGraph sqlgGraph, String schema, String tableName, Map<String, PropertyType> columns) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = GraphTraversalSource.build().with(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create()).create(sqlgGraph);
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema " + schema + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(schemas.size() == 1, "Multiple " + schema + " found in Sqlg's topology. BUG!!!");
            Preconditions.checkState(tableName.startsWith(SchemaManager.VERTEX_PREFIX));
            Vertex schemaVertex = schemas.get(0);

            Vertex vertex = sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_VERTEX_LABEL,
                    "name", tableName.substring(SchemaManager.VERTEX_PREFIX.length()),
                    "schemaVertex", schema + tableName, //this is here for readability when in pgadmin
                    "createdOn", LocalDateTime.now()
            );
            schemaVertex.addEdge(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertex);
            for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {

                Vertex property = sqlgGraph.addVertex(
                        T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_PROPERTY,
                        "name", columnEntry.getKey(),
                        "type", columnEntry.getValue().name(),
                        "createdOn", LocalDateTime.now()
                );
                vertex.addEdge(SchemaManager.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, property);

            }

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }


    static void addEdgeLabel(SqlgGraph sqlgGraph, String schema, String prefixedTable, SchemaTable foreignKeyIn, SchemaTable foreignKeyOut, Map<String, PropertyType> columns) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = GraphTraversalSource.build().with(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create()).create(sqlgGraph);
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema " + schema + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(schemas.size() == 1, "Multiple " + schema + " found in Sqlg's topology. BUG!!!");
            Vertex schemaVertex = schemas.get(0);

            List<Vertex> outVertices = traversalSource.V(schemaVertex)
                    .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyOut.getTable())
                    .toList();
            Preconditions.checkState(!outVertices.isEmpty(), "Out vertex " + foreignKeyIn.toString() + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(outVertices.size() == 1, "Multiple out vertices " + foreignKeyIn.toString() + " found in Sqlg's topology. BUG!!!");
            Preconditions.checkState(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX));
            Vertex outVertex = outVertices.get(0);

            //Get the schema of the in vertex
            schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeyIn.getSchema())
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema " + schema + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(schemas.size() == 1, "Multiple " + schema + " found in Sqlg's topology. BUG!!!");
            Vertex schemaInVertex = schemas.get(0);

            List<Vertex> inVertices = traversalSource.V(schemaInVertex)
                    .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyIn.getTable())
                    .toList();
            Preconditions.checkState(!inVertices.isEmpty(), "In vertex " + foreignKeyIn.toString() + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(inVertices.size() == 1, "Multiple in vertices " + foreignKeyIn.toString() + " found in Sqlg's topology. BUG!!!");
            Vertex inVertex = inVertices.get(0);

            Vertex edgeVertex = sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_EDGE_LABEL,
                    "name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length()),
                    "createdOn", LocalDateTime.now()
            );

            outVertex.addEdge(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertex);
            inVertex.addEdge(SchemaManager.SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertex);

            for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {

                Vertex property = sqlgGraph.addVertex(
                        T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_PROPERTY,
                        "name", columnEntry.getKey(),
                        "type", columnEntry.getValue().name(),
                        "createdOn", LocalDateTime.now()
                );
                edgeVertex.addEdge(SchemaManager.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, property);

            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    public static void addVertexColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map.Entry<String, PropertyType> column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(SchemaManager.VERTEX_PREFIX), "prefixedTable must be for a vertex. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.traversal(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create());

            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", prefixedTable.substring(SchemaManager.VERTEX_PREFIX.length()))
                    .toList();
            if (vertices.size() == 0) {
                throw new IllegalStateException("Found no vertex for " + schema + "." + prefixedTable);
            }
            if (vertices.size() > 1) {
                throw new IllegalStateException("Found more than one vertex for " + schema + "." + prefixedTable);
            }
            Vertex vertex = vertices.get(0);


            Vertex property = sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_PROPERTY,
                    "name", column.getKey(),
                    "type", column.getValue().name(),
                    "createdOn", LocalDateTime.now()
            );
            vertex.addEdge(SchemaManager.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, property);

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    public static void addEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map.Entry<String, PropertyType> column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX), "prefixedTable must be for an edge. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.traversal(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create());

            List<Vertex> edges = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length()))
                    .as("a")
                    .in(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .in(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", schema)
                    .<Vertex>select("a")
                    .toList();
            if (edges.size() == 0) {
                throw new IllegalStateException("Found no edge for " + prefixedTable);
            }
            if (edges.size() > 1) {
                throw new IllegalStateException("Found more than one edge for " + prefixedTable);
            }
            Vertex edge = edges.get(0);


            Vertex property = sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_PROPERTY,
                    "name", column.getKey(),
                    "type", column.getValue().name(),
                    "createdOn", LocalDateTime.now()
            );
            edge.addEdge(SchemaManager.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, property);
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
}
