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

    private TopologyManager() {}

    static Vertex addSchema(SqlgGraph sqlgGraph, String schema) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            return sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA,
                    "name", schema,
                    "createdOn", LocalDateTime.now()
            );
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    static void addVertexLabel(SqlgGraph sqlgGraph, String schema, String tableName, Map<String, PropertyType> columns) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.traversal().withStrategies(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create());
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
            GraphTraversalSource traversalSource = sqlgGraph.traversal().withStrategies(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create());
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
            Preconditions.checkState(!outVertices.isEmpty(), "Out vertex " + foreignKeyOut.toString() + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(outVertices.size() == 1, "Multiple out vertices " + foreignKeyOut.toString() + " found in Sqlg's topology. BUG!!!");
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

    static void addLabelToEdge(SqlgGraph sqlgGraph, String schema, String prefixedTable, boolean in, SchemaTable foreignKey) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.traversal().withStrategies(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create());
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema " + schema + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(schemas.size() == 1, "Multiple " + schema + " found in Sqlg's topology. BUG!!!");

            String foreignKeySchema = foreignKey.getSchema();
            schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeySchema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), "Schema " + foreignKeySchema + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(schemas.size() == 1, "Multiple " + foreignKeySchema + " found in Sqlg's topology. BUG!!!");
            Vertex foreignKeySchemaVertex = schemas.get(0);

            Preconditions.checkState(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX));
            List<Vertex> edgeVertices = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length())).as("a")
                    .in(in ? SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE : SchemaManager.SQLG_SCHEMA_IN_EDGES_EDGE)
                    .in(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", schema)
                    .<Vertex>select("a")
                    .dedup()
                    .toList();
            Preconditions.checkState(!edgeVertices.isEmpty(), "Edge vertex " + foreignKey.toString() + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(edgeVertices.size() == 1, "Multiple edge vertices " + foreignKey.toString() + " found in Sqlg's topology. BUG!!!");
            Vertex edgeVertex = edgeVertices.get(0);

            String foreignKeyVertexTable;
            if (in)  {
                foreignKeyVertexTable = foreignKey.getTable().substring(0, foreignKey.getTable().length() - SchemaManager.IN_VERTEX_COLUMN_END.length());
            } else {
                foreignKeyVertexTable = foreignKey.getTable().substring(0, foreignKey.getTable().length() - SchemaManager.OUT_VERTEX_COLUMN_END.length());
            }
            List<Vertex> foreignKeyVertices = traversalSource.V(foreignKeySchemaVertex)
                    .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyVertexTable)
                    .toList();
            Preconditions.checkState(!foreignKeyVertices.isEmpty(), "Out vertex " + foreignKey.toString() + " does not exist in Sqlg's topology. BUG!!!");
            Preconditions.checkState(foreignKeyVertices.size() == 1, "Multiple out vertices " + foreignKey.toString() + " found in Sqlg's topology. BUG!!!");
            Preconditions.checkState(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX));
            Vertex foreignKeyVertex = foreignKeyVertices.get(0);

            if (in) {
                foreignKeyVertex.addEdge(SchemaManager.SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertex);
            } else {
                foreignKeyVertex.addEdge(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertex);
            }

        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    static void addVertexColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map.Entry<String, PropertyType> column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(SchemaManager.VERTEX_PREFIX), "prefixedTable must be for a vertex. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.traversal().withStrategies(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create());

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

    static void addEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map<String, PropertyType> column) {
        for (Map.Entry<String, PropertyType> stringPropertyTypeEntry : column.entrySet()) {
            addEdgeColumn(sqlgGraph, schema, prefixedTable, stringPropertyTypeEntry);
        }
    }

    static void addEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map.Entry<String, PropertyType> column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX), "prefixedTable must be for an edge. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.traversal().withStrategies(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create());

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
