package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.strategy.TopologyStrategy;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Created by pieter on 2015/12/08.
 */
public class TopologyManager {

    static void addVertexLabel(SqlgGraph sqlgGraph, String schema, String tableName, Map<String, PropertyType> columns) {
        GraphTraversalSource traversalSource = GraphTraversalSource.build().with(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create()).create(sqlgGraph);
        GraphTraversal<Vertex, Vertex> gt = traversalSource.V().hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA).has("name", schema);
        Preconditions.checkState(gt.hasNext(), "Schema " + schema + " does not exist but it should. BUG!!!");
        Vertex schemaVertex = gt.next();
        Vertex vertex = sqlgGraph.addVertex(
                T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_VERTEX_LABEL,
                "name", tableName,
                "createdOn", LocalDateTime.now()
        );
        schemaVertex.addEdge(SchemaManager.SQLG_SCHEMA_EDGE_SCHEMA_VERTEX, vertex);
        for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {

            Vertex property = sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_PROPERTY,
                    "name", columnEntry.getKey(),
                    "createdOn", LocalDateTime.now()
            );
            vertex.addEdge(SchemaManager.SQLG_SCHEMA_EDGE_VERTEX_PROPERTIES, property);

        }
    }

    static Vertex addSchema(SqlgGraph sqlgGraph, String schema) {
        return sqlgGraph.addVertex(
                T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA,
                "name", schema,
                "createdOn", LocalDateTime.now()
        );
    }
}
