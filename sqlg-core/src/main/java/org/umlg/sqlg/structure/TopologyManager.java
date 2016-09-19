package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by pieter on 2015/12/08.
 */
public class TopologyManager {

    public static final String CREATED_ON = "createdOn";
    public static final String DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG = " does not exist in Sqlg's topology. BUG!!!";
    public static final String SCHEMA = "Schema ";
    public static final String FOUND_IN_SQLG_S_TOPOLOGY_BUG = " found in Sqlg's topology. BUG!!!";
    public static final String MULTIPLE = "Multiple ";

    private TopologyManager() {}

    static Vertex addSchema(SqlgGraph sqlgGraph, String schema) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            return sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA,
                    "name", schema,
                    CREATED_ON, LocalDateTime.now()
            );
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    static void addVertexLabel(SqlgGraph sqlgGraph, String schema, String tableName, Map<String, PropertyType> columns) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + schema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + schema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(tableName.startsWith(SchemaManager.VERTEX_PREFIX));
            Vertex schemaVertex = schemas.get(0);

            Vertex vertex = sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_VERTEX_LABEL,
                    "name", tableName.substring(SchemaManager.VERTEX_PREFIX.length()),
                    "schemaVertex", schema + tableName, //this is here for readability when in pgadmin
                    CREATED_ON, LocalDateTime.now()
            );
            schemaVertex.addEdge(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertex);
            for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {

                Vertex property = sqlgGraph.addVertex(
                        T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_PROPERTY,
                        "name", columnEntry.getKey(),
                        "type", columnEntry.getValue().name(),
                        CREATED_ON, LocalDateTime.now()
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
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + schema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + schema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex schemaVertex = schemas.get(0);

            List<Vertex> outVertices = traversalSource.V(schemaVertex)
                    .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyOut.getTable())
                    .toList();
            Preconditions.checkState(!outVertices.isEmpty(), "Out vertex " + foreignKeyOut.toString() + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(outVertices.size() == 1, "Multiple out vertices " + foreignKeyOut.toString() + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX));
            Vertex outVertex = outVertices.get(0);

            //Get the schema of the in vertex
            schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeyIn.getSchema())
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + schema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + schema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex schemaInVertex = schemas.get(0);

            List<Vertex> inVertices = traversalSource.V(schemaInVertex)
                    .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyIn.getTable())
                    .toList();
            Preconditions.checkState(!inVertices.isEmpty(), "In vertex " + foreignKeyIn.toString() + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(inVertices.size() == 1, "Multiple in vertices " + foreignKeyIn.toString() + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex inVertex = inVertices.get(0);

            Vertex edgeVertex = sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_EDGE_LABEL,
                    "name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length()),
                    CREATED_ON, LocalDateTime.now()
            );

            outVertex.addEdge(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertex);
            inVertex.addEdge(SchemaManager.SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertex);

            for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {

                Vertex property = sqlgGraph.addVertex(
                        T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_PROPERTY,
                        "name", columnEntry.getKey(),
                        "type", columnEntry.getValue().name(),
                        CREATED_ON, LocalDateTime.now()
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
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + schema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + schema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);

            String foreignKeySchema = foreignKey.getSchema();
            schemas = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeySchema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + foreignKeySchema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + foreignKeySchema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex foreignKeySchemaVertex = schemas.get(0);

            Preconditions.checkState(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX));
            List<Vertex> edgeVertices = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length())).as("a")
                    .in(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .in(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", schema)
                    .<Vertex>select("a")
                    .dedup()
                    .toList();
            Preconditions.checkState(!edgeVertices.isEmpty(), "Edge vertex " + foreignKey.toString() + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(edgeVertices.size() == 1, "Multiple edge vertices " + foreignKey.toString() + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
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
            Preconditions.checkState(!foreignKeyVertices.isEmpty(), "Out vertex " + foreignKey.toString() + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(foreignKeyVertices.size() == 1, "Multiple out vertices " + foreignKey.toString() + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
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
            GraphTraversalSource traversalSource = sqlgGraph.topology();

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
                    CREATED_ON, LocalDateTime.now()
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
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            Set<Vertex> edges = traversalSource.V()
                    .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length()))
                    .as("a")
                    .in(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .in(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
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
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_PROPERTY,
                    "name", column.getKey(),
                    "type", column.getValue().name(),
                    CREATED_ON, LocalDateTime.now()
            );
            edge.addEdge(SchemaManager.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, property);
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }

    static void addUniqueConstraint(SqlgGraph sqlgGraph, boolean onVertices, String schema,
            String property, String... labels) {

        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);

        try {
            String constraintName = SchemaManager.getUniqueConstraintTableName(onVertices, schema, property, labels);

            Vertex constraint = sqlgGraph.addVertex(
                    T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_UNIQUE_CONSTRAINT,
                    "name", constraintName,
                    "onVertices", onVertices,
                    CREATED_ON, LocalDateTime.now());

            GraphTraversalSource source = sqlgGraph.topology();

            Vertex schemaVertex = source.V()
                    .has(T.label, SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema).next();

            schemaVertex.addEdge(SchemaManager.SQLG_SCHEMA_SCHEMA_UNIQUE_CONSTRAINT_EDGE, constraint);

            if (labels.length == 0) {
                constraint.property("property", property);
            } else {
                //find all the vertices/edges that we define the unique constraint on the property
                Supplier<GraphTraversal<?, Vertex>> labelQuery = () -> {
                    GraphTraversal<?, Vertex> q = source.V(schemaVertex)
                            .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                            .has("name", P.within(labels))
                            .as("v");
                    if (onVertices) {
                        q = q.out(SchemaManager.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE);
                    } else {
                        q = q.out(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE)
                                .out(SchemaManager.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE);
                    }

                    return q.has("name", property).select("v");
                };

                List<Vertex> labelVertices = labelQuery.get().toList();

                if (labelVertices.size() == labels.length) {
                    String edgeName = onVertices
                            ? SchemaManager.SQLG_SCHEMA_VERTEX_UNIQUE_CONSTRAINT_EDGE
                            : SchemaManager.SQLG_SCHEMA_EDGE_UNIQUE_CONSTRAINT_EDGE;

                    for (Vertex l : labelVertices) {
                        l.addEdge(edgeName, constraint, "property", property);
                    }
                } else {
                    List<String> found = labelQuery.get().<String>properties("name").toStream()
                            .map(p -> p.orElse(null))
                            .collect(Collectors.toList());
                    List<String> notFound = new ArrayList<>(Arrays.asList(labels));
                    notFound.removeAll(found);
                    throw new IllegalStateException("You can only add a unique constraint on already existing" +
                            " properties. These labels were not found to contain the property: " + notFound);
                }
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

    static PropertyType getPropertyType(SqlgGraph sqlgGraph, boolean onVertices, String schema, String label,
            String property) {
        GraphTraversal<?, ? extends Element> q = sqlgGraph.topology().V()
                .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                .has("name", schema)
                .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .has("name", label);

        if (onVertices) {
            q = q.out(SchemaManager.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE);
        } else {
            q = q.out(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .has("name", label)
                    .out(SchemaManager.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE);
        }

        return q.has("name", property)
                .<String>properties("type")
                .toStream()
                .map(p -> PropertyType.valueOf(p.value()))
                .findFirst().orElse(null);
    }

    static Set<String> getLabelsDefiningProperty(SqlgGraph sqlgGraph, boolean onVertices, String schema, String property) {
        GraphTraversal<?, ? extends Element> q = sqlgGraph.topology().V()
                .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA)
                .has("name", schema)
                .out(SchemaManager.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .as("v");

        if (onVertices) {
            q = q.out(SchemaManager.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE);
        } else {
            q = q.out(SchemaManager.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .out(SchemaManager.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE);
        }

        return q.has("name", property)
                .select("v").<String>properties("name").toStream().map(Property::value)
                .collect(Collectors.toSet());
    }

}
