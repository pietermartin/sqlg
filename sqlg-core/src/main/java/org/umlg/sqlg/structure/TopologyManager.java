package org.umlg.sqlg.structure;

import static org.umlg.sqlg.structure.Topology.SCHEMA_VERTEX_DISPLAY;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_EDGE_INDEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_EDGE_LABEL;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_INDEX;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_INDEX_INDEX_TYPE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_INDEX_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_INDEX_PROPERTY_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_IN_EDGES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_OUT_EDGES_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_PROPERTY;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_INDEX_EDGE;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_LABEL;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.base.Preconditions;

/**
 * Created by pieter on 2015/12/08.
 */
public class TopologyManager {

    public static final String CREATED_ON = "createdOn";
    public static final String DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG = " does not exist in Sqlg's topology. BUG!!!";
    public static final String SCHEMA = "Schema ";
    public static final String FOUND_IN_SQLG_S_TOPOLOGY_BUG = " found in Sqlg's topology. BUG!!!";
    public static final String MULTIPLE = "Multiple ";

    private TopologyManager() {
    }

    public static Vertex addSchema(SqlgGraph sqlgGraph, String schema) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            return sqlgGraph.addVertex(
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
            if (schemas.size()>0){
            	Vertex vs=schemas.get(0);
            	traversalSource.V(vs)
            		.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
            		.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
            		.drop().iterate();
            	traversalSource.V(vs)
	        		.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
	        		.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
	        		.inE(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
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
	        		.inE(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
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
            	
            	// delete global unique indices with no properties left
            	// TODO this doesn't work, to investigate?
            	/*traversalSource.V().hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX)
            		.where(__.not(__.out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)))
            		.drop().iterate();*/
            	for (Vertex v:traversalSource.V().hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX)
            			.toList()){
            		if (!v.edges(Direction.OUT, SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE).hasNext()){
            			traversalSource.V(v).drop().iterate();
            		}
            	}
            	traversalSource.V(vs)
    				.drop().iterate();
            } 	
           
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void addVertexLabel(SqlgGraph sqlgGraph, String schema, String tableName, Map<String, PropertyType> columns) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            //get the schema vertex
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + schema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + schema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(!tableName.startsWith(SchemaManager.VERTEX_PREFIX));
            Vertex schemaVertex = schemas.get(0);

            Vertex vertex = sqlgGraph.addVertex(
                    T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL,
                    SQLG_SCHEMA_VERTEX_LABEL_NAME, tableName,
                    SCHEMA_VERTEX_DISPLAY, schema + "." + SchemaManager.VERTEX_PREFIX + tableName, //this is here for display when in pgadmin
                    CREATED_ON, LocalDateTime.now()
            );
            schemaVertex.addEdge(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertex);
            for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {
                Vertex property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PROPERTY,
                        "name", columnEntry.getKey(),
                        "type", columnEntry.getValue().name(),
                        CREATED_ON, LocalDateTime.now()
                );
                vertex.addEdge(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, property);
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }
    
    public static void removeVertexLabel(SqlgGraph sqlgGraph, VertexLabel lbl){
    	BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
        	GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .has("name", lbl.getSchema().getName())
                    .toList();
            if (schemas.size()>0){
            	Vertex schemaVertex = schemas.get(0);
            	List<Vertex> vertices= traversalSource.V(schemaVertex)
            			.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
            			.has(SQLG_SCHEMA_VERTEX_LABEL_NAME,lbl.getLabel())
            			.toList();
            	if (vertices.size()>0){
            		Vertex vertex=vertices.get(0);
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

    public static void addEdgeLabel(SqlgGraph sqlgGraph, String schema, String prefixedTable, SchemaTable foreignKeyOut, SchemaTable foreignKeyIn, Map<String, PropertyType> columns) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + schema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + schema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex schemaVertex = schemas.get(0);

            List<Vertex> outVertices = traversalSource.V(schemaVertex)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyOut.getTable())
                    .toList();
            Preconditions.checkState(!outVertices.isEmpty(), "Out vertex " + foreignKeyOut.toString() + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(outVertices.size() == 1, "Multiple out vertices " + foreignKeyOut.toString() + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX));
            Vertex outVertex = outVertices.get(0);

            //Get the schema of the in vertex
            schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeyIn.getSchema())
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + schema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + schema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex schemaInVertex = schemas.get(0);

            List<Vertex> inVertices = traversalSource.V(schemaInVertex)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyIn.getTable())
                    .toList();
            Preconditions.checkState(!inVertices.isEmpty(), "In vertex " + foreignKeyIn.toString() + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(inVertices.size() == 1, "Multiple in vertices " + foreignKeyIn.toString() + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex inVertex = inVertices.get(0);

            Vertex edgeVertex = sqlgGraph.addVertex(
                    T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL,
                    "name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length()),
                    CREATED_ON, LocalDateTime.now()
            );

            outVertex.addEdge(SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertex);
            inVertex.addEdge(SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertex);

            for (Map.Entry<String, PropertyType> columnEntry : columns.entrySet()) {
                Vertex property = sqlgGraph.addVertex(
                        T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_PROPERTY,
                        "name", columnEntry.getKey(),
                        "type", columnEntry.getValue().name(),
                        CREATED_ON, LocalDateTime.now()
                );
                edgeVertex.addEdge(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, property);
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    public static void removeEdgeLabel(SqlgGraph sqlgGraph, EdgeLabel edge){
    	 BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
         try {
             GraphTraversalSource traversalSource = sqlgGraph.topology();
             List<Vertex> edges=
	             traversalSource.V()
		          	.hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
		          	.has("name", edge.getSchema().getName())
		         	.out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
		         	.out(SQLG_SCHEMA_OUT_EDGES_EDGE)
		         	.has("name",edge.getName()).toList();
	         if (edges.size()>0){
	        	 Vertex edgeV=edges.get(0);
             
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
    
    public static void addLabelToEdge(SqlgGraph sqlgGraph, String schema, String prefixedTable, boolean in, SchemaTable foreignKey) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + schema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + schema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);

            String foreignKeySchema = foreignKey.getSchema();
            schemas = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", foreignKeySchema)
                    .toList();
            Preconditions.checkState(!schemas.isEmpty(), SCHEMA + foreignKeySchema + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(schemas.size() == 1, MULTIPLE + foreignKeySchema + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex foreignKeySchemaVertex = schemas.get(0);

            Preconditions.checkState(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX));
            List<Vertex> edgeVertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length())).as("a")
                    .in(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", schema)
                    .<Vertex>select("a")
                    .dedup()
                    .toList();
            Preconditions.checkState(!edgeVertices.isEmpty(), "Edge vertex " + foreignKey.toString() + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(edgeVertices.size() == 1, "Multiple edge vertices " + foreignKey.toString() + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Vertex edgeVertex = edgeVertices.get(0);

            String foreignKeyVertexTable;
            if (in) {
                foreignKeyVertexTable = foreignKey.getTable().substring(0, foreignKey.getTable().length() - SchemaManager.IN_VERTEX_COLUMN_END.length());
            } else {
                foreignKeyVertexTable = foreignKey.getTable().substring(0, foreignKey.getTable().length() - SchemaManager.OUT_VERTEX_COLUMN_END.length());
            }
            List<Vertex> foreignKeyVertices = traversalSource.V(foreignKeySchemaVertex)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", foreignKeyVertexTable)
                    .toList();
            Preconditions.checkState(!foreignKeyVertices.isEmpty(), "Out vertex " + foreignKey.toString() + DOES_NOT_EXIST_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(foreignKeyVertices.size() == 1, "Multiple out vertices " + foreignKey.toString() + FOUND_IN_SQLG_S_TOPOLOGY_BUG);
            Preconditions.checkState(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX));
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
            Preconditions.checkArgument(prefixedTable.startsWith(SchemaManager.VERTEX_PREFIX), "prefixedTable must be for a vertex. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();

            List<Vertex> vertices = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
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
            Preconditions.checkArgument(prefixedTable.startsWith(SchemaManager.VERTEX_PREFIX), "prefixedTable must be for a vertex. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();

            traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA)
                    .has("name", schema)
                    .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", prefixedTable.substring(SchemaManager.VERTEX_PREFIX.length()))
                    .out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
                    .has("name",column)
                    .drop().iterate();
            
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }

    }
    
    public static void removeEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, String column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX), "prefixedTable must be for an edge. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();

            traversalSource.V()
            		.hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
            		.has("name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length()))
            		.as("a")
                    .in(SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .has("name", schema)
                    .select("a")
                    .out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                    .has("name",column)
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
            
            AbstractLabel abstractLabel=index.getParentLabel();
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
            for (PropertyColumn property : index.getProperties()) {
                List<Vertex> propertyVertexes = traversalSource.V(abstractLabelVertex)
                        .out(abstractLabel instanceof VertexLabel ? SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE : SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                        .has("name", property.getName())
                        .toList();
                Preconditions.checkState(!propertyVertexes.isEmpty(), "Property %s for AbstractLabel %s.%s does not exists", property.getName(), abstractLabel.getSchema().getName(), abstractLabel.getLabel());
                Preconditions.checkState(propertyVertexes.size() == 1, "BUG: multiple Properties %s found for AbstractLabels found for %s.%s", property.getName(), abstractLabel.getSchema().getName(), abstractLabel.getLabel());
                Vertex propertyVertex = propertyVertexes.get(0);
                indexVertex.addEdge(SQLG_SCHEMA_INDEX_PROPERTY_EDGE, propertyVertex);
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
            
            AbstractLabel abstractLabel=index.getParentLabel();
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
        	if (abstractLabelVertexes.size()>0){
        		Vertex v=abstractLabelVertexes.get(0);
        		traversalSource.V(v.id())
        			.out(abstractLabel instanceof VertexLabel?SQLG_SCHEMA_VERTEX_INDEX_EDGE:SQLG_SCHEMA_EDGE_INDEX_EDGE)
        			.has(SQLG_SCHEMA_INDEX_NAME,index.getName())
        			.out(SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
        			.drop()
        			.iterate();
        		traversalSource.V(v.id())
	    			.out(abstractLabel instanceof VertexLabel?SQLG_SCHEMA_VERTEX_INDEX_EDGE:SQLG_SCHEMA_EDGE_INDEX_EDGE)
	    			.has(SQLG_SCHEMA_INDEX_NAME,index.getName())
	    			.drop()
	    			.iterate();
        	}
            
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }
    
    /**
     * add an index from information schema
     * @param sqlgGraph the graph
     * @param schema the schema name
     * @param label the label name
     * @param vertex is it a vertex or an edge label?
     * @param index the index name
     * @param indexType index type
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
            Vertex indexVertex =  null;
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
                    indexVertex.addEdge(SQLG_SCHEMA_INDEX_PROPERTY_EDGE, propertyVertex);
                }
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    static void addEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map<String, PropertyType> column) {
        for (Map.Entry<String, PropertyType> stringPropertyTypeEntry : column.entrySet()) {
            addEdgeColumn(sqlgGraph, schema, prefixedTable, stringPropertyTypeEntry);
        }
    }

    public static void addEdgeColumn(SqlgGraph sqlgGraph, String schema, String prefixedTable, Map.Entry<String, PropertyType> column) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            Preconditions.checkArgument(prefixedTable.startsWith(SchemaManager.EDGE_PREFIX), "prefixedTable must be for an edge. prefixedTable = " + prefixedTable);
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            Set<Vertex> edges = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                    .has("name", prefixedTable.substring(SchemaManager.EDGE_PREFIX.length()))
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
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }

    static void addGlobalUniqueIndex(SqlgGraph sqlgGraph, String globalUniqueIndexName, Set<PropertyColumn> properties) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> uniquePropertyConstraints = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX)
                    .has("name", globalUniqueIndexName)
                    .toList();
            if (uniquePropertyConstraints.size() > 0) {
                throw new IllegalStateException("Unique property constraint with name already exists. name = " + globalUniqueIndexName);
            }
            Vertex globalUniquePropertyConstraint = sqlgGraph.addVertex(
                    T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
                    "name", globalUniqueIndexName,
                    CREATED_ON, LocalDateTime.now()
            );
            for (PropertyColumn property : properties) {
                String elementLabel = property.getParentLabel().getLabel();
                List<Vertex> uniquePropertyConstraintProperty;
                if (property.getParentLabel() instanceof VertexLabel) {
                    uniquePropertyConstraintProperty = traversalSource.V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL)
                            .has("name", elementLabel)
                            .out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE)
                            .has("name", property.getName())
                            .toList();
                } else {
                    Set<Vertex> edges = traversalSource.V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL)
                            .has("name", elementLabel)
                            .as("a")
                            .in(SQLG_SCHEMA_OUT_EDGES_EDGE)
                            .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                            .has("name", property.getParentLabel().getSchema().getName())
                            .<Vertex>select("a")
                            .toSet();
                    if (edges.size() == 0) {
                        throw new IllegalStateException(String.format("Found no edge for %s.%s", property.getParentLabel().getSchema().getName(), elementLabel));
                    }
                    if (edges.size() > 1) {
                        throw new IllegalStateException(String.format("Found more than one edge for %s.%s", property.getParentLabel().getSchema().getName(), elementLabel));
                    }
                    Vertex edge = edges.iterator().next();
                    uniquePropertyConstraintProperty = traversalSource.V(edge)
                            .out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                            .has("name", property.getName())
                            .toList();
                }
                if (uniquePropertyConstraintProperty.size() == 0) {
                    throw new IllegalStateException(String.format("Found no Property for %s.%s.%s", property.getParentLabel().getSchema().getName(), property.getParentLabel().getLabel(), property.getName()));
                }
                Vertex propertyVertex = uniquePropertyConstraintProperty.get(0);
                globalUniquePropertyConstraint.addEdge(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE, propertyVertex);
            }
        } finally {
            sqlgGraph.tx().batchMode(batchModeType);
        }
    }
    
    static void removeGlobalUniqueIndex(SqlgGraph sqlgGraph, String globalUniqueIndexName) {
        BatchManager.BatchModeType batchModeType = flushAndSetTxToNone(sqlgGraph);
        try {
            GraphTraversalSource traversalSource = sqlgGraph.topology();
            List<Vertex> uniquePropertyConstraints = traversalSource.V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX)
                    .has("name", globalUniqueIndexName)
                    .toList();
            if (uniquePropertyConstraints.size() > 0) {
            	traversalSource.V(uniquePropertyConstraints.get(0))
            		.out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE)
            		.drop()
            		.iterate();
            	traversalSource.V(uniquePropertyConstraints.get(0))
        			.drop()
        			.iterate();
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
}
