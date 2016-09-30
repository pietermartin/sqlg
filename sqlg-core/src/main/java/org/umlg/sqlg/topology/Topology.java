package org.umlg.sqlg.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.umlg.sqlg.structure.SchemaManager.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Topology {

    private SqlgGraph sqlgGraph;
    private boolean distributed;
    private ReentrantLock schemaLock;

    //Map the topology. This is for regular schemas. i.e. 'public.Person', 'special.Car'
    private Map<String, Schema> schemas = new HashMap<>();
    private Map<String, Schema> uncommittedSchemas = new HashMap<>();

    //meta schema
    private Map<String, Schema> metaSchemas = new HashMap<>();

    //Map the topology's topology. This is the

    private static final int LOCK_TIMEOUT = 10;

    public Topology(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.distributed = sqlgGraph.configuration().getBoolean(SqlgGraph.DISTRIBUTED, false);
        this.schemaLock = new ReentrantLock();
        Schema sqlgSchema = Schema.createSqlgSchema(this);
        this.metaSchemas.put(SQLG_SCHEMA, sqlgSchema);

        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        VertexLabel schemaVertexLabel = sqlgSchema.createVertexLabel(sqlgGraph, SQLG_SCHEMA_SCHEMA, columns);
        VertexLabel edgeVertexLabel = sqlgSchema.createVertexLabel(sqlgGraph, SQLG_SCHEMA_EDGE_LABEL, columns);

        columns.put(SQLG_SCHEMA_PROPERTY_TYPE, PropertyType.STRING);
        VertexLabel propertyVertexLabel = sqlgSchema.createVertexLabel(sqlgGraph, SQLG_SCHEMA_PROPERTY, columns);
        columns.remove(SQLG_SCHEMA_PROPERTY_TYPE);

        columns.put(SCHEMA_VERTEX_DISPLAY, PropertyType.STRING);
        VertexLabel vertexVertexLabel = sqlgSchema.createVertexLabel(sqlgGraph, SQLG_SCHEMA_VERTEX_LABEL, columns);

        columns.remove(SCHEMA_VERTEX_DISPLAY);

        EdgeLabel schemaVertexEdgeLabel = schemaVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertexVertexLabel, columns);
        sqlgSchema.addEdgeLabel(schemaVertexEdgeLabel);

        EdgeLabel schemaVertexInEdgeLabel = vertexVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertexLabel, columns);
        sqlgSchema.addEdgeLabel(schemaVertexInEdgeLabel);

        EdgeLabel schemaVertexOutEdgeLabel = vertexVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertexLabel, columns);
        sqlgSchema.addEdgeLabel(schemaVertexOutEdgeLabel);

        EdgeLabel schemaVertexPropertyEdgeLabel = vertexVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, propertyVertexLabel, columns);
        sqlgSchema.addEdgeLabel(schemaVertexPropertyEdgeLabel);

        EdgeLabel schemaEdgePropertyEdgeLabel = edgeVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, propertyVertexLabel, columns);
        sqlgSchema.addEdgeLabel(schemaEdgePropertyEdgeLabel);

        //add the public schema
        this.schemas.put(sqlgGraph.getSqlDialect().getPublicSchema(), Schema.createPublicSchema(this, sqlgGraph.getSqlDialect().getPublicSchema()));
    }

    /**
     * Global lock on the topology.
     * For distributed graph (multiple jvm) this happens on the db via a lock sql statement.
     */
    public void lock() {
        if (!isHeldByCurrentThread()) {
            try {
                if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                    throw new RuntimeException("timeout lapsed to acquire lock schema creation.");
                }
                if (this.distributed) {
                    ((SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect()).lock(this.sqlgGraph);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    boolean isHeldByCurrentThread() {
        return this.schemaLock.isHeldByCurrentThread();
    }

    public boolean existSchema(String schema) {
        return getSchema(schema).isPresent();
    }

    public Optional<Schema> getSchema(String schema) {
        Schema result = this.schemas.get(schema);
        if (result == null) {
            result = this.uncommittedSchemas.get(schema);
            if (result == null) {
                result = this.metaSchemas.get(schema);
            }
        }
        return Optional.ofNullable(result);
    }

    public Optional<VertexLabel> getVertexLabel(String schemaName, String label) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), String.format("vertex label may not start with %s", VERTEX_PREFIX));
        Optional<Schema> schemaOptional = this.getSchema(schemaName);
        if (schemaOptional.isPresent()) {
            return schemaOptional.get().getVertexLabel(label);
        } else {
            return Optional.empty();
        }
    }

    public boolean existVertexLabel(String schemaName, String label) {
        return getVertexLabel(schemaName, label).isPresent();
    }

    public void createVertexLabel(String schemaName, String label, ConcurrentHashMap<String, PropertyType> columns) {
        Preconditions.checkArgument(!schemaName.equals(SQLG_SCHEMA), "createVertexLabel may not be called for %s as its pre-created.", SQLG_SCHEMA);
        Optional<Schema> schemaOptional = this.getSchema(schemaName);
        Schema schema;
        if (!schemaOptional.isPresent()) {
            schema = Schema.createSchema(this.sqlgGraph, this, schemaName);
            this.uncommittedSchemas.put(schemaName, schema);
        } else {
            schema = schemaOptional.get();
        }
        schema.createVertexLabel(this.sqlgGraph, label, columns);
    }

    public Optional<EdgeLabel> getEdgeLabel(String edgeLabelName) {
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edge label name may not start with %s", EDGE_PREFIX);
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            Optional<EdgeLabel> edgeLabel = schemaEntry.getValue().getEdgeLabel(edgeLabelName);
            if (edgeLabel.isPresent()) {
                return edgeLabel;
            }
        }
        if (this.isHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                Optional<EdgeLabel> edgeLabel = schemaEntry.getValue().getEdgeLabel(edgeLabelName);
                if (edgeLabel.isPresent()) {
                    return edgeLabel;
                }
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            Optional<EdgeLabel> edgeLabel = schemaEntry.getValue().getEdgeLabel(edgeLabelName);
            if (edgeLabel.isPresent()) {
                return edgeLabel;
            }
        }
        return Optional.empty();
    }

    /**
     * Checks if the edge already exists.
     *
     * @param edgeLabelName The edge label to check for existence.
     * @return Returns true if the label exists.
     */
    public boolean existEdgeLabel(String edgeLabelName) {
        return getEdgeLabel(edgeLabelName).isPresent();
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public void createEdgeLabel(String edgeLabelName, SchemaTable foreignKeyOut, SchemaTable foreignKeyIn, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(this.isHeldByCurrentThread(), "Lock must be held by the thread to call createEdgeLabel");
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edgeLabelName may not start with " + EDGE_PREFIX);

        Optional<Schema> outVertexSchema = findVertexSchema(foreignKeyOut.getSchema());
        Preconditions.checkState(outVertexSchema.isPresent(), "schema not found for \"%s\"", foreignKeyOut.getSchema());
        Optional<Schema> inVertexSchema = findVertexSchema(foreignKeyIn.getSchema());
        Preconditions.checkState(inVertexSchema.isPresent(), "schema not found for \"%s\"", foreignKeyIn.getSchema());

        //The out and in vertex labels must already exist.
        Optional<VertexLabel> outVertexLabel = outVertexSchema.get().getVertexLabel(foreignKeyOut.getTable());
        Preconditions.checkState(outVertexLabel.isPresent(), "BUG: Out vertex label for edge creation can not be null. out vertex label = \"%s\"", foreignKeyOut.getTable());

        Optional<VertexLabel> inVertexLabel = inVertexSchema.get().getVertexLabel(foreignKeyIn.getTable());
        Preconditions.checkState(inVertexLabel.isPresent(), "BUG: In vertex label for edge creation can not be null. in vertex label = \"%s\"", foreignKeyIn.getTable());

        //Edge may not already exist.
        Preconditions.checkArgument(!existEdgeLabel(edgeLabelName), "Edge \"%s\" already exists!", edgeLabelName);

        if (!outVertexSchema.get().isSqlgSchema()) {
            TopologyManager.addEdgeLabel(this.sqlgGraph, outVertexSchema.get().getName(), EDGE_PREFIX + edgeLabelName, foreignKeyIn, foreignKeyOut, columns);
        }

        EdgeLabel edgeLabel = outVertexLabel.get().addEdgeLabel(sqlgGraph, edgeLabelName, inVertexLabel.get(), columns);
        outVertexSchema.get().addEdgeLabel(edgeLabel);

    }

    private Optional<Schema> findVertexSchema(String schemaName) {
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            if (schemaEntry.getValue().getName().equals(schemaName)) {
                return Optional.of(schemaEntry.getValue());
            }
        }
        if (this.isHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                if (schemaEntry.getValue().getName().equals(schemaName)) {
                    return Optional.of(schemaEntry.getValue());
                }
            }
        }
        return Optional.empty();
    }

    public void ensureVertexColumnsExist(String schemaName, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        if (!schemaName.equals(SQLG_SCHEMA)) {
            Schema schema = this.schemas.get(schemaName);
            if (schema == null) {
                schema = this.uncommittedSchemas.get(schemaName);
            }
            if (schema == null) {
                throw new IllegalStateException(String.format("BUG: schema %s can not be null", schemaName));
            }
            //createVertexLabel the table
            schema.ensureVertexColumnsExist(this.sqlgGraph, label, columns);
        }
    }

    public void ensureEdgeColumnsExist(String schemaName, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        if (!schemaName.equals(SQLG_SCHEMA)) {
            Schema schema = this.schemas.get(schemaName);
            if (schema == null) {
                schema = this.uncommittedSchemas.get(schemaName);
            }
            if (schema == null) {
                throw new IllegalStateException(String.format("BUG: schema %s can not be null", schemaName));
            }
            //createVertexLabel the table
            schema.ensureEdgeColumnsExist(this.sqlgGraph, label, columns);
        }
    }

    public void afterCommit() {
        for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Schema> entry = it.next();
            this.schemas.put(entry.getKey(), entry.getValue());
            it.remove();
        }
        for (Schema schema : this.schemas.values()) {
            schema.afterCommit();
        }
    }

    public void afterRollback() {
        for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Schema> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        for (Schema schema : this.schemas.values()) {
            schema.afterRollback();
        }
    }

    public Map<String, Map<String, PropertyType>> getAllTablesWithout(List<String> filter) {
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTablesWithout(filter));
        }
        //TODO
//        result.putAll(this.temporaryTables);
        if (!this.uncommittedSchemas.isEmpty() && isHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                result.putAll(schemaEntry.getValue().getAllTablesWithout(filter));
            }
        }
        //And the meta schema tables
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTablesWithout(filter));
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, Map<String, PropertyType>> getAllTables() {
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTables());
        }
        //TODO
//        result.putAll(this.temporaryTables);
        if (!this.uncommittedSchemas.isEmpty() && isHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                result.putAll(schemaEntry.getValue().getAllTables());
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTables());
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, Map<String, PropertyType>> getAllTablesFrom(List<String> selectFrom) {
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTablesFrom(selectFrom));
        }
        //TODO
//        result.putAll(this.temporaryTables);
        if (!this.uncommittedSchemas.isEmpty() && isHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                result.putAll(schemaEntry.getValue().getAllTablesFrom(selectFrom));
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTablesFrom(selectFrom));
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Map<String, PropertyType> result = new HashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                result.putAll(schemaEntry.getValue().getTableFor(schemaTable));
            }
        }
        //TODO
//        result.putAll(this.temporaryTables);
        if (!this.uncommittedSchemas.isEmpty() && isHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                    result.putAll(schemaEntry.getValue().getTableFor(schemaTable));
                }
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                result.putAll(schemaEntry.getValue().getTableFor(schemaTable));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> map = new HashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels();
            map.putAll(result);
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels();
            map.putAll(result);
        }
        return map;
    }

    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels(SchemaTable schemaTable) {
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                Optional<Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels(schemaTable);
                if (result.isPresent()) {
                    return result.get();
                }
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                Optional<Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels(schemaTable);
                if (result.isPresent()) {
                    return result.get();
                }
            }
        }
        return Pair.of(
                Collections.emptySet(),
                Collections.emptySet());
    }

    public Map<String, Set<String>> getAllEdgeForeignKeys() {
        Map<String, Set<String>> result = new HashMap<>();
        for (Schema schema : this.schemas.values()) {
            result.putAll(schema.getAllEdgeForeignKeys());
        }
        for (Schema schema : this.metaSchemas.values()) {
            result.putAll(schema.getAllEdgeForeignKeys());
        }
        return result;
    }

    public void loadUserSchema() {
        GraphTraversalSource traversalSource = this.sqlgGraph.topology();
        List<Vertex> schemaVertices = traversalSource.V().hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA).toList();
        for (Vertex schemaVertex : schemaVertices) {
            String schemaName = schemaVertex.value("name");
            Optional<Schema> schemaOptional = getSchema(schemaName);
            if (schemaName.equals(SQLG_SCHEMA)) {
                Preconditions.checkState(schemaOptional.isPresent(), "\"public\" schema must always be present.");
            }
            Schema schema;
            if (!schemaOptional.isPresent()) {
                schema = Schema.loadUserSchema(this, schemaName);
                this.schemas.put(schemaName, schema);
            } else {
                schema = schemaOptional.get();

            }
            schema.loadVertexAndEdgeLabels(traversalSource, schemaVertex);
        }
    }

}
