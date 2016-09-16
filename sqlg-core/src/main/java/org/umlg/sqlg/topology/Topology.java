package org.umlg.sqlg.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.umlg.sqlg.structure.SchemaManager.*;
import static org.umlg.sqlg.topology.Schema.createSchema;

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

    //Map the edges.
    //An edge can be across many schemas.
    //As such it will be created in the first outVertex that creates the edge.
    private Map<String, EdgeLabel> edges = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedEdges = new HashMap<>();

    private Map<String, EdgeLabel> metaEdges = new HashMap<>();

    //Map the topology's topology. This is the

    private static final int LOCK_TIMEOUT = 10;

    public Topology(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.distributed = sqlgGraph.configuration().getBoolean(SqlgGraph.DISTRIBUTED);
        this.schemaLock = new ReentrantLock();
        Schema sqlgSchema = Schema.createSchema(sqlgGraph, this, SQLG_SCHEMA);
        this.metaSchemas.put(SQLG_SCHEMA, sqlgSchema);

        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        VertexLabel schemaVertexLabel = sqlgSchema.createVertexLabel(sqlgGraph, SQLG_SCHEMA_SCHEMA, columns);
        VertexLabel edgeVertexLabel = sqlgSchema.createVertexLabel(sqlgGraph, SQLG_SCHEMA_EDGE_LABEL, columns);
        VertexLabel propertyVertexLabel = sqlgSchema.createVertexLabel(sqlgGraph, SQLG_SCHEMA_PROPERTY, columns);

        columns.put(SCHEMA_VERTEX_DISPLAY, PropertyType.STRING);
        VertexLabel vertexVertexLabel = sqlgSchema.createVertexLabel(sqlgGraph, SQLG_SCHEMA_VERTEX_LABEL, columns);

        columns.remove(SCHEMA_VERTEX_DISPLAY);

        EdgeLabel schemaVertexEdgeLabel = schemaVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertexVertexLabel, columns);
        this.metaEdges.put(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, schemaVertexEdgeLabel);

        EdgeLabel schemaVertexInEdgeLabel = vertexVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertexLabel, columns);
        this.metaEdges.put(SQLG_SCHEMA_IN_EDGES_EDGE, schemaVertexInEdgeLabel);

        EdgeLabel schemaVertexOutEdgeLabel = vertexVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertexLabel, columns);
        this.metaEdges.put(SQLG_SCHEMA_OUT_EDGES_EDGE, schemaVertexOutEdgeLabel);

        EdgeLabel schemaVertexPropertyEdgeLabel = vertexVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, propertyVertexLabel, columns);
        this.metaEdges.put(SQLG_SCHEMA_OUT_EDGES_EDGE, schemaVertexPropertyEdgeLabel);

        EdgeLabel schemaEdgePropertyEdgeLabel = edgeVertexLabel.addEdgeLabel(sqlgGraph, SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, propertyVertexLabel, columns);
        this.metaEdges.put(SQLG_SCHEMA_OUT_EDGES_EDGE, schemaEdgePropertyEdgeLabel);

        //add the public schema
        this.schemas.put(sqlgGraph.getSqlDialect().getPublicSchema(), Schema.createSchema(sqlgGraph, this, sqlgGraph.getSqlDialect().getPublicSchema()));
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
        return this.schemas.containsKey(schema) || this.uncommittedSchemas.containsKey(schema);
    }

    /**
     * A vertex always belongs to a schema. Unlike an edge which can join vertices from different schemas.
     * This is a transactional check. Returns true if the label already exists or has been created in this thread
     * for the current transaction.
     *
     * @param schemaName The schema the vertex belongs to.
     * @param label      The vertex's label prefixed with
     * @return true if the label exists.
     */
    public boolean existVertexLabel(String schemaName, String label) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), String.format("vertex label may not start with %s", VERTEX_PREFIX));
        if (schemaName.equals(SQLG_SCHEMA)) {
            return true;
        } else if (existSchema(schemaName)) {
            //first check already existing schemas
            Schema schema = this.schemas.get(schemaName);
            if (schema != null) {
                return schema.existVertexLabel(label);
            } else {
                //check uncommitted schemas created in this transaction
                schema = this.uncommittedSchemas.get(schemaName);
                return schema.existVertexLabel(label);
            }
        } else {
            return false;
        }
    }

    public void createVertexLabel(String schemaName, String label, ConcurrentHashMap<String, PropertyType> columns) {
        if (!schemaName.equals(SQLG_SCHEMA)) {
            Schema schema = this.schemas.get(schemaName);
            if (schema == null) {
                schema = this.uncommittedSchemas.get(schemaName);
                if (schema == null) {
                    //createVertexLabel the schema
                    schema = createSchema(this.sqlgGraph, this, schemaName);
                    this.uncommittedSchemas.put(schemaName, schema);
                }
            }
            //createVertexLabel the table
            schema.createVertexLabel(this.sqlgGraph, label, columns);
        }
    }

    /**
     * Checks if the edge already exists.
     *
     * @param edgeLabel The edge label to check for existence.
     * @return Returns true if the label exists.
     */
    public boolean existEdgeLabel(String edgeLabel) {
        Preconditions.checkArgument(!edgeLabel.startsWith(EDGE_PREFIX), String.format("edge label name may not start with %s", EDGE_PREFIX));
        return this.edges.containsKey(edgeLabel) || this.uncommittedEdges.containsKey(edgeLabel) || this.metaEdges.containsKey(edgeLabel);
    }

    public void createEdgeLabel(String edgeLabelName, SchemaTable foreignKeyOut, SchemaTable foreignKeyIn, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(this.isHeldByCurrentThread(), "Lock must be held by the thread to call createEdgeLabel");
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edgeLabelName may not start with " + EDGE_PREFIX);

        Schema schema;
        if (foreignKeyOut.getSchema().equals(SQLG_SCHEMA)) {
            schema = this.metaSchemas.get(foreignKeyOut.getSchema());
            if (schema == null) {
                throw new IllegalStateException(String.format("BUG: %s can not be null.", foreignKeyOut.getSchema()));
            }
        } else {
            schema = this.schemas.get(foreignKeyOut.getSchema());
            if (schema == null) {
                schema = this.uncommittedSchemas.get(foreignKeyOut.getSchema());
                if (schema == null) {
                    throw new IllegalStateException(String.format("BUG: Schema can not be null. Schema = %s", foreignKeyOut.getSchema()));
                }
            }
        }

        //The out and in vertex labels must already exist.
        VertexLabel outVertexLabel = schema.getVertexLabel(foreignKeyOut.getTable());
        if (outVertexLabel == null) {
            throw new IllegalStateException(String.format("BUG: Out vertex label for edge creation can not be null. out vertex label = %s", foreignKeyOut.getTable()));
        }
        VertexLabel inVertexLabel = schema.getVertexLabel(foreignKeyIn.getTable());
        if (inVertexLabel == null) {
            throw new IllegalStateException(String.format("BUG: In vertex label for edge creation can not be null. in vertex label = %s", foreignKeyIn.getTable()));
        }
        //Edge may not already exist.
        Preconditions.checkArgument(!existEdgeLabel(edgeLabelName), String.format("Edge %s already exists!", edgeLabelName));

        if (!SQLG_SCHEMA.equals(schema.getName())) {
            TopologyManager.addEdgeLabel(this.sqlgGraph, schema.getName(), EDGE_PREFIX + edgeLabelName, foreignKeyIn, foreignKeyOut, columns);
        }

        EdgeLabel edgeLabel = inVertexLabel.addEdgeLabel(sqlgGraph, edgeLabelName, outVertexLabel, columns);

    }

    public void afterCommit() {
        for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Schema> entry = it.next();
            this.schemas.put(entry.getKey(), entry.getValue());
            it.remove();
        }
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            schemaEntry.getValue().afterCommit();
        }
    }

    public void afterRollback() {
        for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Schema> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            schemaEntry.getValue().afterRollback();
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

//        Map<String, PropertyType> result = this.tables.get(schemaTable.toString());
//        if (!this.uncommittedTables.isEmpty() && this.isLockedByCurrentThread()) {
//            Map<String, PropertyType> tmp = this.uncommittedTables.get(schemaTable.toString());
//            if (tmp != null) {
//                result = tmp;
//            }
//        }
//        if (result == null) {
//            return Collections.emptyMap();
//        } else {
//            return Collections.unmodifiableMap(result);
//        }

    }

    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels(SchemaTable schemaTable) {
//        Pair<Set<SchemaTable>, Set<SchemaTable>> result = this.tableLabels.get(schemaTable);
//        if (result == null) {
//            if (!this.uncommittedTableLabels.isEmpty() && this.isLockedByCurrentThread()) {
//                Pair<Set<SchemaTable>, Set<SchemaTable>> pair = this.uncommittedTableLabels.get(schemaTable);
//                if (pair != null) {
//                    return Pair.of(Collections.unmodifiableSet(pair.getLeft()), Collections.unmodifiableSet(pair.getRight()));
//                }
//            }
//            return Pair.of(Collections.EMPTY_SET, Collections.EMPTY_SET);
//        } else {
//            Set<SchemaTable> left = new HashSet<>(result.getLeft());
//            Set<SchemaTable> right = new HashSet<>(result.getRight());
//            if (!this.uncommittedTableLabels.isEmpty() && this.isLockedByCurrentThread()) {
//                Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedLabels = this.uncommittedTableLabels.get(schemaTable);
//                if (uncommittedLabels != null) {
//                    left.addAll(uncommittedLabels.getLeft());
//                    right.addAll(uncommittedLabels.getRight());
//                }
//            }
//            return Pair.of(
//                    Collections.unmodifiableSet(left),
//                    Collections.unmodifiableSet(right));
//        }

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
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {

            result.putAll(schemaEntry.getValue().getAllEdgeForeignKeys());

        }

        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {

            result.putAll(schemaEntry.getValue().getAllEdgeForeignKeys());

        }

        return result;
    }
}
