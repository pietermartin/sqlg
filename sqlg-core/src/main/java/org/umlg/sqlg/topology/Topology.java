package org.umlg.sqlg.topology;

import com.google.common.base.Preconditions;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.SQLG_SCHEMA;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Topology {

    private SqlgGraph sqlgGraph;
    private boolean distributed;
    private ReentrantLock schemaLock;
    private Map<String, Schema> schemas = new HashMap<>();
    private Map<String, Schema> uncommittedSchemas = new HashMap<>();
    private static final int LOCK_TIMEOUT = 10;

    public Topology(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.distributed = sqlgGraph.configuration().getBoolean(SqlgGraph.DISTRIBUTED);
        this.schemaLock = new ReentrantLock();
        init(sqlgGraph);
    }

    /**
     * Global lock on the topology.
     * For distributed graph (multiple jvm) this happens on the db via a lock sql statement.
     */
    public void lock() {
        if (!this.schemaLock.isHeldByCurrentThread()) {
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

    public boolean existSchema(String schema) {
        return this.schemas.containsKey(schema) || this.uncommittedSchemas.containsKey(schema);
    }

    public boolean existVertexTable(String schemaName, String prefixedTable) {
        Preconditions.checkArgument(prefixedTable.startsWith(VERTEX_PREFIX), String.format("Table name must be prefixed with %s", VERTEX_PREFIX));
        if (existSchema(schemaName)) {
            //first check already existing schemas
            Schema schema = this.schemas.get(schemaName);
            if (schema != null) {
                return schema.existVertexLabel(prefixedTable);
            } else {
                //check uncommitted schemas created in this transaction
                schema = this.uncommittedSchemas.get(schemaName);
                return schema.existVertexLabel(prefixedTable);
            }
        } else {
            return false;
        }
    }

    public boolean existEdgeTable(String schemaName, String outVertexTable, String edgeTable) {
        Preconditions.checkArgument(outVertexTable.startsWith(VERTEX_PREFIX), String.format("Table name must be prefixed with %s", VERTEX_PREFIX));
        Preconditions.checkArgument(edgeTable.startsWith(EDGE_PREFIX), String.format("Table name must be prefixed with %s", EDGE_PREFIX));
        if (existSchema(schemaName)) {
            //first check already existing schemas
            Schema schema = this.schemas.get(schemaName);
            if (schema != null) {
                VertexLabel vertexLabel = schema.getVertexLabel(outVertexTable);
                if (vertexLabel != null) {

                    return vertexLabel.existOutEdgeLabel(outVertexTable);

                } else {
                    return false;
                }
            } else {
                //check uncommitted schemas created in this transaction
                schema = this.uncommittedSchemas.get(schemaName);
                VertexLabel vertexLabel = schema.getVertexLabel(outVertexTable);
                if (vertexLabel != null) {

                    return vertexLabel.existOutEdgeLabel(outVertexTable);

                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    public void create(String schemaName, String prefixedTable, ConcurrentHashMap<String, PropertyType> columns) {
        //first check already existing schemas
        Schema schema = this.schemas.get(schemaName);
        if (schema == null) {
            schema = this.uncommittedSchemas.get(schemaName);
            if (schema == null) {
                //create the schema
                schema = Schema.createSchema(this.sqlgGraph, schemaName);
                this.uncommittedSchemas.put(schemaName, schema);
            }
        }
        //create the table
        schema.createVertexLabel(this.sqlgGraph, prefixedTable, columns);
    }

    private void init(SqlgGraph sqlgGraph) {
        this.schemas.put(SQLG_SCHEMA, Schema.createMetaSchema(SQLG_SCHEMA));
    }

    public void afterCommit() {
        for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Schema> entry = it.next();
            this.schemas.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }
    }

    public void afterRollback() {
        for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Schema> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
    }

}
