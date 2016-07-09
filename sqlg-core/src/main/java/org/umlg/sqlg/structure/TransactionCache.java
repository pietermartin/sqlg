package org.umlg.sqlg.structure;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * A transaction scoped cache.
 * Date: 2014/10/04
 * Time: 3:20 PM
 */
class TransactionCache {

    private Connection connection;
    private Map<ElementPropertyRollback, Object> elementPropertyRollbackFunctions = new WeakHashMap<>();
    private BatchManager batchManager;
    private boolean cacheVertices = false;
    private Map<RecordId, SqlgVertex> vertexCache = new WeakHashMap<>();

    static TransactionCache of(boolean cacheVertices, Connection connection, BatchManager batchManager) {
        return new TransactionCache(cacheVertices, connection, batchManager);
    }

    private TransactionCache(
            boolean cacheVertices,
            Connection connection,
            BatchManager batchManager) {

        this.cacheVertices = cacheVertices;
        this.connection = connection;
        this.batchManager = batchManager;
    }

    Connection getConnection() {
        return this.connection;
    }

    Map<ElementPropertyRollback, Object> getElementPropertyRollback() {
        return this.elementPropertyRollbackFunctions;
    }

    BatchManager getBatchManager() {
        return this.batchManager;
    }

    void clear() {
        this.elementPropertyRollbackFunctions.clear();
        this.batchManager.clear();
        if (this.cacheVertices) {
            this.vertexCache.clear();
        }
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The recordId is not referenced in the SqlgVertex.
     * It is important that the value of the WeakHashMap does not reference the key.
     *
     * @param sqlgGraph The graph
     * @return the vertex. If cacheVertices is true and the vertex is cached then the cached vertex will be returned else
     * a the vertex will be instantiated.
     *
     */
    SqlgVertex putVertexIfAbsent(SqlgGraph sqlgGraph, String schema, String table, Long id) {
        RecordId recordId = RecordId.from(SchemaTable.of(schema, table), id);
        SqlgVertex sqlgVertex;
        if (this.cacheVertices) {
            sqlgVertex = this.vertexCache.get(recordId);
            if (sqlgVertex == null) {
                sqlgVertex = new SqlgVertex(sqlgGraph, id, schema, table);
                this.vertexCache.put(recordId, sqlgVertex);
                return sqlgVertex;
            }
        } else {
            sqlgVertex = new SqlgVertex(sqlgGraph, id, schema, table);
        }
        return sqlgVertex;
    }

    SqlgVertex putVertexIfAbsent(SqlgVertex sqlgVertex) {
        RecordId vertexRecordId = (RecordId)sqlgVertex.id();
        SqlgVertex sqlgVertexFromCache;
        if (this.cacheVertices) {
            sqlgVertexFromCache = this.vertexCache.get(vertexRecordId);
            if (sqlgVertexFromCache == null) {
                //copy the RecordId so that the WeakHashMap value does not reference the key
                SchemaTable schemaTable = vertexRecordId.getSchemaTable();
                RecordId recordId = RecordId.from(SchemaTable.of(schemaTable.getSchema(), schemaTable.getTable()), vertexRecordId.getId());
                this.vertexCache.put(recordId, sqlgVertex);
                return sqlgVertex;
            } else {
                return sqlgVertexFromCache;
            }

        } else {
            return sqlgVertex;
        }
    }

    void add(SqlgVertex sqlgVertex) {
        RecordId vertexRecordId = (RecordId) sqlgVertex.id();
        if (this.vertexCache.containsKey(vertexRecordId)) {
            throw new IllegalStateException("The vertex cache should never already contain a new vertex!");
        } else {
            SchemaTable schemaTable = vertexRecordId.getSchemaTable();
            RecordId recordId = RecordId.from(SchemaTable.of(schemaTable.getSchema(), schemaTable.getTable()), vertexRecordId.getId());
            this.vertexCache.put(recordId, sqlgVertex);
        }
    }

}
