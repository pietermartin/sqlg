package org.umlg.sqlg.structure;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A transaction scoped cache.
 * Date: 2014/10/04
 * Time: 3:20 PM
 */
public class TransactionCache {

    private Connection connection;
    private List<ElementPropertyRollback> elementPropertyRollbackFunctions;
    private BatchManager batchManager;
    private Map<Long, SqlgVertex> vertexCache = new HashMap<>();

    static TransactionCache of(Connection connection, List<ElementPropertyRollback> elementPropertyRollbackFunctions, BatchManager batchManager) {
        return new TransactionCache(connection, elementPropertyRollbackFunctions, batchManager);
    }

    private TransactionCache(
            Connection connection,
            List<ElementPropertyRollback> elementPropertyRollbackFunctions,
            BatchManager batchManager) {

        this.connection = connection;
        this.elementPropertyRollbackFunctions = elementPropertyRollbackFunctions;
        this.batchManager = batchManager;
    }

    Connection getConnection() {
        return this.connection;
    }

    List<ElementPropertyRollback> getElementPropertyRollback() {
        return this.elementPropertyRollbackFunctions;
    }

    BatchManager getBatchManager() {
        return this.batchManager;
    }

    void putSqlgVertex(SqlgVertex sqlgVertex) {
        this.vertexCache.put((Long)sqlgVertex.id(), sqlgVertex);
    }

    SqlgVertex getSqlgVertex(Long id) {
        return this.vertexCache.get(id);
    }

    void clear() {
        this.elementPropertyRollbackFunctions.clear();
        this.batchManager.clear();
        this.vertexCache.clear();
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    SqlgVertex putVertexIfAbsent(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        if (!this.vertexCache.containsKey(id)) {
            SqlgVertex sqlgVertex = new SqlgVertex(sqlgGraph, id, schema, table);
            this.vertexCache.put(id, sqlgVertex);
            return sqlgVertex;
        } else {
            return this.vertexCache.get(id);
        }
    }

    SqlgVertex putVertexIfAbsent(SqlgVertex sqlgVertex) {
        if (!this.vertexCache.containsKey(sqlgVertex.id())) {
            this.vertexCache.put((Long) sqlgVertex.id(), sqlgVertex);
            return sqlgVertex;
        } else {
            return this.vertexCache.get(sqlgVertex.id());
        }
    }

    void add(SqlgVertex sqlgVertex) {
        if (this.vertexCache.containsKey(sqlgVertex.id())) {
            throw new IllegalStateException("The vertex cache should never already contain a new vertex!");
        } else {
            this.vertexCache.put((Long) sqlgVertex.id(), sqlgVertex);
        }
    }

}
