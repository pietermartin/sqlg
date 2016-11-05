package org.umlg.sqlg.sql.dialect;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.structure.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/**
 * Date: 2016/09/03
 * Time: 2:56 PM
 */
public interface SqlBulkDialect extends SqlDialect {

    String ERROR_MESSAGE = "Batch processing is not supported by ";


    default Map<SchemaTable, Pair<Long, Long>> flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default void flushEdgeCache(SqlgGraph sqlgGraph, Map<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());

    }

    default String getBatchNull() {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default void flushVertexPropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default void flushEdgePropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default void flushRemovedVertices(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgVertex>> removeVertexCache) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default void flushRemovedEdges(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default <L, R> void bulkAddEdges(SqlgGraph sqlgGraph, SchemaTable in, SchemaTable out, String edgeLabel, Pair<String, String> idFields, Collection<Pair<L, R>> uids) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default String constructCompleteCopyCommandTemporarySqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default String constructCompleteCopyCommandSqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default String constructCompleteCopyCommandSqlEdge(SqlgGraph sqlgGraph, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default void writeStreamingVertex(OutputStream out, Map<String, Object> keyValueMap) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default void writeStreamingEdge(OutputStream out, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) throws IOException {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default String temporaryTableCopyCommandSqlVertex(SqlgGraph sqlgGraph, SchemaTable schemaTable, Map<String, Object> keyValueMap) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default InputStream inputStreamSql(SqlgGraph sqlgGraph, String sql) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default OutputStream streamSql(SqlgGraph sqlgGraph, String sql) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

}
