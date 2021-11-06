package org.umlg.sqlg.sql.dialect;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

import javax.annotation.Nullable;
import java.io.Writer;
import java.util.*;

import static javax.swing.JOptionPane.ERROR_MESSAGE;

/**
 * Date: 2016/09/03
 * Time: 2:56 PM
 */
public interface SqlBulkDialect extends SqlDialect {

    void flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache);

    void flushEdgeCache(SqlgGraph sqlgGraph, Map<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache);

    void flushVertexPropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache);

    void flushEdgePropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache);

    void flushRemovedVertices(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgVertex>> removeVertexCache);

    default void flushRemovedEdges(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default String getBatchNull() {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default <L, R> void bulkAddEdges(SqlgGraph sqlgGraph, SchemaTable in, SchemaTable out, String edgeLabel, Pair<String, String> idFields, Collection<Pair<L, R>> uids, Map<String, PropertyType> edgeColumns, Map<String, Object> edgePropertyMap) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default String constructCompleteCopyCommandTemporarySqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default String constructCompleteCopyCommandSqlVertex(SqlgGraph sqlgGraph, String schema, String table, Set<String> keys) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default String constructCompleteCopyCommandSqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default String constructCompleteCopyCommandSqlEdge(SqlgGraph sqlgGraph, SqlgEdge sqlgEdge, VertexLabel outVertexLabel, VertexLabel inVertexLabel, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default void writeStreamingVertex(Writer writer, Map<String, Object> keyValueMap, @Nullable VertexLabel vertexLabel) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default void writeTemporaryStreamingVertex(Writer writer, Map<String, Object> keyValueMap) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default void writeStreamingEdge(
            Writer writer,
            SqlgEdge sqlgEdge,
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            SqlgVertex outVertex,
            SqlgVertex inVertex,
            Map<String, Object> keyValueMap,
            EdgeLabel edgeLabel) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

    default String temporaryTableCopyCommandSqlVertex(SqlgGraph sqlgGraph, SchemaTable schemaTable, Set<String> keys) {
        throw new UnsupportedOperationException(ERROR_MESSAGE + dialectName());
    }

    default Writer streamSql(SqlgGraph sqlgGraph, String sql) {
        throw SqlgExceptions.batchModeNotSupported(dialectName());
    }

}
