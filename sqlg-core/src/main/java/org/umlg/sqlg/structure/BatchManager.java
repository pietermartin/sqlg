package org.umlg.sqlg.structure;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.sql.dialect.SqlBulkDialect;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * Date: 2014/09/12
 * Time: 5:08 PM
 */
public class BatchManager {

    private SqlgGraph sqlgGraph;
    private SqlBulkDialect sqlDialect;

    //map per label/keys, contains a map of vertices with a triple representing outLabels, inLabels and vertex properties
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache = new HashMap<>();
    //map per label, contains a map edges. The triple is outVertex, inVertex, edge properties

    private Map<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache = new HashMap<>();

    //this is a cache of changes to properties that are already persisted, i.e. not in the vertexCache
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache = new LinkedHashMap<>();
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache = new LinkedHashMap<>();

    //map per label's vertices to delete
    private Map<SchemaTable, List<SqlgVertex>> removeVertexCache = new LinkedHashMap<>();
    //map per label's edges to delete
    private Map<SchemaTable, List<SqlgEdge>> removeEdgeCache = new LinkedHashMap<>();

    private Map<SchemaTable, Writer> streamingVertexOutputStreamCache = new LinkedHashMap<>();
    private Map<SchemaTable, Writer> streamingEdgeOutputStreamCache = new LinkedHashMap<>();

    //indicates what is being streamed
    private SchemaTable streamingBatchModeVertexSchemaTable;
    private List<String> streamingBatchModeVertexKeys;
    private SchemaTable streamingBatchModeEdgeSchemaTable;
    private List<String> streamingBatchModeEdgeKeys;

    private int batchCount;
    private long batchIndex;
    private boolean isBusyFlushing;

    public enum BatchModeType {
        NONE, NORMAL, STREAMING, STREAMING_WITH_LOCK
    }

    private BatchModeType batchModeType = BatchModeType.NONE;

    BatchManager(SqlgGraph sqlgGraph, SqlBulkDialect sqlDialect) {
        this.sqlgGraph = sqlgGraph;
        this.sqlDialect = sqlDialect;
    }

    public boolean isInNormalMode() {
        return this.batchModeType == BatchModeType.NORMAL;
    }

    public boolean isInStreamingMode() {
        return this.batchModeType == BatchModeType.STREAMING;
    }

    public boolean isInStreamingModeWithLock() {
        return this.batchModeType == BatchModeType.STREAMING_WITH_LOCK;
    }

    boolean isInBatchMode() {
        return this.batchModeType != BatchModeType.NONE;
    }

    BatchModeType getBatchModeType() {
        return batchModeType;
    }

    void batchModeOn(BatchModeType batchModeType) {
        this.batchModeType = batchModeType;
    }

    void addTemporaryVertex(SqlgVertex sqlgVertex, Map<String, Object> keyValueMap) {
        SchemaTable schemaTable = SchemaTable.of(sqlgVertex.getSchema(), sqlgVertex.getTable());
        Writer writer = this.streamingVertexOutputStreamCache.get(schemaTable);
        if (writer == null) {
            String sql = this.sqlDialect.constructCompleteCopyCommandTemporarySqlVertex(sqlgGraph, sqlgVertex, keyValueMap);
            writer = this.sqlDialect.streamSql(this.sqlgGraph, sql);
            this.streamingVertexOutputStreamCache.put(schemaTable, writer);
        }
        this.sqlDialect.writeStreamingVertex(writer, keyValueMap);

    }

    void addVertex(boolean temporary, boolean streaming, SqlgVertex sqlgVertex, Map<String, Object> keyValueMap) {
        SchemaTable schemaTable = SchemaTable.of(sqlgVertex.getSchema(), sqlgVertex.getTable(), temporary);
        if (!streaming) {
            Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> pairs = this.vertexCache.get(schemaTable);
            if (pairs == null) {
                pairs = Pair.of(new TreeSet<>(keyValueMap.keySet()), new LinkedHashMap<>());
                pairs.getRight().put(sqlgVertex, keyValueMap);
                this.vertexCache.put(schemaTable, pairs);
            } else {
                pairs.getLeft().addAll(keyValueMap.keySet());
                pairs.getRight().put(sqlgVertex, keyValueMap);
            }
        } else {
            if (this.streamingBatchModeVertexSchemaTable == null) {
                this.streamingBatchModeVertexSchemaTable = sqlgVertex.getSchemaTable();
            }
            if (this.streamingBatchModeVertexKeys == null) {
                this.streamingBatchModeVertexKeys = new ArrayList<>(keyValueMap.keySet());
            }
            if (this.isStreamingEdges()) {
                throw new IllegalStateException("streaming edge is in progress, first flush or commit before streaming vertices.");
            }
            if (this.isInStreamingModeWithLock() && this.batchCount == 0) {
                //lock the table,
                this.sqlDialect.lockTable(sqlgGraph, schemaTable, VERTEX_PREFIX);
                this.batchIndex = this.sqlDialect.nextSequenceVal(sqlgGraph, schemaTable, VERTEX_PREFIX);
            }
            if (this.isInStreamingModeWithLock()) {
                sqlgVertex.setInternalPrimaryKey(RecordId.from(schemaTable, ++this.batchIndex));
            }
            Writer writer = this.streamingVertexOutputStreamCache.get(schemaTable);
            if (writer == null) {
                String sql = this.sqlDialect.constructCompleteCopyCommandSqlVertex(sqlgGraph, sqlgVertex, keyValueMap);
                writer = this.sqlDialect.streamSql(this.sqlgGraph, sql);
                this.streamingVertexOutputStreamCache.put(schemaTable, writer);
            }
            this.sqlDialect.writeStreamingVertex(writer, keyValueMap);
            if (this.isInStreamingModeWithLock()) {
                this.batchCount++;
            }
        }
    }

    void addEdge(boolean streaming, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        SchemaTable outSchemaTable = SchemaTable.of(outVertex.getSchema(), sqlgEdge.getTable());
        MetaEdge metaEdge = MetaEdge.from(outSchemaTable, outVertex, inVertex);
        if (!streaming) {
            Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples = this.edgeCache.get(metaEdge);
            if (triples == null) {
                triples = Pair.of(new TreeSet<>(keyValueMap.keySet()), new LinkedHashMap<>());
                triples.getRight().put(sqlgEdge, Triple.of(outVertex, inVertex, keyValueMap));
                this.edgeCache.put(metaEdge, triples);
            } else {
                triples.getLeft().addAll(keyValueMap.keySet());
                triples.getRight().put(sqlgEdge, Triple.of(outVertex, inVertex, keyValueMap));
            }
        } else {
            if (this.streamingBatchModeEdgeSchemaTable == null) {
                this.streamingBatchModeEdgeSchemaTable = sqlgEdge.getSchemaTablePrefixed();
            }
            if (this.streamingBatchModeEdgeKeys == null) {
                this.streamingBatchModeEdgeKeys = new ArrayList<>(keyValueMap.keySet());
            }
            if (this.isStreamingVertices()) {
                throw new IllegalStateException("streaming vertex is in progress, first flush or commit before streaming edges.");
            }
            if (this.isInStreamingModeWithLock() && this.batchCount == 0) {
                //lock the table,
                this.sqlDialect.lockTable(sqlgGraph, outSchemaTable, EDGE_PREFIX);
                this.batchIndex = this.sqlDialect.nextSequenceVal(sqlgGraph, outSchemaTable, EDGE_PREFIX);
            }
            if (this.isInStreamingModeWithLock()) {
                sqlgEdge.setInternalPrimaryKey(RecordId.from(outSchemaTable, ++this.batchIndex));
            }
            Writer writer = this.streamingEdgeOutputStreamCache.get(outSchemaTable);
            if (writer == null) {
                String sql = this.sqlDialect.constructCompleteCopyCommandSqlEdge(sqlgGraph, sqlgEdge, outVertex, inVertex, keyValueMap);
                writer = this.sqlDialect.streamSql(this.sqlgGraph, sql);
                this.streamingEdgeOutputStreamCache.put(outSchemaTable, writer);
            }
            try {
                this.sqlDialect.writeStreamingEdge(writer, sqlgEdge, outVertex, inVertex, keyValueMap);
                if (this.isInStreamingModeWithLock()) {
                    this.batchCount++;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void flush() {
    	try {
    		try {
    			this.isBusyFlushing = true;
    			this.sqlDialect.flushVertexCache(this.sqlgGraph, this.vertexCache);
    			this.sqlDialect.flushEdgeCache(this.sqlgGraph, this.edgeCache);
    			this.sqlDialect.flushVertexPropertyCache(this.sqlgGraph, this.vertexPropertyCache);
    			this.sqlDialect.flushEdgePropertyCache(this.sqlgGraph, this.edgePropertyCache);
    			this.sqlDialect.flushRemovedEdges(this.sqlgGraph, this.removeEdgeCache);
    			this.sqlDialect.flushRemovedVertices(this.sqlgGraph, this.removeVertexCache);
    		} finally {
    			this.close();
    			this.isBusyFlushing = false;
    		}
    		this.sqlDialect.flushVertexGlobalUniqueIndexes(this.sqlgGraph, this.vertexCache);
    		this.sqlDialect.flushEdgeGlobalUniqueIndexes(this.sqlgGraph, this.edgeCache);
    		this.sqlDialect.flushVertexGlobalUniqueIndexPropertyCache(this.sqlgGraph, this.vertexPropertyCache);
    		this.sqlDialect.flushEdgeGlobalUniqueIndexPropertyCache(this.sqlgGraph, this.edgePropertyCache);
    		this.sqlDialect.flushRemovedGlobalUniqueIndexVertices(this.sqlgGraph, this.removeVertexCache);
    	} finally {
    		this.clear();
    	}
    }

    public void close() {
        this.streamingVertexOutputStreamCache.values().forEach(o -> {
            try {
                o.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        this.streamingVertexOutputStreamCache.clear();
        this.streamingEdgeOutputStreamCache.values().forEach(o -> {
            try {
                o.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        if (this.isInStreamingModeWithLock()) {
            this.batchCount = 0;
        }
        this.streamingEdgeOutputStreamCache.clear();
        this.streamingBatchModeVertexSchemaTable = null;
        if (this.streamingBatchModeVertexKeys != null)
            this.streamingBatchModeVertexKeys.clear();

        this.streamingBatchModeEdgeSchemaTable = null;
        if (this.streamingBatchModeEdgeKeys != null)
            this.streamingBatchModeEdgeKeys.clear();

    }

    boolean updateProperty(SqlgElement sqlgElement, String key, Object value) {
        SchemaTable schemaTable = SchemaTable.of(sqlgElement.getSchema(), sqlgElement.getTable());
        if (Vertex.class.isAssignableFrom(sqlgElement.getClass())) {
            Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> triples = this.vertexCache.get(schemaTable);
            if (triples != null) {
                Map<String, Object> triple = triples.getRight().get(sqlgElement);
                if (triple != null) {
                    triple.put(key, value);
                    triples.getLeft().add(key);
                    return true;
                }
            } else {
                Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> schemaVertices = this.vertexPropertyCache.get(schemaTable);
                //noinspection Java8ReplaceMapGet
                if (schemaVertices == null) {
                    schemaVertices = Pair.of(new TreeSet<>(), new LinkedHashMap<>());
                    this.vertexPropertyCache.put(schemaTable, schemaVertices);
                }
                SortedSet<String> keys = schemaVertices.getLeft();
                keys.add(key);
                Map<String, Object> properties = schemaVertices.getRight().get(sqlgElement);
                if (properties == null) {
                    properties = new LinkedHashMap<>();
                    schemaVertices.getRight().put((SqlgVertex) sqlgElement, properties);
                }
                properties.put(key, value);
                return true;
            }
        } else {
            SqlgEdge sqlgEdge = (SqlgEdge)sqlgElement;
            MetaEdge metaEdge = MetaEdge.from(schemaTable, sqlgEdge.getOutVertex(), sqlgEdge.getInVertex());
            Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples = this.edgeCache.get(metaEdge);
            if (triples != null) {
                Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple = triples.getRight().get(sqlgElement);
                if (triple != null) {
                    triple.getRight().put(key, value);
                    triples.getLeft().add(key);
                    return true;
                }
            } else {
                Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>> schemaEdges = this.edgePropertyCache.get(schemaTable);
                if (schemaEdges == null) {
                    schemaEdges = Pair.of(new TreeSet<>(), new LinkedHashMap<>());
                    this.edgePropertyCache.put(schemaTable, schemaEdges);
                }
                SortedSet<String> keys = schemaEdges.getLeft();
                keys.add(key);
                Map<String, Object> properties = schemaEdges.getRight().get(sqlgElement);
                if (properties == null) {
                    properties = new LinkedHashMap<>();
                    schemaEdges.getRight().put((SqlgEdge) sqlgElement, properties);
                }
                properties.put(key, value);
                return true;
            }
        }
        return false;
    }

    boolean removeProperty(SqlgProperty sqlgProperty, String key) {
        SqlgElement sqlgElement = (SqlgElement) sqlgProperty.element();
        SchemaTable schemaTable = SchemaTable.of(sqlgElement.getSchema(), sqlgElement.getTable());
        if (Vertex.class.isAssignableFrom(sqlgElement.getClass())) {
            Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> triples = this.vertexCache.get(schemaTable);
            if (triples != null) {
                Map<String, Object> triple = triples.getRight().get(sqlgElement);
                if (triple != null) {
                    triple.remove(key);
                    return true;
                }
            }
        } else {
            SqlgEdge sqlgEdge = (SqlgEdge)sqlgElement;
            MetaEdge metaEdge = MetaEdge.from(schemaTable, sqlgEdge.getOutVertex(), sqlgEdge.getInVertex());
            Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples = this.edgeCache.get(metaEdge);
            if (triples != null) {
                Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple = triples.getRight().get(sqlgElement);
                if (triple != null) {
                    triple.getRight().remove(key);
                    return true;
                }
            }
        }
        return false;
    }

    boolean vertexIsCached(SqlgVertex vertex) {
        Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> map = this.vertexCache.get(SchemaTable.of(vertex.getSchema(), vertex.getTable()));
        return map != null && map.getRight().containsKey(vertex);
    }

    public void clear() {
        this.vertexCache.clear();
        this.edgeCache.clear();
        this.removeEdgeCache.clear();
        this.removeVertexCache.clear();
        this.edgePropertyCache.clear();
        this.vertexPropertyCache.clear();
    }

    void removeVertex(String schema, String table, SqlgVertex vertex) {
        SchemaTable schemaTable = SchemaTable.of(schema, table);
        //check if the vertex is in the newly inserted cache
        Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertexSortedSetMapPair = this.vertexCache.get(schemaTable);
        if (vertexSortedSetMapPair != null && vertexSortedSetMapPair.getRight().containsKey(vertex)) {
            vertexSortedSetMapPair.getRight().remove(vertex);
            //all the edges of a new vertex must also be new
            Set<MetaEdge> toRemoveMetaEdges = new HashSet<>();
            for (Map.Entry<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> metaEdgePairEntry : this.edgeCache.entrySet()) {
                MetaEdge metaEdge = metaEdgePairEntry.getKey();
                Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edges = metaEdgePairEntry.getValue();
                Set<SqlgEdge> toRemove = new HashSet<>();
                for (Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> sqlgEdgeTripleEntry : edges.getRight().entrySet()) {
                    SqlgEdge sqlgEdge = sqlgEdgeTripleEntry.getKey();
                    Triple<SqlgVertex, SqlgVertex, Map<String, Object>> inOutVertices = sqlgEdgeTripleEntry.getValue();
                    if (inOutVertices.getLeft().equals(vertex) || inOutVertices.getMiddle().equals(vertex)) {
                        toRemove.add(sqlgEdge);
                    }
                }
                for (SqlgEdge sqlgEdge : toRemove) {
                    edges.getRight().remove(sqlgEdge);
                }
                if (edges.getRight().isEmpty()) {
                    toRemoveMetaEdges.add(metaEdge);
                }
            }
            for (MetaEdge toRemoveMetaEdge : toRemoveMetaEdges) {
                this.edgeCache.remove(toRemoveMetaEdge);
            }
        } else {
            List<SqlgVertex> vertices = this.removeVertexCache.get(schemaTable);
            if (vertices == null) {
                vertices = new ArrayList<>();
                this.removeVertexCache.put(schemaTable, vertices);
            }
            vertices.add(vertex);
        }
    }

    void removeEdge(String schema, String table, SqlgEdge edge) {
        SchemaTable schemaTable = SchemaTable.of(schema, table);
        //check it the edge is in the newly inserted cache

        MetaEdge metaEdge = MetaEdge.from(schemaTable, edge.getOutVertex(), edge.getInVertex());

        Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> sqlgEdgeTripleMap = this.edgeCache.get(metaEdge);
        if (sqlgEdgeTripleMap != null && sqlgEdgeTripleMap.getRight().containsKey(edge)) {
            sqlgEdgeTripleMap.getRight().remove(edge);
        } else {
            List<SqlgEdge> edges = this.removeEdgeCache.get(schemaTable);
            if (edges == null) {
                edges = new ArrayList<>();
                this.removeEdgeCache.put(schemaTable, edges);
            }
            edges.add(edge);
        }
    }

    SchemaTable getStreamingBatchModeVertexSchemaTable() {
        return streamingBatchModeVertexSchemaTable;
    }

    List<String> getStreamingBatchModeVertexKeys() {
        return streamingBatchModeVertexKeys;
    }

    SchemaTable getStreamingBatchModeEdgeSchemaTable() {
        return streamingBatchModeEdgeSchemaTable;
    }

    List<String> getStreamingBatchModeEdgeKeys() {
        return streamingBatchModeEdgeKeys;
    }

    public boolean isStreaming() {
        return isStreamingVertices() || isStreamingEdges();
    }

    private boolean isStreamingVertices() {
        return !this.streamingVertexOutputStreamCache.isEmpty();
    }

    private boolean isStreamingEdges() {
        return !this.streamingEdgeOutputStreamCache.isEmpty();
    }

    boolean isBusyFlushing() {
        return isBusyFlushing;
    }

}