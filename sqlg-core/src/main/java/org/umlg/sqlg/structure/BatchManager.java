package org.umlg.sqlg.structure;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.*;

/**
 * Date: 2014/09/12
 * Time: 5:08 PM
 */
public class BatchManager {

    //This is postgres's default copy command null value
    private SqlgGraph sqlgGraph;
    private SqlDialect sqlDialect;

    public enum BatchModeType {
        NONE, NORMAL, COMPLETE
    }

    private BatchModeType batchModeType = BatchModeType.NONE;

    //map per label/keys, contains a map of vertices with a triple representing outLabels, inLabels and vertex properties
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>>> vertexCache = new LinkedHashMap<>();
    //map per label, contains a map edges. The triple is outVertex, inVertex, edge properties
    private Map<SchemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache = new LinkedHashMap<>();

    private Map<SqlgVertex, Map<SchemaTable, List<SqlgEdge>>> vertexInEdgeCache = new HashMap<>();
    private Map<SqlgVertex, Map<SchemaTable, List<SqlgEdge>>> vertexOutEdgeCache = new HashMap<>();

    //this is a cache of changes to properties that are already persisted, i.e. not in the vertexCache
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache = new LinkedHashMap<>();
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache = new LinkedHashMap<>();

    //map per label's vertices to delete
    private Map<SchemaTable, List<SqlgVertex>> removeVertexCache = new LinkedHashMap<>();
    //map per label's edges to delete
    private Map<SchemaTable, List<SqlgEdge>> removeEdgeCache = new LinkedHashMap<>();

    private Map<SchemaTable, PipedOutputStream> completeVertexCache = new LinkedHashMap<>();

    BatchManager(SqlgGraph sqlgGraph, SqlDialect sqlDialect) {
        this.sqlgGraph = sqlgGraph;
        this.sqlDialect = sqlDialect;
    }

    public boolean isBatchModeNormal() {
        return this.batchModeType == BatchModeType.NORMAL;
    }

    public boolean isBatchModeComplete() {
        return this.batchModeType == BatchModeType.COMPLETE;
    }

    public boolean isBatchModeOn(){
        return (this.batchModeType == BatchModeType.NORMAL) || (this.batchModeType == BatchModeType.COMPLETE);
    }

    public void batchModeOn(boolean completeBatchModeOn) {
        if (completeBatchModeOn == true) {
            this.batchModeType = BatchModeType.COMPLETE;
        } else {
            this.batchModeType = BatchModeType.NORMAL;
        }
    }

    public void batchModeOn() {
        batchModeOn(false);
    }

    public synchronized void addVertex(boolean complete, SqlgVertex vertex, Map<String, Object> keyValueMap) {
        SchemaTable schemaTable = SchemaTable.of(vertex.getSchema(), vertex.getTable());
        if (!complete) {
            Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> pairs = this.vertexCache.get(schemaTable);
            if (pairs == null) {
                pairs = Pair.of(new TreeSet<>(keyValueMap.keySet()), new LinkedHashMap<>());
                pairs.getRight().put(vertex, Triple.of(this.sqlDialect.getBatchNull(), this.sqlDialect.getBatchNull(), keyValueMap));
                this.vertexCache.put(schemaTable, pairs);
            } else {
                pairs.getLeft().addAll(keyValueMap.keySet());
                pairs.getRight().put(vertex, Triple.of(this.sqlDialect.getBatchNull(), this.sqlDialect.getBatchNull(), keyValueMap));
            }
        } else {
            try {
                PipedOutputStream out = this.completeVertexCache.get(schemaTable);
                if (out == null) {
                    //Cache outputStream per Table.
                    out = new PipedOutputStream();
                    this.completeVertexCache.put(schemaTable, out);
                    this.sqlDialect.copyIn(this.sqlgGraph, vertex, keyValueMap, new PipedInputStream(out));
                }
                this.sqlDialect.flushCompleteVertex(out, keyValueMap);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void addEdge(boolean complete, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        SchemaTable outSchemaTable = SchemaTable.of(outVertex.getSchema(), sqlgEdge.getTable());
        if (!complete) {
            Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> triples = this.edgeCache.get(outSchemaTable);
            if (triples == null) {
                triples = new LinkedHashMap<>();
                triples.put(sqlgEdge, Triple.of(outVertex, inVertex, keyValueMap));
                this.edgeCache.put(outSchemaTable, triples);
            } else {
                triples.put(sqlgEdge, Triple.of(outVertex, inVertex, keyValueMap));
            }
            Map<SchemaTable, List<SqlgEdge>> outEdgesMap = this.vertexOutEdgeCache.get(outVertex);
            if (outEdgesMap == null) {
                outEdgesMap = new HashMap<>();
                List<SqlgEdge> edges = new ArrayList<>();
                edges.add(sqlgEdge);
                outEdgesMap.put(SchemaTable.of(sqlgEdge.getSchema(), sqlgEdge.getTable()), edges);
                this.vertexOutEdgeCache.put(outVertex, outEdgesMap);
            } else {
                List<SqlgEdge> sqlgEdges = outEdgesMap.get(SchemaTable.of(sqlgEdge.getSchema(), sqlgEdge.getTable()));
                if (sqlgEdges == null) {
                    List<SqlgEdge> edges = new ArrayList<>();
                    edges.add(sqlgEdge);
                    outEdgesMap.put(SchemaTable.of(sqlgEdge.getSchema(), sqlgEdge.getTable()), edges);
                } else {
                    sqlgEdges.add(sqlgEdge);
                }
            }
            Map<SchemaTable, List<SqlgEdge>> inEdgesMap = this.vertexInEdgeCache.get(outVertex);
            if (inEdgesMap == null) {
                inEdgesMap = new HashMap<>();
                List<SqlgEdge> edges = new ArrayList<>();
                edges.add(sqlgEdge);
                inEdgesMap.put(SchemaTable.of(sqlgEdge.getSchema(), sqlgEdge.getTable()), edges);
                this.vertexInEdgeCache.put(inVertex, inEdgesMap);
            } else {
                List<SqlgEdge> sqlgEdges = inEdgesMap.get(SchemaTable.of(sqlgEdge.getSchema(), sqlgEdge.getTable()));
                if (sqlgEdges == null) {
                    List<SqlgEdge> edges = new ArrayList<>();
                    edges.add(sqlgEdge);
                    inEdgesMap.put(SchemaTable.of(sqlgEdge.getSchema(), sqlgEdge.getTable()), edges);
                } else {
                    sqlgEdges.add(sqlgEdge);
                }
            }
        } else {

        }
    }

    public synchronized Map<SchemaTable, Pair<Long, Long>> flush() {
        Map<SchemaTable, Pair<Long, Long>> verticesRange = this.sqlDialect.flushVertexCache(this.sqlgGraph, this.vertexCache);
        this.sqlDialect.flushEdgeCache(this.sqlgGraph, this.edgeCache);
        this.sqlDialect.flushVertexPropertyCache(this.sqlgGraph, this.vertexPropertyCache);
        this.sqlDialect.flushEdgePropertyCache(this.sqlgGraph, this.edgePropertyCache);
        this.sqlDialect.flushRemovedEdges(this.sqlgGraph, this.removeEdgeCache);
        this.sqlDialect.flushRemovedVertices(this.sqlgGraph, this.removeVertexCache);
        this.completeVertexCache.values().forEach(o -> {
            try {
                o.flush();
                o.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return verticesRange;
    }

    public boolean updateProperty(SqlgElement sqlgElement, String key, Object value) {
        SchemaTable schemaTable = SchemaTable.of(sqlgElement.getSchema(), sqlgElement.getTable());
        if (Vertex.class.isAssignableFrom(sqlgElement.getClass())) {
            Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> triples = this.vertexCache.get(schemaTable);
            if (triples != null) {
                Triple<String, String, Map<String, Object>> triple = triples.getRight().get(sqlgElement);
                if (triple != null) {
                    triple.getRight().put(key, value);
                    triples.getLeft().add(key);
                    return true;
                }
            } else {
                Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> schemaVertices = this.vertexPropertyCache.get(schemaTable);
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
            Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> triples = this.edgeCache.get(schemaTable);
            if (triples != null) {
                Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple = triples.get(sqlgElement);
                if (triple != null) {
                    triple.getRight().put(key, value);
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

    public boolean removeProperty(SqlgProperty sqlgProperty, String key) {
        SqlgElement sqlgElement = (SqlgElement) sqlgProperty.element();
        SchemaTable schemaTable = SchemaTable.of(sqlgElement.getSchema(), sqlgElement.getTable());
        if (Vertex.class.isAssignableFrom(sqlgElement.getClass())) {
            Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> triples = this.vertexCache.get(schemaTable);
            if (triples != null) {
                Triple<String, String, Map<String, Object>> triple = triples.getRight().get(sqlgElement);
                if (triple != null) {
                    triple.getRight().remove(key);
                    return true;
                }
            }
        } else {
            Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> triples = this.edgeCache.get(schemaTable);
            if (triples != null) {
                Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple = triples.get(sqlgElement);
                if (triple != null) {
                    triple.getRight().remove(key);
                    return true;
                }
            }
        }
        return false;
    }

    public List<Vertex> getVertices(SqlgVertex vertex, Direction direction, String[] labels) {
        List<Vertex> vertices = new ArrayList<>();
        List<Edge> edges = getEdges(vertex, direction, labels);
        for (Edge sqlgEdge : edges) {
            switch (direction) {
                case IN:
                    vertices.add(((SqlgEdge) sqlgEdge).getOutVertex());
                    break;
                case OUT:
                    vertices.add(((SqlgEdge) sqlgEdge).getInVertex());
                    break;
                default:
                    vertices.add(((SqlgEdge) sqlgEdge).getInVertex());
                    vertices.add(((SqlgEdge) sqlgEdge).getOutVertex());
            }
        }
        return vertices;
    }

    public List<Edge> getEdges(SqlgVertex sqlgVertex, Direction direction, String... labels) {
        List<Edge> result = new ArrayList<>();
        switch (direction) {
            case IN:
                internalGetEdgesFromMap(this.vertexInEdgeCache, sqlgVertex, result, labels);
                break;
            case OUT:
                internalGetEdgesFromMap(this.vertexOutEdgeCache, sqlgVertex, result, labels);
                break;
            default:
                internalGetEdgesFromMap(this.vertexInEdgeCache, sqlgVertex, result, labels);
                internalGetEdgesFromMap(this.vertexOutEdgeCache, sqlgVertex, result, labels);
        }
        return result;
    }

    private void internalGetEdgesFromMap(Map<SqlgVertex, Map<SchemaTable, List<SqlgEdge>>> vertexEdgeCache, SqlgVertex sqlgVertex, List<Edge> result, String[] labels) {
        for (String label : labels) {
            Map<SchemaTable, List<SqlgEdge>> labeledEdgeMap = vertexEdgeCache.get(sqlgVertex);
            if (labeledEdgeMap != null) {
                for (Map.Entry<SchemaTable, List<SqlgEdge>> schemaTableListEntry : labeledEdgeMap.entrySet()) {
                    SchemaTable edgeSchemaTable = schemaTableListEntry.getKey();
                    List<SqlgEdge> sqlgEdges = schemaTableListEntry.getValue();
                    if (edgeSchemaTable.getTable().equals(label)) {
                        result.addAll(sqlgEdges);
                    }
                }
            }
        }
    }

    public boolean vertexIsCached(SqlgVertex vertex) {
        Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> map = this.vertexCache.get(SchemaTable.of(vertex.getSchema(), vertex.getTable()));
        return map != null && map.getRight().containsKey(vertex);
    }

    public void clear() {
        this.vertexCache.clear();
        this.edgeCache.clear();
        this.vertexInEdgeCache.clear();
        this.vertexOutEdgeCache.clear();
        this.removeEdgeCache.clear();
        this.removeVertexCache.clear();
        this.edgePropertyCache.clear();
        this.vertexPropertyCache.clear();
    }

    public void removeVertex(String schema, String table, SqlgVertex vertex) {
        SchemaTable schemaTable = SchemaTable.of(schema, table);
        //check if the vertex is in the newly inserted cache
        Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> vertexSortedSetMapPair = this.vertexCache.get(schemaTable);
        if (vertexSortedSetMapPair != null && vertexSortedSetMapPair.getRight().containsKey(vertex)) {
            vertexSortedSetMapPair.getRight().remove(vertex);
            //all the edges of a new vertex must also be new
            Map<SchemaTable, List<SqlgEdge>> outEdges = this.vertexOutEdgeCache.get(vertex);
            if (outEdges != null) {
                for (Map.Entry<SchemaTable, List<SqlgEdge>> entry : outEdges.entrySet()) {
                    SchemaTable edgeSchemaTable = entry.getKey();
                    List<SqlgEdge> edges = entry.getValue();
                    //remove these edges from the edge cache
                    Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> cachedEdge = this.edgeCache.get(edgeSchemaTable);
                    if (cachedEdge == null) {
                        throw new IllegalStateException("BUG: new edge not found in edgeCache during bulk remove!");
                    }
                    for (SqlgEdge sqlgEdge : edges) {
                        cachedEdge.remove(sqlgEdge);
                    }
                    if (cachedEdge.isEmpty()) {
                        this.edgeCache.remove(edgeSchemaTable);
                    }
                }
                this.vertexOutEdgeCache.remove(vertex);
            }
            Map<SchemaTable, List<SqlgEdge>> inEdges = this.vertexInEdgeCache.get(vertex);
            if (inEdges != null) {
                for (Map.Entry<SchemaTable, List<SqlgEdge>> entry : inEdges.entrySet()) {
                    SchemaTable edgeSchemaTable = entry.getKey();
                    List<SqlgEdge> edges = entry.getValue();
                    //remove these edges from the edge cache
                    Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> cachedEdge = this.edgeCache.get(edgeSchemaTable);
                    if (cachedEdge == null) {
                        throw new IllegalStateException("BUG: new edge not found in edgeCache during bulk remove!");
                    }
                    for (SqlgEdge sqlgEdge : edges) {
                        cachedEdge.remove(sqlgEdge);
                    }
                    if (cachedEdge.isEmpty()) {
                        this.edgeCache.remove(edgeSchemaTable);
                    }
                }
                this.vertexInEdgeCache.remove(vertex);
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

    public void removeEdge(String schema, String table, SqlgEdge edge) {
        SchemaTable schemaTable = SchemaTable.of(schema, table);
        //check it the edge is in the newly inserted cache
        Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> sqlgEdgeTripleMap = this.edgeCache.get(schemaTable);
        if (sqlgEdgeTripleMap != null && sqlgEdgeTripleMap.containsKey(edge)) {
            sqlgEdgeTripleMap.remove(edge);
        } else {
            List<SqlgEdge> edges = this.removeEdgeCache.get(schemaTable);
            if (edges == null) {
                edges = new ArrayList<>();
                this.removeEdgeCache.put(schemaTable, edges);
            }
            edges.add(edge);
        }
    }

}