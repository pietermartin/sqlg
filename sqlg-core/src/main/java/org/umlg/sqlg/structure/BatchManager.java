package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.util.*;

/**
 * Date: 2014/09/12
 * Time: 5:08 PM
 */
public class BatchManager {

    //This is postgres's default copy command null value
    private SqlgGraph sqlgGraph;
    private SqlDialect sqlDialect;
    private boolean batchModeOn = false;

    //map per label/keys, contains a map of vertices with a triple representing outLabels, inLabels and vertex properties
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>>> vertexCache = new LinkedHashMap<>();
    //map per label, contains a map edges. The triple is outVertex, inVertex, edge properties
    private Map<SchemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache = new LinkedHashMap<>();

    private Map<SqlgVertex, Map<SchemaTable, List<SqlgEdge>>> vertexInEdgeCache = new HashMap<>();
    private Map<SqlgVertex, Map<SchemaTable, List<SqlgEdge>>> vertexOutEdgeCache = new HashMap<>();

    //this cache is used to cache out and in labels of vertices that are not itself in the cache.
    //i.e. when adding edges between vertices in batch mode
    private Map<SqlgVertex, Pair<String, String>> vertexOutInLabelCache = new LinkedHashMap<>();

    //this is a cache of changes to properties that are already persisted, i.e. not in the vertexCache
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache = new LinkedHashMap<>();
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache = new LinkedHashMap<>();

    //map per label's vertices to delete
    private Map<SchemaTable, List<SqlgVertex>> removeVertexCache = new LinkedHashMap<>();
    //map per label's edges to delete
    private Map<SchemaTable, List<SqlgEdge>> removeEdgeCache = new LinkedHashMap<>();

    BatchManager(SqlgGraph sqlgGraph, SqlDialect sqlDialect) {
        this.sqlgGraph = sqlgGraph;
        this.sqlDialect = sqlDialect;
    }

    public boolean isBatchModeOn() {
        return batchModeOn;
    }

    public void batchModeOn() {
        this.batchModeOn = true;
    }

    public synchronized void addVertex(SqlgVertex vertex, Map<String, Object> keyValueMap) {
        SchemaTable schemaTable = SchemaTable.of(vertex.getSchema(), vertex.getTable());
        Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> pairs = this.vertexCache.get(schemaTable);
        if (pairs == null) {
            pairs = Pair.of(new TreeSet<>(keyValueMap.keySet()), new LinkedHashMap<>());
            pairs.getRight().put(vertex, Triple.of(this.sqlDialect.getBatchNull(), this.sqlDialect.getBatchNull(), keyValueMap));
            this.vertexCache.put(schemaTable, pairs);
        } else {
            pairs.getLeft().addAll(keyValueMap.keySet());
            pairs.getRight().put(vertex, Triple.of(this.sqlDialect.getBatchNull(), this.sqlDialect.getBatchNull(), keyValueMap));
        }
    }

    public void addEdge(SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        SchemaTable outSchemaTable = SchemaTable.of(outVertex.getSchema(), sqlgEdge.getTable());
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
    }

    public synchronized Map<SchemaTable, Pair<Long, Long>> flush() {
        Map<SchemaTable, Pair<Long, Long>> verticesRange = this.sqlDialect.flushVertexCache(this.sqlgGraph, this.vertexCache);
        this.sqlDialect.flushEdgeCache(this.sqlgGraph, this.edgeCache);
        this.sqlDialect.flushVertexLabelCache(this.sqlgGraph, this.vertexOutInLabelCache);
        this.sqlDialect.flushVertexPropertyCache(this.sqlgGraph, this.vertexPropertyCache);
        this.sqlDialect.flushEdgePropertyCache(this.sqlgGraph, this.edgePropertyCache);
        this.sqlDialect.flushRemovedEdges(this.sqlgGraph, this.removeEdgeCache);
        this.sqlDialect.flushRemovedVertices(this.sqlgGraph, this.removeVertexCache);
        return verticesRange;
    }

    public boolean updateVertexCacheWithEdgeLabel(SqlgVertex vertex, SchemaTable schemaTable, boolean inDirection) {
        Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> vertices = this.vertexCache.get(vertex.getSchemaTable());
        if (vertices != null) {
            Triple<String, String, Map<String, Object>> triple = vertices.getRight().get(vertex);
            if (triple != null) {
                if (inDirection) {
                    //vertex = inVertex
                    //keep outLabels untouched, update inLabels
                    vertices.getRight().put(vertex, Triple.of(triple.getLeft(), updateVertexLabels(triple.getMiddle(), schemaTable), triple.getRight()));
                    vertex.inLabelsForVertex.add(schemaTable);
                } else {
                    //vertex = outVertex
                    //keep inLabels untouched, update outLabels
                    vertices.getRight().put(vertex, Triple.of(updateVertexLabels(triple.getLeft(), schemaTable), triple.getMiddle(), triple.getRight()));
                    vertex.outLabelsForVertex.add(schemaTable);
                }
                return true;
            }
        } else {
            if (inDirection) {
                Pair<String, String> outInLabel = this.vertexOutInLabelCache.get(vertex);
                if (outInLabel == null) {
                    Set<SchemaTable> currentInLabels = this.sqlgGraph.getSchemaManager().getLabelsForVertex(vertex, true);
                    Set<SchemaTable> currentOutLabels = this.sqlgGraph.getSchemaManager().getLabelsForVertex(vertex, false);
                    String inLabels = schemaTablesToString(currentInLabels);
                    String outLabels = schemaTablesToString(currentOutLabels);
                    this.vertexOutInLabelCache.put(vertex, Pair.of(outLabels, updateVertexLabels(inLabels, schemaTable)));
                } else {
                    this.vertexOutInLabelCache.put(vertex, Pair.of(outInLabel.getLeft(), updateVertexLabels(outInLabel.getRight(), schemaTable)));
                }
                vertex.inLabelsForVertex.add(schemaTable);
            } else {
                Pair<String, String> outInLabel = this.vertexOutInLabelCache.get(vertex);
                if (outInLabel == null) {
                    Set<SchemaTable> currentInLabels = this.sqlgGraph.getSchemaManager().getLabelsForVertex(vertex, true);
                    Set<SchemaTable> currentOutLabels = this.sqlgGraph.getSchemaManager().getLabelsForVertex(vertex, false);
                    String inLabels = schemaTablesToString(currentInLabels);
                    String outLabels = schemaTablesToString(currentOutLabels);
                    this.vertexOutInLabelCache.put(vertex, Pair.of(updateVertexLabels(outLabels, schemaTable), inLabels));
                } else {
                    this.vertexOutInLabelCache.put(vertex, Pair.of(updateVertexLabels(outInLabel.getLeft(), schemaTable), outInLabel.getRight()));
                }
                vertex.outLabelsForVertex.add(schemaTable);
            }
        }
        return false;
    }

    private String schemaTablesToString(Set<SchemaTable> schemaTables) {
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (SchemaTable schemaTable : schemaTables) {
            sb.append(schemaTable.toString());
            if (count++ < schemaTables.size()) {
                sb.append(SchemaManager.LABEL_SEPERATOR);
            }
        }
        String result = sb.toString();
        if (StringUtils.isEmpty(result)) {
            return null;
        } else {
            return result;
        }
    }

    private String updateVertexLabels(String currentLabel, SchemaTable schemaTable) {
        if (StringUtils.isEmpty(currentLabel) || currentLabel.equals(this.sqlDialect.getBatchNull())) {
            return schemaTable.toString();
        } else if (currentLabel.equals(schemaTable.toString()) ||
                currentLabel.startsWith(schemaTable.toString() + SchemaManager.LABEL_SEPERATOR) ||
                currentLabel.contains(SchemaManager.LABEL_SEPERATOR + schemaTable.toString() + SchemaManager.LABEL_SEPERATOR) ||
                currentLabel.endsWith(schemaTable.toString() + SchemaManager.LABEL_SEPERATOR)) {
            return currentLabel;
        } else {
            return currentLabel + SchemaManager.LABEL_SEPERATOR + schemaTable.toString();
        }
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
        SqlgElement sqlgElement = (SqlgElement) sqlgProperty.getElement();
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

    public List<SqlgVertex> getVertices(SqlgVertex vertex, Direction direction, String[] labels) {
        List<SqlgVertex> vertices = new ArrayList<>();
        List<SqlgEdge> edges = getEdges(vertex, direction, labels);
        for (SqlgEdge sqlgEdge : edges) {
            switch (direction) {
                case IN:
                    vertices.add(sqlgEdge.getOutVertex());
                    break;
                case OUT:
                    vertices.add(sqlgEdge.getInVertex());
                    break;
                default:
                    vertices.add(sqlgEdge.getInVertex());
                    vertices.add(sqlgEdge.getOutVertex());
            }
        }
        return vertices;
    }

    public List<SqlgEdge> getEdges(SqlgVertex sqlgVertex, Direction direction, String... labels) {
        List<SqlgEdge> result = new ArrayList<>();
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

    private void internalGetEdgesFromMap(Map<SqlgVertex, Map<SchemaTable, List<SqlgEdge>>> vertexEdgeCache, SqlgVertex sqlgVertex, List<SqlgEdge> result, String[] labels) {
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
        if (map != null) {
            return map.getRight().containsKey(vertex);
        } else {
            return false;
        }
    }

    public void clear() {
        this.vertexCache.clear();
        this.edgeCache.clear();
        this.vertexInEdgeCache.clear();
        this.vertexOutEdgeCache.clear();
        this.vertexOutInLabelCache.clear();
        this.removeEdgeCache.clear();
        this.removeVertexCache.clear();
        this.edgePropertyCache.clear();
        this.vertexPropertyCache.clear();
    }

    public void removeVertex(String schema, String table, SqlgVertex vertex) {
        SchemaTable schemaTable = SchemaTable.of(schema, table);
        //check it the vertex is in the newly inserted cache
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