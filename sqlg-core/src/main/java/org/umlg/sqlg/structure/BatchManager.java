package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.codehaus.groovy.util.StringUtil;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.util.*;

/**
 * Date: 2014/09/12
 * Time: 5:08 PM
 */
public class BatchManager {

    //This is postgres's default copy command null value
    private SqlG sqlG;
    private SqlDialect sqlDialect;
    private boolean batchModeOn = false;

    //map per label/keys, contains a map of vertices with a triple representing outLabels, inLabels and vertex properties
    private Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>>> vertexCache = new LinkedHashMap<>();
    //map per label, contains a map edges. The triple is outVertex, inVertex, edge properties
    private Map<SchemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache = new LinkedHashMap<>();

    private Map<SqlgVertex, Map<String, List<SqlgEdge>>> vertexInEdgeCache = new HashMap<>();
    private Map<SqlgVertex, Map<String, List<SqlgEdge>>> vertexOutEdgeCache = new HashMap<>();

    //this cache is used to cache out and in labels of vertices that are not itself in the cache.
    //i.e. when adding edges between vertices in batch mode
    private Map<SqlgVertex, Pair<String, String>> vertexOutInLabelCache = new LinkedHashMap<>();

    //this is a cache of changes to properties that are already persisted, i.e. not in the vertexCache
    private Map<SqlgVertex, Map<String, Object>> vertexPropertyCache = new LinkedHashMap<>();
    private Map<SqlgEdge, Map<String, Object>> edgePropertyCache = new LinkedHashMap<>();

    BatchManager(SqlG sqlG, SqlDialect sqlDialect) {
        this.sqlG = sqlG;
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
            pairs = Pair.of(new TreeSet<>(keyValueMap.keySet()) ,new LinkedHashMap<>());
            pairs.getRight().put(vertex, Triple.of(this.sqlDialect.getBatchNull(), this.sqlDialect.getBatchNull(), keyValueMap));
            this.vertexCache.put(schemaTable, pairs);
        } else {
            pairs.getLeft().addAll(keyValueMap.keySet());
            pairs.getRight().put(vertex, Triple.of(this.sqlDialect.getBatchNull(), this.sqlDialect.getBatchNull(), keyValueMap));
        }
    }

    public synchronized void addEdge(SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        SchemaTable outSchemaTable = SchemaTable.of(outVertex.getSchema(), sqlgEdge.getTable());
        Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> triples = this.edgeCache.get(outSchemaTable);
        if (triples == null) {
            triples = new LinkedHashMap<>();
            triples.put(sqlgEdge, Triple.of(outVertex, inVertex, keyValueMap));
            this.edgeCache.put(outSchemaTable, triples);
        } else {
            triples.put(sqlgEdge, Triple.of(outVertex, inVertex, keyValueMap));
        }
        Map<String, List<SqlgEdge>> outEdgesMap = this.vertexOutEdgeCache.get(outVertex);
        if (outEdgesMap == null) {
            outEdgesMap = new HashMap<>();
            List<SqlgEdge> edges = new ArrayList<>();
            edges.add(sqlgEdge);
            outEdgesMap.put(sqlgEdge.label(), edges);
            this.vertexOutEdgeCache.put(outVertex, outEdgesMap);
        } else {
            List<SqlgEdge> sqlgEdges = outEdgesMap.get(sqlgEdge.label());
            if (sqlgEdges == null) {
                List<SqlgEdge> edges = new ArrayList<>();
                edges.add(sqlgEdge);
                outEdgesMap.put(sqlgEdge.label(), edges);
            } else {
                sqlgEdges.add(sqlgEdge);
            }
        }
        Map<String, List<SqlgEdge>> inEdgesMap = this.vertexInEdgeCache.get(outVertex);
        if (inEdgesMap == null) {
            inEdgesMap = new HashMap<>();
            List<SqlgEdge> edges = new ArrayList<>();
            edges.add(sqlgEdge);
            inEdgesMap.put(sqlgEdge.label(), edges);
            this.vertexInEdgeCache.put(inVertex, inEdgesMap);
        } else {
            List<SqlgEdge> sqlgEdges = inEdgesMap.get(sqlgEdge.label());
            if (sqlgEdges == null) {
                List<SqlgEdge> edges = new ArrayList<>();
                edges.add(sqlgEdge);
                inEdgesMap.put(sqlgEdge.label(), edges);
            } else {
                sqlgEdges.add(sqlgEdge);
            }
        }
    }

    public synchronized Map<SchemaTable, Pair<Long, Long>> flush() {
        Map<SchemaTable, Pair<Long, Long>> verticesRange = this.sqlDialect.flushVertexCache(this.sqlG, this.vertexCache);
        this.sqlDialect.flushEdgeCache(this.sqlG, this.edgeCache);
        this.sqlDialect.flushVertexLabelCache(this.sqlG, this.vertexOutInLabelCache);
        this.sqlDialect.flushVertexPropertyCache(this.sqlG, this.vertexPropertyCache);
        this.sqlDialect.flushEdgePropertyCache(this.sqlG, this.edgePropertyCache);
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
                } else {
                    //vertex = outVertex
                    //keep inLabels untouched, update outLabels
                    vertices.getRight().put(vertex, Triple.of(updateVertexLabels(triple.getLeft(), schemaTable), triple.getMiddle(), triple.getRight()));
                }
                return true;
            }
        } else {
            if (inDirection) {
                Pair<String, String> outInLabel = this.vertexOutInLabelCache.get(vertex);
                if (outInLabel == null) {
                    Set<SchemaTable> currentInLabels = this.sqlG.getSchemaManager().getLabelsForVertex(vertex, true);
                    Set<SchemaTable> currentOutLabels = this.sqlG.getSchemaManager().getLabelsForVertex(vertex, false);
                    String inLabels = schemaTablesToString(currentInLabels);
                    String outLabels = schemaTablesToString(currentOutLabels);
                    this.vertexOutInLabelCache.put(vertex, Pair.of(outLabels, updateVertexLabels(inLabels, schemaTable)));
                } else {
                    this.vertexOutInLabelCache.put(vertex, Pair.of(outInLabel.getLeft(), updateVertexLabels(outInLabel.getRight(), schemaTable)));
                }
            } else {
                Pair<String, String> outInLabel = this.vertexOutInLabelCache.get(vertex);
                if (outInLabel == null) {
                    Set<SchemaTable> currentInLabels = this.sqlG.getSchemaManager().getLabelsForVertex(vertex, true);
                    Set<SchemaTable> currentOutLabels = this.sqlG.getSchemaManager().getLabelsForVertex(vertex, false);
                    String inLabels = schemaTablesToString(currentInLabels);
                    String outLabels = schemaTablesToString(currentOutLabels);
                    this.vertexOutInLabelCache.put(vertex, Pair.of(updateVertexLabels(outLabels, schemaTable), inLabels));
                } else {
                    this.vertexOutInLabelCache.put(vertex, Pair.of(updateVertexLabels(outInLabel.getLeft(), schemaTable), outInLabel.getRight()));
                }
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
        } else if (currentLabel.equals(schemaTable.toString())) {
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
                Map<String, Object> properties = this.vertexPropertyCache.get(sqlgElement);
                if (properties==null) {
                    this.vertexPropertyCache.put((SqlgVertex)sqlgElement, null);
                } else {
                    properties.put(key, value);
                }
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
                Map<String, Object> properties = this.edgePropertyCache.get(sqlgElement);
                if (properties==null) {
                    this.edgePropertyCache.put((SqlgEdge)sqlgElement, null);
                } else {
                    properties.put(key, value);
                }
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
        for (SqlgEdge sqlgEdge: edges) {
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

    private void internalGetEdgesFromMap(Map<SqlgVertex, Map<String, List<SqlgEdge>>> vertexEdgeCache, SqlgVertex sqlgVertex, List<SqlgEdge> result, String[] labels) {
        for (String label : labels) {
            Map<String, List<SqlgEdge>> labeledEdgeMap = vertexEdgeCache.get(sqlgVertex);
            if (labeledEdgeMap != null && labeledEdgeMap.containsKey(label)) {
                result.addAll(labeledEdgeMap.get(label));
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
    }

}