package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang3.tuple.Triple;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.util.*;

/**
 * Date: 2014/09/12
 * Time: 5:08 PM
 */
public class BatchManager {

    //This is postgres's default copy command null value
    //TODO move it to postgres dialect when implementing Batch for hsqldb and MariaDB
    private static final String BATCH_NULL = "\\N";
    private SqlG sqlG;
    private SqlDialect sqlDialect;

    //map per label, contains a map of vertices with a triple representing outLabels, inLabels and vertex properties
    private Map<SchemaTable, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>> vertexCache = new HashMap<>();
    //map per label, contains a map edges. The triple is outVertex, inVertex, edge properties
    private Map<SchemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache = new HashMap<>();

    private Map<SqlgVertex, Map<String, List<SqlgEdge>>> vertexInEdgeCache = new HashMap<>();
    private Map<SqlgVertex, Map<String, List<SqlgEdge>>> vertexOutEdgeCache = new HashMap<>();

    BatchManager(SqlG sqlG, SqlDialect sqlDialect) {
        this.sqlG = sqlG;
        this.sqlDialect = sqlDialect;
    }

    public synchronized void addVertex(SqlgVertex vertex, Map<String, Object> keyValueMap) {
        SchemaTable schemaTable = SchemaTable.of(vertex.getSchema(), vertex.getTable());
        Map<SqlgVertex, Triple<String, String, Map<String, Object>>> pairs = this.vertexCache.get(schemaTable);
        if (pairs == null) {
            pairs = new LinkedHashMap<>();
            pairs.put(vertex, Triple.of(BATCH_NULL, BATCH_NULL, keyValueMap));
            this.vertexCache.put(schemaTable, pairs);
        } else {
            pairs.put(vertex, Triple.of(BATCH_NULL, BATCH_NULL, keyValueMap));
        }
    }

    public synchronized void addEdge(SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        SchemaTable schemaTable = SchemaTable.of(sqlgEdge.getSchema(), sqlgEdge.getTable());
        Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> triples = this.edgeCache.get(schemaTable);
        if (triples == null) {
            triples = new LinkedHashMap<>();
            triples.put(sqlgEdge, Triple.of(outVertex, inVertex, keyValueMap));
            this.edgeCache.put(schemaTable, triples);
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

    public synchronized void flush() {
        this.sqlDialect.flushVertexCache(this.sqlG, this.vertexCache);
        this.sqlDialect.flushEdgeCache(this.sqlG, this.edgeCache);
    }

    public boolean updateVertexCacheWithEdgeLabel(SqlgVertex vertex, SchemaTable schemaTable, boolean inDirection) {
        Map<SqlgVertex, Triple<String, String, Map<String, Object>>> vertices = this.vertexCache.get(SchemaTable.of(vertex.getSchema(), vertex.getTable()));
        if (vertices != null) {
            Triple<String, String, Map<String, Object>> triple = vertices.get(vertex);
            if (triple != null) {
                if (inDirection) {
                    //vertex = inVertex
                    //keep outLabels untouched, update inLabels
                    vertices.put(vertex, Triple.of(triple.getLeft(), updateVertexLabels(triple.getMiddle(), schemaTable), triple.getRight()));
                } else {
                    //vertex = outVertex
                    //keep inLabels untouched, update outLabels
                    vertices.put(vertex, Triple.of(updateVertexLabels(triple.getLeft(), schemaTable), triple.getMiddle(), triple.getRight()));
                }
                return true;
            }
        }
        return false;
    }

    private String updateVertexLabels(String currentLabel, SchemaTable schemaTable) {
        if (currentLabel.equals(BATCH_NULL)) {
            return schemaTable.toString();
        } else if (currentLabel.equals(schemaTable.toString())) {
            return currentLabel;
        } else {
            return currentLabel + "," + schemaTable.toString();
        }
    }

    public boolean updateProperty(SqlgElement sqlgElement, String key, Object value) {
        SchemaTable schemaTable = SchemaTable.of(sqlgElement.getSchema(), sqlgElement.getTable());
        if (Vertex.class.isAssignableFrom(sqlgElement.getClass())) {
            Map<SqlgVertex, Triple<String, String, Map<String, Object>>> triples = this.vertexCache.get(schemaTable);
            if (triples != null) {
                Triple<String, String, Map<String, Object>> triple = triples.get(sqlgElement);
                if (triple != null) {
                    triple.getRight().put(key, value);
                    return true;
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
            }
        }
        return false;
    }

    public boolean removeProperty(SqlgProperty sqlgProperty, String key) {
        SqlgElement sqlgElement = (SqlgElement) sqlgProperty.getElement();
        SchemaTable schemaTable = SchemaTable.of(sqlgElement.getSchema(), sqlgElement.getTable());
        if (Vertex.class.isAssignableFrom(sqlgElement.getClass())) {
            Map<SqlgVertex, Triple<String, String, Map<String, Object>>> triples = this.vertexCache.get(schemaTable);
            if (triples != null) {
                Triple<String, String, Map<String, Object>> triple = triples.get(sqlgElement);
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
        Map<SqlgVertex, Triple<String, String, Map<String, Object>>> map = this.vertexCache.get(SchemaTable.of(vertex.getSchema(), vertex.getTable()));
        if (map != null) {
            return map.containsKey(vertex);
        } else {
            return false;
        }
    }

    public void clear() {
        this.vertexCache.clear();
        this.edgeCache.clear();
        this.vertexInEdgeCache.clear();
        this.vertexOutEdgeCache.clear();
    }

}