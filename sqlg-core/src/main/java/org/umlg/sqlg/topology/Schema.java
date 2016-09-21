package org.umlg.sqlg.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.umlg.sqlg.structure.SchemaManager.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Schema {

    private static Logger logger = LoggerFactory.getLogger(Schema.class.getName());
    private Topology topology;
    private String name;
    private Map<String, VertexLabel> vertexLabels = new HashMap<>();
    private Map<String, VertexLabel> uncommittedVertexLabels = new HashMap<>();
    private Map<String, EdgeLabel> edgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedEdgeLabels = new HashMap<>();

    public static Schema createSchema(SqlgGraph sqlgGraph, Topology topology, String name) {
        Schema schema = new Schema(topology, name);
        if (!name.equals(SQLG_SCHEMA) && !sqlgGraph.getSqlDialect().getPublicSchema().equals(name)) {
            schema.createSchemaOnDb(sqlgGraph);
            TopologyManager.addSchema(sqlgGraph, name);
        }
        return schema;
    }

    private Schema(Topology topology, String name) {
        this.topology = topology;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Topology getTopology() {
        return topology;
    }

    public boolean existVertexLabel(String vertexLabelName) {
        return getVertexLabel(vertexLabelName).isPresent();
    }

    public Optional<VertexLabel> getVertexLabel(String vertexLabelName) {
        Preconditions.checkArgument(!vertexLabelName.startsWith(SchemaManager.VERTEX_PREFIX), "vertex label may not start with \"%s\"", SchemaManager.VERTEX_PREFIX);
        VertexLabel vertexLabel = this.vertexLabels.get(vertexLabelName);
        if (vertexLabel != null) {
            return Optional.of(vertexLabel);
        } else {
            vertexLabel = this.uncommittedVertexLabels.get(vertexLabelName);
            if (vertexLabel != null) {
                return Optional.of(vertexLabel);
            } else {
                return Optional.empty();
            }
        }
    }

    public VertexLabel createVertexLabel(SqlgGraph sqlgGraph, String vertexLabelName, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!vertexLabelName.startsWith(SchemaManager.VERTEX_PREFIX), "vertex label may not start with " + SchemaManager.VERTEX_PREFIX);
        VertexLabel vertexLabel = VertexLabel.createVertexLabel(sqlgGraph, this, vertexLabelName, columns);
        if (!this.name.equals(SQLG_SCHEMA)) {
            this.uncommittedVertexLabels.put(vertexLabelName, vertexLabel);
        } else {
            this.vertexLabels.put(vertexLabelName, vertexLabel);
        }
        return vertexLabel;
    }

    public boolean existEdgeLabel(String edgeLabelName) {
        return getEdgeLabel(edgeLabelName).isPresent();
    }

    public Optional<EdgeLabel> getEdgeLabel(String edgeLabelName) {
        Preconditions.checkArgument(!edgeLabelName.startsWith(SchemaManager.EDGE_PREFIX), "edge label may not start with " + SchemaManager.EDGE_PREFIX);
        EdgeLabel edgeLabel = this.edgeLabels.get(edgeLabelName);
        if (edgeLabel != null) {
            return Optional.of(edgeLabel);
        } else {
            edgeLabel = this.uncommittedEdgeLabels.get(edgeLabelName);
            if (edgeLabel != null) {
                return Optional.of(edgeLabel);
            } else {
                return Optional.empty();
            }
        }
    }

    public void addEdgeLabel(EdgeLabel edgeLabel) {
        this.uncommittedEdgeLabels.put(edgeLabel.getLabel(), edgeLabel);
    }

    /**
     * Creates a new schema on the database. i.e. 'CREATE SCHEMA...' sql statement.
     *
     * @param sqlgGraph The graph.
     */
    private void createSchemaOnDb(SqlgGraph sqlgGraph) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE SCHEMA ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void ensureVertexColumnsExist(SqlgGraph sqlgGraph, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        Optional<VertexLabel> vertexLabel = getVertexLabel(label);
        Preconditions.checkState(vertexLabel.isPresent(), String.format("BUG: vertexLabel \"%s\" must exist", label));
        //noinspection OptionalGetWithoutIsPresent
        vertexLabel.get().ensureColumnsExist(sqlgGraph, columns);
    }

    public void ensureEdgeColumnsExist(SqlgGraph sqlgGraph, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        Optional<EdgeLabel> edgeLabel = getEdgeLabel(label);
        Preconditions.checkState(edgeLabel.isPresent(), "BUG: edgeLabel \"%s\" must exist", label);
        //noinspection OptionalGetWithoutIsPresent
        edgeLabel.get().ensureColumnsExist(sqlgGraph, columns);
    }

    public Map<String, Map<String, PropertyType>> getAllTablesWithout(List<String> filter) {

        Map<String, Map<String, PropertyType>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {

            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
            String vertexLabelQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();

            if (!filter.contains(vertexLabelQualifiedName)) {

                result.put(vertexLabelQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());

            }

        }

        for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.edgeLabels.entrySet()) {

            Preconditions.checkState(!edgeLabelEntry.getValue().getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
            String edgeLabelQualifiedName = edgeLabelEntry.getValue().getSchema().getName() + "." + EDGE_PREFIX + edgeLabelEntry.getValue().getLabel();

            if (!filter.contains(edgeLabelQualifiedName)) {

                result.put(edgeLabelQualifiedName, edgeLabelEntry.getValue().getPropertyTypeMap());

            }

        }

        if (this.topology.isHeldByCurrentThread()) {

            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {

                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
                String vertexLabelQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();

                if (!filter.contains(vertexLabelQualifiedName)) {

                    result.put(vertexLabelQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());

                }

            }

            for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.uncommittedEdgeLabels.entrySet()) {

                Preconditions.checkState(!edgeLabelEntry.getValue().getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
                String edgeLabelQualifiedName = edgeLabelEntry.getValue().getSchema().getName() + "." + EDGE_PREFIX + edgeLabelEntry.getValue().getLabel();

                if (!filter.contains(edgeLabelQualifiedName)) {

                    result.put(edgeLabelQualifiedName, edgeLabelEntry.getValue().getPropertyTypeMap());

                }

            }

        }

        return result;

    }

    public Map<String, Map<String, PropertyType>> getAllTablesFrom(List<String> selectFrom) {

        Map<String, Map<String, PropertyType>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {

            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();

            if (selectFrom.contains(vertexQualifiedName)) {

                result.put(vertexQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());

            }

        }

        for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.edgeLabels.entrySet()) {

            Preconditions.checkState(!edgeLabelEntry.getValue().getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
            String edgeQualifiedName = this.name + "." + EDGE_PREFIX + edgeLabelEntry.getValue().getLabel();

            if (selectFrom.contains(edgeQualifiedName)) {

                result.put(edgeQualifiedName, edgeLabelEntry.getValue().getPropertyTypeMap());

            }

        }

        if (this.topology.isHeldByCurrentThread()) {

            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {

                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
                String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();

                if (selectFrom.contains(vertexQualifiedName)) {

                    result.put(vertexQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());

                }

            }

            for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.uncommittedEdgeLabels.entrySet()) {

                Preconditions.checkState(!edgeLabelEntry.getValue().getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
                String edgeQualifiedName = this.name + "." + EDGE_PREFIX + edgeLabelEntry.getValue().getLabel();

                if (selectFrom.contains(edgeQualifiedName)) {

                    result.put(edgeQualifiedName, edgeLabelEntry.getValue().getPropertyTypeMap());

                }

            }

        }

        return result;

    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {

        Map<String, PropertyType> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {

            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);

            String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();

            if (schemaTable.getTable().equals(prefixedVertexName)) {

                result.putAll(vertexLabelEntry.getValue().getPropertyTypeMap());
                break;

            }

        }

        for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.edgeLabels.entrySet()) {

            Preconditions.checkState(!edgeLabelEntry.getValue().getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);

            String prefixedEdgeName = EDGE_PREFIX + edgeLabelEntry.getValue().getLabel();

            if (schemaTable.getTable().equals(prefixedEdgeName)) {

                result.putAll(edgeLabelEntry.getValue().getPropertyTypeMap());
                break;

            }

        }

        if (this.topology.isHeldByCurrentThread()) {

            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {

                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);

                String prefixedVertexName = SchemaManager.VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();

                if (schemaTable.getTable().equals(prefixedVertexName)) {

                    result.putAll(vertexLabelEntry.getValue().getPropertyTypeMap());
                    break;

                }

            }

            for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.uncommittedEdgeLabels.entrySet()) {

                Preconditions.checkState(!edgeLabelEntry.getValue().getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);

                String prefixedEdgeName = SchemaManager.EDGE_PREFIX + edgeLabelEntry.getValue().getLabel();

                if (schemaTable.getTable().equals(prefixedEdgeName)) {

                    result.putAll(edgeLabelEntry.getValue().getPropertyTypeMap());
                    break;

                }

            }

        }

        return result;
    }

    public Optional<Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels(SchemaTable schemaTable) {
//        Pair<Set<SchemaTable>, Set<SchemaTable>> result = this.tableLabels.get(schemaTable);
//        if (result == null) {
//            if (!this.uncommittedTableLabels.isEmpty() && this.isLockedByCurrentThread()) {
//                Pair<Set<SchemaTable>, Set<SchemaTable>> pair = this.uncommittedTableLabels.get(schemaTable);
//                if (pair != null) {
//                    return Pair.of(Collections.unmodifiableSet(pair.getLeft()), Collections.unmodifiableSet(pair.getRight()));
//                }
//            }
//            return Pair.of(Collections.EMPTY_SET, Collections.EMPTY_SET);
//        } else {
//            Set<SchemaTable> left = new HashSet<>(result.getLeft());
//            Set<SchemaTable> right = new HashSet<>(result.getRight());
//            if (!this.uncommittedTableLabels.isEmpty() && this.isLockedByCurrentThread()) {
//                Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedLabels = this.uncommittedTableLabels.get(schemaTable);
//                if (uncommittedLabels != null) {
//                    left.addAll(uncommittedLabels.getLeft());
//                    right.addAll(uncommittedLabels.getRight());
//                }
//            }
//            return Pair.of(
//                    Collections.unmodifiableSet(left),
//                    Collections.unmodifiableSet(right));
//        }

        Pair<Set<SchemaTable>, Set<SchemaTable>> result = null;
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {

            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with " + VERTEX_PREFIX);

            String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();

            if (schemaTable.getTable().equals(prefixedVertexName)) {

                result = vertexLabelEntry.getValue().getTableLabels();
                break;

            }

        }

        Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedResult = null;
        if (this.topology.isHeldByCurrentThread()) {

            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {

                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with " + VERTEX_PREFIX);

                String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();

                if (schemaTable.getTable().equals(prefixedVertexName)) {

                    uncommittedResult = vertexLabelEntry.getValue().getTableLabels();
                    break;

                }

            }


        }
        //need to merge in the uncommitted table labels in
        if (result != null && uncommittedResult != null) {
            result.getLeft().addAll(uncommittedResult.getLeft());
            result.getRight().addAll(uncommittedResult.getRight());
            return Optional.of(result);
        } else if (result != null) {
            return Optional.of(result);
        } else if (uncommittedResult != null) {
            return Optional.of(uncommittedResult);
        } else {
            return Optional.empty();
        }
    }

    public Map<String, Set<String>> getAllEdgeForeignKeys() {
//        Map<String, Set<String>> result = new HashMap<>();
//        result.putAll(this.edgeForeignKeys);
//        if (!this.uncommittedEdgeForeignKeys.isEmpty() && this.isLockedByCurrentThread()) {
//            result.putAll(this.uncommittedEdgeForeignKeys);
//            for (Map.Entry<String, Set<String>> schemaTableEntry : this.uncommittedEdgeForeignKeys.entrySet()) {
//                Set<String> foreignKeys = result.get(schemaTableEntry.getKey());
//                if (foreignKeys == null) {
//                    foreignKeys = new HashSet<>();
//                }
//                foreignKeys.addAll(schemaTableEntry.getValue());
//                foreignKeys.addAll(schemaTableEntry.getValue());
//                result.put(schemaTableEntry.getKey(), foreignKeys);
//            }
//        }
//        return Collections.unmodifiableMap(result);

        Map<String, Set<String>> result = new HashMap<>();
        for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.edgeLabels.entrySet()) {

            Preconditions.checkState(!edgeLabelEntry.getValue().getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);

            result.put(this.getName() + "." + EDGE_PREFIX + edgeLabelEntry.getValue().getLabel(), edgeLabelEntry.getValue().getAllEdgeForeignKeys());

        }

        if (this.topology.isHeldByCurrentThread()) {

            for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.uncommittedEdgeLabels.entrySet()) {

                Preconditions.checkState(!edgeLabelEntry.getValue().getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);

                result.put(this.getName() + "." + EDGE_PREFIX + edgeLabelEntry.getValue().getLabel(), edgeLabelEntry.getValue().getAllEdgeForeignKeys());

            }

        }
        return result;
    }

    public void afterCommit() {
        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            this.vertexLabels.put(entry.getKey(), entry.getValue());
            it.remove();
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterCommit();
        }
        for (Iterator<Map.Entry<String, EdgeLabel>> it = this.uncommittedEdgeLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, EdgeLabel> entry = it.next();
            this.edgeLabels.put(entry.getKey(), entry.getValue());
            entry.getValue().afterCommit();
            it.remove();
        }
        for (EdgeLabel edgeLabel : this.edgeLabels.values()) {
            edgeLabel.afterCommit();
        }
    }

    public void afterRollback() {
        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterCommit();
        }
        for (Iterator<Map.Entry<String, EdgeLabel>> it = this.uncommittedEdgeLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, EdgeLabel> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        for (EdgeLabel edgeLabel : this.edgeLabels.values()) {
            edgeLabel.afterCommit();
        }
    }

    public boolean isSqlgSchema() {
        return this.name.equals(SQLG_SCHEMA);
    }

    @Override
    public String toString() {
        return this.name;
    }

}
