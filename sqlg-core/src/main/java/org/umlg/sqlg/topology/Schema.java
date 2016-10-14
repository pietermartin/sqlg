package org.umlg.sqlg.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.umlg.sqlg.structure.SchemaManager.*;
import static org.umlg.sqlg.topology.Topology.OBJECT_MAPPER;

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

    /**
     * Creates the SqlgSchema. The sqlg_schema always exist and is created via sql in {@link SqlDialect#sqlgTopologyCreationScripts()}
     *
     * @param topology A reference to the {@link Topology} that contains the sqlg_schema schema.
     * @return The Schema that represents 'sqlg_schema'
     */
    static Schema instantiateSqlgSchema(Topology topology) {
        return new Schema(topology, SchemaManager.SQLG_SCHEMA);
    }

    /**
     * Creates the 'public' schema that always already exist and is pre-loaded in {@link SchemaManager#addPublicSchema()} @see {@link SchemaManager#loadUserSchema()}
     *
     * @param publicSchemaName The 'public' schema's name. Sometimes its upper case (Hsqldb) sometimes lower (Postgresql)
     * @param topology         The {@link Topology} that contains the public schema.
     * @return The Schema that represents 'public'
     */
    static Schema createPublicSchema(Topology topology, String publicSchemaName) {
        return new Schema(topology, publicSchemaName);
    }

    static Schema createSchema(SqlgGraph sqlgGraph, Topology topology, String name) {
        Schema schema = new Schema(topology, name);
        Preconditions.checkArgument(!name.equals(SQLG_SCHEMA) && !sqlgGraph.getSqlDialect().getPublicSchema().equals(name), "createSchema may not be called for 'sqlg_schema' or 'public'");
        schema.createSchemaOnDb(sqlgGraph);
        TopologyManager.addSchema(sqlgGraph, name);
        return schema;
    }

    static Schema instantiateSchema(Topology topology, String schemaName) {
        return new Schema(topology, schemaName);
    }

    private Schema(Topology topology, String name) {
        this.topology = topology;
        this.name = name;
    }

    void ensureVertexTableExist(final SqlgGraph sqlgGraph, final String label, final Object... keyValues) {
        Objects.requireNonNull(label, GIVEN_TABLE_MUST_NOT_BE_NULL);
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), String.format("label may not be prefixed with %s", VERTEX_PREFIX));

        Optional<VertexLabel> vertexLabelOptional = this.getVertexLabel(label);
        if (!vertexLabelOptional.isPresent()) {
            this.topology.lock();
            vertexLabelOptional = this.getVertexLabel(label);
            if (!vertexLabelOptional.isPresent()) {
                final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
                this.createVertexLabel(sqlgGraph, label, columns);
            }
        } else {
            VertexLabel vertexLabel = vertexLabelOptional.get();
            //check if all the columns are there.
            final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
            vertexLabel.ensureColumnsExist(sqlgGraph, columns);
        }
    }

    SchemaTable ensureEdgeTableExist(final SqlgGraph sqlgGraph, final String edgeLabelName, final SchemaTable foreignKeyOut, final SchemaTable foreignKeyIn, Object... keyValues) {
        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName must not be null");
        Objects.requireNonNull(foreignKeyOut, "Given outTable must not be null");
        Objects.requireNonNull(foreignKeyIn, "Given inTable must not be null");

        EdgeLabel edgeLabel;
        Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
        if (!edgeLabelOptional.isPresent()) {
            this.topology.lock();
            edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
            if (!edgeLabelOptional.isPresent()) {
                final Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
                edgeLabel = this.createEdgeLabel(sqlgGraph, edgeLabelName, foreignKeyOut, foreignKeyIn, columns);
                //nothing more to do as the edge did not exist and will have been created with the correct foreign keys.
            } else {
                edgeLabel = internalEnsureEdgeTableExists(sqlgGraph, foreignKeyOut, foreignKeyIn, edgeLabelOptional.get(), keyValues);
            }
        } else {
            edgeLabel = internalEnsureEdgeTableExists(sqlgGraph, foreignKeyOut, foreignKeyIn, edgeLabelOptional.get(), keyValues);
        }
        return SchemaTable.of(edgeLabel.getSchema().getName(), edgeLabel.getLabel());
    }

    private EdgeLabel internalEnsureEdgeTableExists(SqlgGraph sqlgGraph, SchemaTable foreignKeyOut, SchemaTable foreignKeyIn, EdgeLabel edgeLabel, Object[] keyValues) {
        //need to check that the out foreign keys exist.
        Optional<VertexLabel> outVertexLabelOptional = this.getVertexLabel(foreignKeyOut.getTable());
        Preconditions.checkState(outVertexLabelOptional.isPresent(), "Out vertex label not found for %s.%s", foreignKeyIn.getSchema(), foreignKeyIn.getTable());

        //need to check that the in foreign keys exist.
        //The in vertex might be in a different schema so search on the topology
        Optional<VertexLabel> inVertexLabelOptional = this.topology.getVertexLabel(foreignKeyIn.getSchema(), foreignKeyIn.getTable());
        Preconditions.checkState(inVertexLabelOptional.isPresent(), "In vertex label not found for %s.%s", foreignKeyIn.getSchema(), foreignKeyIn.getTable());

        //noinspection OptionalGetWithoutIsPresent
        edgeLabel.ensureEdgeForeignKeysExist(sqlgGraph, false, outVertexLabelOptional.get(), foreignKeyOut);
        //noinspection OptionalGetWithoutIsPresent
        edgeLabel.ensureEdgeForeignKeysExist(sqlgGraph, true, inVertexLabelOptional.get(), foreignKeyIn);
        final Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        edgeLabel.ensureColumnsExist(sqlgGraph, columns);
        return edgeLabel;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private EdgeLabel createEdgeLabel(final SqlgGraph sqlgGraph, final String edgeLabelName, final SchemaTable foreignKeyOut, final SchemaTable foreignKeyIn, final Map<String, PropertyType> columns) {
        Preconditions.checkArgument(this.topology.isHeldByCurrentThread(), "Lock must be held by the thread to call createEdgeLabel");
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edgeLabelName may not start with " + EDGE_PREFIX);
        Preconditions.checkState(!this.isSqlgSchema(), "createEdgeLabel may not be called for \"%s\"", SQLG_SCHEMA);

        Optional<Schema> inVertexSchema = this.topology.getSchema(foreignKeyIn.getSchema());
        Preconditions.checkState(inVertexSchema.isPresent(), "schema not found for \"%s\"", foreignKeyIn.getSchema());

        //The out and in vertex labels must already exist.
        Optional<VertexLabel> outVertexLabel = this.getVertexLabel(foreignKeyOut.getTable());
        Preconditions.checkState(outVertexLabel.isPresent(), "BUG: Out vertex label for edge creation can not be null. out vertex label = \"%s\"", foreignKeyOut.getTable());

        Optional<VertexLabel> inVertexLabel = inVertexSchema.get().getVertexLabel(foreignKeyIn.getTable());
        Preconditions.checkState(inVertexLabel.isPresent(), "BUG: In vertex label for edge creation can not be null. in vertex label = \"%s\"", foreignKeyIn.getTable());

        //Edge may not already exist.
        Preconditions.checkState(!getEdgeLabel(edgeLabelName).isPresent(), "BUG: Edge \"%s\" already exists!", edgeLabelName);

        TopologyManager.addEdgeLabel(sqlgGraph, this.getName(), EDGE_PREFIX + edgeLabelName, foreignKeyOut, foreignKeyIn, columns);
        return outVertexLabel.get().addEdgeLabel(sqlgGraph, edgeLabelName, inVertexLabel.get(), columns);
    }

    VertexLabel createSqlgSchemaVertexLabel(String vertexLabelName, Map<String, PropertyType> columns) {
        Preconditions.checkState(this.isSqlgSchema(), "createSqlgSchemaVertexLabel may only be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(SchemaManager.VERTEX_PREFIX), "vertex label may not start with " + SchemaManager.VERTEX_PREFIX);
        VertexLabel vertexLabel = VertexLabel.createSqlgSchemaVertexLabel(this, vertexLabelName, columns);
        this.vertexLabels.put(vertexLabelName, vertexLabel);
        return vertexLabel;
    }

    VertexLabel createVertexLabel(SqlgGraph sqlgGraph, String vertexLabelName, Map<String, PropertyType> columns) {
        Preconditions.checkState(!this.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(SchemaManager.VERTEX_PREFIX), "vertex label may not start with " + SchemaManager.VERTEX_PREFIX);
        VertexLabel vertexLabel = VertexLabel.createVertexLabel(sqlgGraph, this, vertexLabelName, columns);
        this.uncommittedVertexLabels.put(vertexLabelName, vertexLabel);
        return vertexLabel;
    }

    public void ensureVertexColumnsExist(SqlgGraph sqlgGraph, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        Preconditions.checkState(!isSqlgSchema(), "Schema.ensureVertexColumnsExist may not be called for \"%s\"", SQLG_SCHEMA);

        Optional<VertexLabel> vertexLabel = getVertexLabel(label);
        Preconditions.checkState(vertexLabel.isPresent(), String.format("BUG: vertexLabel \"%s\" must exist", label));

        //noinspection OptionalGetWithoutIsPresent
        vertexLabel.get().ensureColumnsExist(sqlgGraph, columns);
    }

    public void ensureEdgeColumnsExist(SqlgGraph sqlgGraph, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        Preconditions.checkState(!isSqlgSchema(), "Schema.ensureEdgeColumnsExist may not be called for \"%s\"", SQLG_SCHEMA);

        Optional<EdgeLabel> edgeLabel = getEdgeLabel(label);
        Preconditions.checkState(edgeLabel.isPresent(), "BUG: edgeLabel \"%s\" must exist", label);
        //noinspection OptionalGetWithoutIsPresent
        edgeLabel.get().ensureColumnsExist(sqlgGraph, columns);
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

    /**
     * Loads the existing schema from the topology.
     *
     * @param topology   The {@link Topology} that contains this schema.
     * @param schemaName The schema's name.
     * @return The loaded Schema.
     */
    static Schema loadUserSchema(Topology topology, String schemaName) {
        return new Schema(topology, schemaName);
    }


    public String getName() {
        return name;
    }

    public Topology getTopology() {
        return topology;
    }

    public Map<String, VertexLabel> getVertexLabels() {
        return this.vertexLabels;
    }

    public Map<String, VertexLabel> getUncommittedVertexLabels() {
        return this.uncommittedVertexLabels;
    }

    public boolean existVertexLabel(String vertexLabelName) {
        return getVertexLabel(vertexLabelName).isPresent();
    }

    Optional<VertexLabel> getVertexLabel(String vertexLabelName) {
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

    private Set<EdgeLabel> getEdgeLabels() {
        Set<EdgeLabel> result = new HashSet<>();
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            result.addAll(vertexLabel.getOutEdgeLabels());
        }
        if (this.topology.isHeldByCurrentThread()) {
            for (VertexLabel vertexLabel : this.uncommittedVertexLabels.values()) {
                result.addAll(vertexLabel.getOutEdgeLabels());
            }
        }
        return result;
    }

    Optional<EdgeLabel> getEdgeLabel(String edgeLabelName) {
        Preconditions.checkArgument(!edgeLabelName.startsWith(SchemaManager.EDGE_PREFIX), "edge label may not start with \"%s\"", SchemaManager.EDGE_PREFIX);
        for (EdgeLabel edgeLabel : getEdgeLabels()) {
            if (edgeLabel.getSchema().equals(this) && edgeLabel.getLabel().equals(edgeLabelName)) {
                return Optional.of(edgeLabel);
            }
        }
        return Optional.empty();
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
        if (this.topology.isHeldByCurrentThread()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
                String vertexLabelQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
                if (!filter.contains(vertexLabelQualifiedName)) {
                    result.put(vertexLabelQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());
                }
            }
        }
        for (EdgeLabel edgeLabel : this.getEdgeLabels()) {
            Preconditions.checkState(!edgeLabel.getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
            String edgeLabelQualifiedName = edgeLabel.getSchema().getName() + "." + EDGE_PREFIX + edgeLabel.getLabel();
            if (!filter.contains(edgeLabelQualifiedName)) {
                result.put(edgeLabelQualifiedName, edgeLabel.getPropertyTypeMap());
            }
        }
        return result;
    }

    public Map<String, Map<String, PropertyType>> getAllTables() {
        Map<String, Map<String, PropertyType>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            result.put(vertexQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());
        }
        if (this.topology.isHeldByCurrentThread()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
                String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
                result.put(vertexQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());
            }
        }
        for (EdgeLabel edgeLabel : this.getEdgeLabels()) {
            Preconditions.checkState(!edgeLabel.getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
            String edgeQualifiedName = this.name + "." + EDGE_PREFIX + edgeLabel.getLabel();
            result.put(edgeQualifiedName, edgeLabel.getPropertyTypeMap());
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
        if (this.topology.isHeldByCurrentThread()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
                String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
                if (selectFrom.contains(vertexQualifiedName)) {
                    result.put(vertexQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());
                }
            }
        }
        for (EdgeLabel edgeLabel : this.getEdgeLabels()) {
            Preconditions.checkState(!edgeLabel.getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
            String edgeQualifiedName = this.name + "." + EDGE_PREFIX + edgeLabel.getLabel();
            if (selectFrom.contains(edgeQualifiedName)) {
                result.put(edgeQualifiedName, edgeLabel.getPropertyTypeMap());
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
        if (this.topology.isHeldByCurrentThread()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with %s", VERTEX_PREFIX);
                String prefixedVertexName = SchemaManager.VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
                if (schemaTable.getTable().equals(prefixedVertexName)) {
                    result.putAll(vertexLabelEntry.getValue().getPropertyTypeMap());
                    break;
                }
            }
        }
        for (EdgeLabel edgeLabel : this.getEdgeLabels()) {
            Preconditions.checkState(!edgeLabel.getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
            String prefixedEdgeName = EDGE_PREFIX + edgeLabel.getLabel();
            if (schemaTable.getTable().equals(prefixedEdgeName)) {
                result.putAll(edgeLabel.getPropertyTypeMap());
                break;
            }
        }
        return result;
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with " + VERTEX_PREFIX);
            String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.of(this.getName(), prefixedVertexName);
            result.put(schemaTable, vertexLabelEntry.getValue().getTableLabels());
        }
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedResult = new HashMap<>();
        if (this.topology.isHeldByCurrentThread()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with " + VERTEX_PREFIX);
                String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
                SchemaTable schemaTable = SchemaTable.of(this.getName(), prefixedVertexName);
                uncommittedResult.put(schemaTable, vertexLabelEntry.getValue().getTableLabels());
            }
        }
        //need to fromNotifyJson in the uncommitted table labels in.
        for (Map.Entry<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTablePairEntry : uncommittedResult.entrySet()) {
            SchemaTable schemaTable = schemaTablePairEntry.getKey();
            Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedForeignKeys = schemaTablePairEntry.getValue();
            Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = result.get(schemaTable);
            if (foreignKeys != null) {
                foreignKeys.getLeft().addAll(uncommittedForeignKeys.getLeft());
                foreignKeys.getRight().addAll(uncommittedForeignKeys.getRight());
            } else {
                result.put(schemaTable, uncommittedForeignKeys);
            }
        }
        return result;
    }

    public Optional<Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels(SchemaTable schemaTable) {
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
        //need to fromNotifyJson in the uncommitted table labels in.
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
        Map<String, Set<String>> result = new HashMap<>();
        for (EdgeLabel edgeLabel : this.getEdgeLabels()) {
            Preconditions.checkState(!edgeLabel.getLabel().startsWith(EDGE_PREFIX), "edgeLabel may not start with %s", EDGE_PREFIX);
            result.put(this.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getAllEdgeForeignKeys());
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
    }

    public void afterRollback() {
        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            entry.getValue().afterRollback();
            it.remove();
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterRollback();
        }
    }

    public boolean isSqlgSchema() {
        return this.name.equals(SQLG_SCHEMA);
    }

    public void loadVertexOutEdgesAndProperties(GraphTraversalSource traversalSource, Vertex schemaVertex) {
        //First load the vertex and its properties
        List<Path> vertices = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                //a vertex does not necessarily have properties so use optional.
                .optional(
                        __.out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).as("property")
                )
                .path()
                .toList();
        for (Path vertexProperties : vertices) {
            Vertex vertexVertex = null;
            Vertex propertyVertex = null;
            List<Set<String>> labelsList = vertexProperties.labels();
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = vertexProperties.get("vertex");
                            break;
                        case "property":
                            propertyVertex = vertexProperties.get("property");
                            break;
                        case "sqlgPathFakeLabel":
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\" and \"property\" is expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String tableName = vertexVertex.value("name");
            VertexLabel vertexLabel = this.vertexLabels.get(tableName);
            if (vertexLabel == null) {
                vertexLabel = new VertexLabel(this, tableName);
                this.vertexLabels.put(tableName, vertexLabel);
            }
            if (propertyVertex != null) {
                vertexLabel.addProperty(propertyVertex);
            }
        }

        //Load the out edges. This will load all edges as all edges have a out vertex.
        List<Path> outEdges = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                //a vertex does not necessarily have properties so use optional.
                .optional(
                        __.out(SQLG_SCHEMA_OUT_EDGES_EDGE).as("outEdgeVertex")
                                .optional(
                                        __.out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE).as("property")
                                )
                )
                .path()
                .toList();
        for (Path outEdgePath : outEdges) {
            List<Set<String>> labelsList = outEdgePath.labels();
            Vertex vertexVertex = null;
            Vertex outEdgeVertex = null;
            Vertex edgePropertyVertex = null;
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = outEdgePath.get("vertex");
                            break;
                        case "outEdgeVertex":
                            outEdgeVertex = outEdgePath.get("outEdgeVertex");
                            break;
                        case "property":
                            edgePropertyVertex = outEdgePath.get("property");
                            break;
                        case "sqlgPathFakeLabel":
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\", \"outEdgeVertex\" and \"property\" is expected as a label. Found \"%s\"", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(tableName);
            Preconditions.checkState(vertexLabel != null, "vertexLabel must be present when loading outEdges. Not found for \"%s\"", tableName);
            if (outEdgeVertex != null) {
                //load the EdgeLabel
                String edgeLabelName = outEdgeVertex.value(SQLG_SCHEMA_EDGE_LABEL_NAME);
                Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
                EdgeLabel edgeLabel;
                if (!edgeLabelOptional.isPresent()) {
                    edgeLabel = EdgeLabel.loadFromDb(edgeLabelName);
                    vertexLabel.addToOutEdgeLabels(edgeLabel);
                } else {
                    edgeLabel = edgeLabelOptional.get();
                    vertexLabel.addToOutEdgeLabels(edgeLabel);
                }
                if (edgePropertyVertex != null) {
                    //load the property
                    edgeLabel.addProperty(edgePropertyVertex);
                }
            }
        }
    }

    public void loadInEdgeLabels(GraphTraversalSource traversalSource, Vertex schemaVertex) {
        //Load the in edges via the out edges. This is necessary as the out vertex is needed to know the schema the edge is in.
        //As all edges are already loaded via the out edges this will only set the in edge association.
        List<Path> inEdges = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                //a vertex does not necessarily have properties so use optional.
                .optional(
                        __.out(SQLG_SCHEMA_OUT_EDGES_EDGE).as("outEdgeVertex").in(SQLG_SCHEMA_IN_EDGES_EDGE).as("inVertex").in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("inSchema")
                )
                .path()
                .toList();
        for (Path inEdgePath : inEdges) {
            List<Set<String>> labelsList = inEdgePath.labels();
            Vertex vertexVertex = null;
            Vertex outEdgeVertex = null;
            Vertex inVertex = null;
            Vertex inSchemaVertex = null;
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = inEdgePath.get("vertex");
                            break;
                        case "outEdgeVertex":
                            outEdgeVertex = inEdgePath.get("outEdgeVertex");
                            break;
                        case "inVertex":
                            inVertex = inEdgePath.get("inVertex");
                            break;
                        case "inSchema":
                            inSchemaVertex = inEdgePath.get("inSchema");
                            break;
                        case "sqlgPathFakeLabel":
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\", \"outEdgeVertex\" and \"inVertex\" are expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(tableName);
            Preconditions.checkState(vertexLabel != null, "vertexLabel must be present when loading inEdges. Not found for %s", tableName);
            if (outEdgeVertex != null) {
                String edgeLabelName = outEdgeVertex.value(SQLG_SCHEMA_EDGE_LABEL_NAME);

                //inVertex and inSchema must be present.
                Preconditions.checkState(inVertex != null, "BUG: In vertex not found edge for \"%s\"", edgeLabelName);
                Preconditions.checkState(inSchemaVertex != null, "BUG: In schema vertex not found for edge \"%s\"", edgeLabelName);

                Optional<EdgeLabel> outEdgeLabelOptional = this.topology.getEdgeLabel(getName(), edgeLabelName);
                Preconditions.checkState(outEdgeLabelOptional.isPresent(), "BUG: EdgeLabel for \"%s\" should already be loaded", getName() + "." + edgeLabelName);
                //noinspection OptionalGetWithoutIsPresent
                EdgeLabel outEdgeLabel = outEdgeLabelOptional.get();

                String inVertexLabelName = inVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
                String inSchemaVertexLabelName = inSchemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
                Optional<VertexLabel> vertexLabelOptional = this.topology.getVertexLabel(inSchemaVertexLabelName, inVertexLabelName);
                Preconditions.checkState(vertexLabelOptional.isPresent(), "BUG: VertexLabel not found for schema %s and label %s", inSchemaVertexLabelName, inVertexLabelName);
                //noinspection OptionalGetWithoutIsPresent
                VertexLabel inVertexLabel = vertexLabelOptional.get();

                inVertexLabel.addToInEdgeLabels(outEdgeLabel);
            }
        }
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof Schema)) {
            return false;
        }
        Schema otherSchema = (Schema) other;
        return otherSchema.name.equals(this.name);
    }

    @Override
    public String toString() {
        return "schema: " + this.name;
    }

    public JsonNode toJson() {
        ObjectNode schemaNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
        schemaNode.put("name", this.getName());
        ArrayNode vertexLabelArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
        for (VertexLabel vertexLabel : this.getVertexLabels().values()) {
            vertexLabelArrayNode.add(vertexLabel.toJson());
        }
        schemaNode.set("vertexLabels", vertexLabelArrayNode);
        return schemaNode;
    }

    public Optional<JsonNode> toNotifyJson() {
        boolean foundVertexLabels = false;
        ObjectNode schemaNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
        schemaNode.put("name", this.getName());
        if (!this.getUncommittedVertexLabels().isEmpty()) {
            ArrayNode vertexLabelArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel vertexLabel : this.getUncommittedVertexLabels().values()) {
                //VertexLabel toNotifyJson always returns something even though its an Optional.
                //This is because it extends AbstractElement's toNotifyJson that does not always return something.
                @SuppressWarnings("OptionalGetWithoutIsPresent")
                JsonNode jsonNode = vertexLabel.toNotifyJson().get();
                vertexLabelArrayNode.add(jsonNode);
            }
            schemaNode.set("uncommittedVertexLabels", vertexLabelArrayNode);
            foundVertexLabels = true;
        }
        if (!this.getVertexLabels().isEmpty()) {
            ArrayNode vertexLabelArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel vertexLabel : this.getVertexLabels().values()) {
                JsonNode notifyJson = vertexLabel.toNotifyJson().get();
                if (notifyJson.get("uncommittedProperties") != null ||
                        notifyJson.get("uncommittedOutEdgeLabels") != null ||
                        notifyJson.get("uncommittedInEdgeLabels") != null ||
                        notifyJson.get("outEdgeLabels") != null ||
                        notifyJson.get("inEdgeLabels") != null) {

                    vertexLabelArrayNode.add(notifyJson);
                    foundVertexLabels = true;
                }
            }
            if (vertexLabelArrayNode.size() > 0) {
                schemaNode.set("vertexLabels", vertexLabelArrayNode);
            }
        }
        if (foundVertexLabels) {
            return Optional.of(schemaNode);
        } else {
            return Optional.empty();
        }
    }

    void fromNotifyJson(JsonNode jsonSchema) {
        for (String s : Arrays.asList("vertexLabels", "uncommittedVertexLabels")) {
            JsonNode vertexLabels = jsonSchema.get(s);
            if (vertexLabels != null) {
                for (JsonNode vertexLabelJson : vertexLabels) {
                    String vertexLabelName = vertexLabelJson.get("label").asText();
                    Optional<VertexLabel> vertexLabelOptional = getVertexLabel(vertexLabelName);
                    VertexLabel vertexLabel;
                    if (vertexLabelOptional.isPresent()) {
                        vertexLabel = vertexLabelOptional.get();
                    } else {
                        vertexLabel = new VertexLabel(this, vertexLabelName);
                        this.vertexLabels.put(vertexLabelName, vertexLabel);
                    }
                    vertexLabel.fromNotifyJson(vertexLabelJson);
                }
            }
        }
        System.out.println("test a bit");
    }
}
