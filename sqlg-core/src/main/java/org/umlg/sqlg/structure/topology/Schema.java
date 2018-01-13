package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.strategy.BaseStrategy;
import org.umlg.sqlg.structure.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Schema implements TopologyInf {

    private static Logger logger = LoggerFactory.getLogger(Schema.class);
    private SqlgGraph sqlgGraph;
    private Topology topology;
    private String name;
    private boolean committed = true;
    //The key is schema + "." + VERTEX_PREFIX + vertex label. i.e. "A.V_A"
    private Map<String, VertexLabel> vertexLabels = new HashMap<>();
    private Map<String, VertexLabel> uncommittedVertexLabels = new HashMap<>();
    public Set<String> uncommittedRemovedVertexLabels = new HashSet<>();

    private Map<String, EdgeLabel> outEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedOutEdgeLabels = new HashMap<>();
    Set<String> uncommittedRemovedEdgeLabels = new HashSet<>();

    public static final String SQLG_SCHEMA = "sqlg_schema";
    public static final String GLOBAL_UNIQUE_INDEX_SCHEMA = "gui_schema";
    private Map<String, GlobalUniqueIndex> uncommittedGlobalUniqueIndexes = new HashMap<>();
    private Set<String> uncommittedRemovedGlobalUniqueIndexes = new HashSet<>();
    Map<String, GlobalUniqueIndex> globalUniqueIndexes = new HashMap<>();
    private static final String MARKER = "~gremlin.incidentToAdjacent";

    //temporary table map. it is in a thread local as temporary tables are only valid per session/connection.
    private final ThreadLocal<Map<String, Map<String, PropertyType>>> threadLocalTemporaryTables = ThreadLocal.withInitial(HashMap::new);

    /**
     * Creates the SqlgSchema. The sqlg_schema always exist and is created via sql in {@link SqlDialect#sqlgTopologyCreationScripts()}
     *
     * @param topology A reference to the {@link Topology} that contains the sqlg_schema schema.
     * @return The Schema that represents 'sqlg_schema'
     */
    static Schema instantiateSqlgSchema(Topology topology) {
        return new Schema(topology, SQLG_SCHEMA);
    }

    /**
     * Creates the 'public' schema that always already exist and is pre-loaded in {@link Topology()} @see {@link Topology#cacheTopology()}
     *
     * @param publicSchemaName The 'public' schema's name. Sometimes its upper case (Hsqldb) sometimes lower (Postgresql)
     * @param topology         The {@link Topology} that contains the public schema.
     * @return The Schema that represents 'public'
     */
    static Schema createPublicSchema(SqlgGraph sqlgGraph, Topology topology, String publicSchemaName) {
        Schema schema = new Schema(topology, publicSchemaName);
        if (!existPublicSchema(sqlgGraph)) {
            schema.createSchemaOnDb();
        }
        schema.committed = false;
        return schema;
    }

    private static boolean existPublicSchema(SqlgGraph sqlgGraph) {
        Connection conn = sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            return sqlgGraph.getSqlDialect().schemaExists(metadata, sqlgGraph.getSqlDialect().getPublicSchema());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates the 'gui_schema'. A schema in which to store the GlobalUniqueIndexes. The 'gui_schema' always already exist and is pre-loaded in {@link Topology()} @see {@link Topology#cacheTopology()}
     *
     * @param topology The {@link Topology} that contains the public schema.
     * @return The Schema that represents 'public'
     */
    static Schema createGlobalUniqueIndexSchema(Topology topology) {
        return new Schema(topology, GLOBAL_UNIQUE_INDEX_SCHEMA);
    }

    static Schema createSchema(SqlgGraph sqlgGraph, Topology topology, String name) {
        Schema schema = new Schema(topology, name);
        Preconditions.checkArgument(!name.equals(SQLG_SCHEMA) && !sqlgGraph.getSqlDialect().getPublicSchema().equals(name), "createSchema may not be called for 'sqlg_schema' or 'public'");
        schema.createSchemaOnDb();
        TopologyManager.addSchema(sqlgGraph, name);
        schema.committed = false;
        return schema;
    }

    /**
     * Only called from {@link Topology#fromNotifyJson(int, LocalDateTime)}
     *
     * @param topology   The {@link Topology}
     * @param schemaName The schema's name
     * @return The Schema that has already been created by another graph.
     */
    static Schema instantiateSchema(Topology topology, String schemaName) {
        Schema schema = new Schema(topology, schemaName);
        return schema;
    }

    private Schema(Topology topology, String name) {
        this.topology = topology;
        this.name = name;
        this.sqlgGraph = this.topology.getSqlgGraph();
    }

    SqlgGraph getSqlgGraph() {
        return this.sqlgGraph;
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    public VertexLabel ensureVertexLabelExist(final String label) {
        return ensureVertexLabelExist(label, Collections.emptyMap());
    }

    void ensureTemporaryVertexTableExist(final String label, final Map<String, PropertyType> columns) {
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);

        final String prefixedTable = VERTEX_PREFIX + label;
        if (!this.threadLocalTemporaryTables.get().containsKey(prefixedTable)) {
            this.topology.lock();
            if (!this.threadLocalTemporaryTables.get().containsKey(prefixedTable)) {
                this.threadLocalTemporaryTables.get().put(prefixedTable, columns);
                createTempTable(prefixedTable, columns);
            }
        }
    }

    public VertexLabel ensureVertexLabelExist(final String label, final Map<String, PropertyType> columns) {
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);

        Optional<VertexLabel> vertexLabelOptional = this.getVertexLabel(label);
        if (!vertexLabelOptional.isPresent()) {
            this.topology.lock();
            vertexLabelOptional = this.getVertexLabel(label);
            if (!vertexLabelOptional.isPresent()) {
                return this.createVertexLabel(label, columns);
            } else {
                return vertexLabelOptional.get();
            }
        } else {
            VertexLabel vertexLabel = vertexLabelOptional.get();
            //check if all the columns are there.
            vertexLabel.ensurePropertiesExist(columns);
            return vertexLabel;
        }
    }

    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel outVertexLabel, final VertexLabel inVertexLabel, Map<String, PropertyType> columns) {
        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName may not be null");
        Objects.requireNonNull(outVertexLabel, "Given outVertexLabel may not be null");
        Objects.requireNonNull(inVertexLabel, "Given inVertexLabel may not be null");

        EdgeLabel edgeLabel;
        Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
        if (!edgeLabelOptional.isPresent()) {
            this.topology.lock();
            edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
            if (!edgeLabelOptional.isPresent()) {
                edgeLabel = this.createEdgeLabel(edgeLabelName, outVertexLabel, inVertexLabel, columns);
                this.uncommittedRemovedEdgeLabels.remove(this.name + "." + EDGE_PREFIX + edgeLabelName);
                this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
                this.getTopology().fire(edgeLabel, "", TopologyChangeAction.CREATE);
                //nothing more to do as the edge did not exist and will have been created with the correct foreign keys.
            } else {
                edgeLabel = internalEnsureEdgeTableExists(edgeLabelOptional.get(), outVertexLabel, inVertexLabel, columns);
            }
        } else {
            edgeLabel = internalEnsureEdgeTableExists(edgeLabelOptional.get(), outVertexLabel, inVertexLabel, columns);
        }
        return edgeLabel;
    }

    private EdgeLabel internalEnsureEdgeTableExists(EdgeLabel edgeLabel, VertexLabel outVertexLabel, VertexLabel inVertexLabel, Map<String, PropertyType> columns) {
        edgeLabel.ensureEdgeVertexLabelExist(Direction.OUT, outVertexLabel);
        edgeLabel.ensureEdgeVertexLabelExist(Direction.IN, inVertexLabel);
        edgeLabel.ensurePropertiesExist(columns);
        return edgeLabel;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private EdgeLabel createEdgeLabel(final String edgeLabelName, final VertexLabel outVertexLabel, final VertexLabel inVertexLabel, final Map<String, PropertyType> columns) {
        Preconditions.checkArgument(this.topology.isSqlWriteLockHeldByCurrentThread(), "Lock must be held by the thread to call createEdgeLabel");
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edgeLabelName may not start with " + EDGE_PREFIX);
        Preconditions.checkState(!this.isSqlgSchema(), "createEdgeLabel may not be called for \"%s\"", SQLG_SCHEMA);

        Schema inVertexSchema = inVertexLabel.getSchema();

        //Edge may not already exist.
        Preconditions.checkState(!getEdgeLabel(edgeLabelName).isPresent(), "BUG: Edge \"%s\" already exists!", edgeLabelName);

        SchemaTable foreignKeyOut = SchemaTable.of(this.name, outVertexLabel.getLabel());
        SchemaTable foreignKeyIn = SchemaTable.of(inVertexSchema.name, inVertexLabel.getLabel());

        TopologyManager.addEdgeLabel(this.sqlgGraph, this.getName(), EDGE_PREFIX + edgeLabelName, foreignKeyOut, foreignKeyIn, columns);
        if (this.sqlgGraph.getSqlDialect().needsSchemaCreationPrecommit()) {
            try {
                this.sqlgGraph.tx().getConnection().commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return outVertexLabel.addEdgeLabel(edgeLabelName, inVertexLabel, columns);
    }

    VertexLabel createSqlgSchemaVertexLabel(String vertexLabelName, Map<String, PropertyType> columns) {
        Preconditions.checkState(this.isSqlgSchema(), "createSqlgSchemaVertexLabel may only be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with " + VERTEX_PREFIX);
        VertexLabel vertexLabel = VertexLabel.createSqlgSchemaVertexLabel(this, vertexLabelName, columns);
        this.vertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
        return vertexLabel;
    }

    private VertexLabel createVertexLabel(String vertexLabelName, Map<String, PropertyType> columns) {
        Preconditions.checkState(!this.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with " + VERTEX_PREFIX);
        this.uncommittedRemovedVertexLabels.remove(this.name + "." + VERTEX_PREFIX + vertexLabelName);
        VertexLabel vertexLabel = VertexLabel.createVertexLabel(this.sqlgGraph, this, vertexLabelName, columns);
        this.uncommittedVertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
        this.getTopology().fire(vertexLabel, "", TopologyChangeAction.CREATE);
        return vertexLabel;
    }

    void ensureVertexColumnsExist(String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        Preconditions.checkState(!isSqlgSchema(), "Schema.ensureVertexLabelPropertiesExist may not be called for \"%s\"", SQLG_SCHEMA);

        Optional<VertexLabel> vertexLabel = getVertexLabel(label);
        Preconditions.checkState(vertexLabel.isPresent(), "BUG: vertexLabel \"%s\" must exist", label);

        //noinspection OptionalGetWithoutIsPresent
        vertexLabel.get().ensurePropertiesExist(columns);
    }

    void ensureEdgeColumnsExist(String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        Preconditions.checkState(!isSqlgSchema(), "Schema.ensureEdgePropertiesExist may not be called for \"%s\"", SQLG_SCHEMA);

        Optional<EdgeLabel> edgeLabel = getEdgeLabel(label);
        Preconditions.checkState(edgeLabel.isPresent(), "BUG: edgeLabel \"%s\" must exist", label);
        //noinspection OptionalGetWithoutIsPresent
        edgeLabel.get().ensurePropertiesExist(columns);
    }

    /**
     * Creates a new schema on the database. i.e. 'CREATE SCHEMA...' sql statement.
     */
    private void createSchemaOnDb() {
        StringBuilder sql = new StringBuilder();
        sql.append(topology.getSqlgGraph().getSqlDialect().createSchemaStatement(this.name));
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            logger.error("schema creation failed " + this.sqlgGraph.toString(), e);
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

    Topology getTopology() {
        return topology;
    }

    private Map<String, VertexLabel> getUncommittedVertexLabels() {
        return this.uncommittedVertexLabels;
    }

    public Optional<VertexLabel> getVertexLabel(String vertexLabelName) {
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with \"%s\"", Topology.VERTEX_PREFIX);
        if (this.topology.isSqlWriteLockHeldByCurrentThread() && this.uncommittedRemovedVertexLabels.contains(this.name + "." + VERTEX_PREFIX + vertexLabelName)) {
            return Optional.empty();
        }
        VertexLabel result = null;
        if (this.topology.isSqlWriteLockHeldByCurrentThread()) {
            result = this.uncommittedVertexLabels.get(this.name + "." + VERTEX_PREFIX + vertexLabelName);
        }
        if (result==null){
        	result = this.vertexLabels.get(this.name + "." + VERTEX_PREFIX + vertexLabelName);
        }
        return Optional.ofNullable(result);
    }

    Map<String, EdgeLabel> getEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>();
        result.putAll(this.outEdgeLabels);
        if (this.topology.isSqlWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedOutEdgeLabels);
            for (String e : uncommittedRemovedEdgeLabels) {
                result.remove(e);
            }
        }
        return result;
    }

    private Map<String, EdgeLabel> getUncommittedOutEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>();
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            result.putAll(vertexLabel.getUncommittedOutEdgeLabels());
        }
        if (this.topology.isSqlWriteLockHeldByCurrentThread()) {
            for (VertexLabel vertexLabel : this.uncommittedVertexLabels.values()) {
                result.putAll(vertexLabel.getUncommittedOutEdgeLabels());
            }
            for (String e : uncommittedRemovedEdgeLabels) {
                result.remove(e);
            }
        }
        return result;
    }

    public Optional<EdgeLabel> getEdgeLabel(String edgeLabelName) {
        Preconditions.checkArgument(!edgeLabelName.startsWith(Topology.EDGE_PREFIX), "edge label may not start with \"%s\"", Topology.EDGE_PREFIX);
        if (this.topology.isSqlWriteLockHeldByCurrentThread() && this.uncommittedRemovedEdgeLabels.contains(this.name + "." + EDGE_PREFIX + edgeLabelName)) {
            return Optional.empty();
        }
        EdgeLabel edgeLabel = this.outEdgeLabels.get(this.name + "." + EDGE_PREFIX + edgeLabelName);
        if (edgeLabel != null) {
            return Optional.of(edgeLabel);
        }
        if (this.topology.isSqlWriteLockHeldByCurrentThread()) {
            edgeLabel = this.uncommittedOutEdgeLabels.get(this.name + "." + EDGE_PREFIX + edgeLabelName);
            if (edgeLabel != null) {
                return Optional.of(edgeLabel);
            }
        }
        return Optional.empty();
    }

    //remove in favour of PropertyColumn
    Map<String, Map<String, PropertyType>> getAllTables() {
        Map<String, Map<String, PropertyType>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            result.put(vertexQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());
        }
        if (this.topology.isSqlWriteLockHeldByCurrentThread()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                String vertexQualifiedName = vertexLabelEntry.getKey();
                VertexLabel vertexLabel = vertexLabelEntry.getValue();
                result.put(vertexQualifiedName, vertexLabel.getPropertyTypeMap());
            }
        }
        for (EdgeLabel edgeLabel : this.getEdgeLabels().values()) {
            String edgeQualifiedName = this.name + "." + EDGE_PREFIX + edgeLabel.getLabel();
            result.put(edgeQualifiedName, edgeLabel.getPropertyTypeMap());
        }
        return result;
    }

    Map<String, VertexLabel> getVertexLabelsOnly() {
        return this.vertexLabels;
    }

    public Map<String, VertexLabel> getVertexLabels() {
        Map<String, VertexLabel> result = new HashMap<>();
        result.putAll(this.vertexLabels);
        if (this.topology.isSqlWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedVertexLabels);
            for (String e : uncommittedRemovedVertexLabels) {
                result.remove(e);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    Map<String, AbstractLabel> getUncommittedLabels() {
        Preconditions.checkState(getTopology().isSqlWriteLockHeldByCurrentThread(), "Schema.getUncommittedAllTables must be called with the lock held");
        Map<String, AbstractLabel> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            Map<String, PropertyColumn> uncommittedPropertyColumnMap = vertexLabelEntry.getValue().getUncommittedPropertyTypeMap();
            Set<String> uncommittedRemovedProperties = vertexLabelEntry.getValue().getUncommittedRemovedProperties();
            if (!uncommittedPropertyColumnMap.isEmpty() || !uncommittedRemovedProperties.isEmpty()) {
                result.put(vertexQualifiedName, vertexLabelEntry.getValue());
            }
        }
        for (Map.Entry<String, VertexLabel> stringVertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
            String vertexQualifiedLabel = stringVertexLabelEntry.getKey();
            VertexLabel vertexLabel = stringVertexLabelEntry.getValue();
            result.put(vertexQualifiedLabel, vertexLabel);
        }
        for (EdgeLabel edgeLabel : this.getUncommittedOutEdgeLabels().values()) {
            result.put(this.name + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel);
        }
        return result;
    }

    Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getUncommittedSchemaTableForeignKeys() {
        Preconditions.checkState(getTopology().isSqlWriteLockHeldByCurrentThread(), "Schema.getUncommittedSchemaTableForeignKeys must be called with the lock held");
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.from(this.sqlgGraph, vertexQualifiedName);
            Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedSchemaTableForeignKeys = vertexLabelEntry.getValue().getUncommittedSchemaTableForeignKeys();
            if (!uncommittedSchemaTableForeignKeys.getLeft().isEmpty() || !uncommittedSchemaTableForeignKeys.getRight().isEmpty()) {
                result.put(schemaTable, uncommittedSchemaTableForeignKeys);
            }
        }
        for (Map.Entry<String, VertexLabel> uncommittedVertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + uncommittedVertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.from(this.sqlgGraph, vertexQualifiedName);
            Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedSchemaTableForeignKeys = uncommittedVertexLabelEntry.getValue().getUncommittedSchemaTableForeignKeys();
            result.put(schemaTable, uncommittedSchemaTableForeignKeys);
        }
        return result;
    }

    Map<String, Set<String>> getUncommittedEdgeForeignKeys() {
        Map<String, Set<String>> result = new HashMap<>();
        for (EdgeLabel outEdgeLabel : this.outEdgeLabels.values()) {
            result.put(this.getName() + "." + Topology.EDGE_PREFIX + outEdgeLabel.getLabel(), outEdgeLabel.getUncommittedEdgeForeignKeys());
        }
        for (EdgeLabel outEdgeLabel : this.uncommittedOutEdgeLabels.values()) {
            result.put(this.getName() + "." + Topology.EDGE_PREFIX + outEdgeLabel.getLabel(), outEdgeLabel.getUncommittedEdgeForeignKeys());
        }
        return result;
    }

    Map<String, PropertyColumn> getPropertiesFor(SchemaTable schemaTable) {
        Preconditions.checkArgument(schemaTable.getTable().startsWith(VERTEX_PREFIX) || schemaTable.getTable().startsWith(EDGE_PREFIX), "label must start with \"%s\" or \"%s\"", Topology.VERTEX_PREFIX, Topology.EDGE_PREFIX);
        if (schemaTable.isVertexTable()) {
            Optional<VertexLabel> vertexLabelOptional = getVertexLabel(schemaTable.withOutPrefix().getTable());
            if (vertexLabelOptional.isPresent()) {
                return vertexLabelOptional.get().getProperties();
            }
        } else {
            Optional<EdgeLabel> edgeLabelOptional = getEdgeLabel(schemaTable.withOutPrefix().getTable());
            if (edgeLabelOptional.isPresent()) {
                return edgeLabelOptional.get().getProperties();
            }
        }
        return Collections.emptyMap();
    }

    Map<String, PropertyColumn> getPropertiesWithGlobalUniqueIndexFor(SchemaTable schemaTable) {
        Preconditions.checkArgument(schemaTable.getTable().startsWith(VERTEX_PREFIX) || schemaTable.getTable().startsWith(EDGE_PREFIX), "label must start with \"%s\" or \"%s\"", VERTEX_PREFIX, EDGE_PREFIX);
        if (schemaTable.isVertexTable()) {
            Optional<VertexLabel> vertexLabelOptional = getVertexLabel(schemaTable.withOutPrefix().getTable());
            if (vertexLabelOptional.isPresent()) {
                return vertexLabelOptional.get().getGlobalUniqueIndexProperties();
            }
        } else {
            Optional<EdgeLabel> edgeLabelOptional = getEdgeLabel(schemaTable.withOutPrefix().getTable());
            if (edgeLabelOptional.isPresent()) {
                return edgeLabelOptional.get().getGlobalUniqueIndexProperties();
            }
        }
        return Collections.emptyMap();
    }

    Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Preconditions.checkArgument(schemaTable.getTable().startsWith(VERTEX_PREFIX) || schemaTable.getTable().startsWith(EDGE_PREFIX), "label must start with \"%s\" or \"%s\"", VERTEX_PREFIX, EDGE_PREFIX);
        if (schemaTable.isVertexTable()) {
            Optional<VertexLabel> vertexLabelOptional = getVertexLabel(schemaTable.withOutPrefix().getTable());
            if (vertexLabelOptional.isPresent()) {
                return vertexLabelOptional.get().getPropertyTypeMap();
            }
        } else {
            Optional<EdgeLabel> edgeLabelOptional = getEdgeLabel(schemaTable.withOutPrefix().getTable());
            if (edgeLabelOptional.isPresent()) {
                return edgeLabelOptional.get().getPropertyTypeMap();
            }
        }
        return Collections.emptyMap();
    }

    Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with " + VERTEX_PREFIX);
            String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.of(this.getName(), prefixedVertexName);
            result.put(schemaTable, vertexLabelEntry.getValue().getTableLabels());
        }
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedResult = new HashMap<>();
        if (this.topology.isSqlWriteLockHeldByCurrentThread()) {
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

    Map<String, Set<String>> getAllEdgeForeignKeys() {
        Map<String, Set<String>> result = new HashMap<>();
        for (Map.Entry<String, EdgeLabel> stringEdgeLabelEntry : getEdgeLabels().entrySet()) {
            String edgeSchemaAndLabel = stringEdgeLabelEntry.getKey();
            EdgeLabel edgeLabel = stringEdgeLabelEntry.getValue();
            result.put(edgeSchemaAndLabel, edgeLabel.getAllEdgeForeignKeys());
        }
        return result;
    }

    public GlobalUniqueIndex ensureGlobalUniqueIndexExist(final Set<PropertyColumn> properties) {
        String globalUniqueIndexName = GlobalUniqueIndex.globalUniqueIndexName(this.topology, properties);
        Optional<GlobalUniqueIndex> globalIndexOptional = this.getGlobalUniqueIndex(globalUniqueIndexName);
        if (!globalIndexOptional.isPresent()) {
            //take any property
            properties.iterator().next().getParentLabel().getSchema().getTopology().lock();
            globalIndexOptional = this.getGlobalUniqueIndex(globalUniqueIndexName);
            if (!globalIndexOptional.isPresent()) {
                GlobalUniqueIndex globalUniqueIndex = GlobalUniqueIndex.createGlobalUniqueIndex(this.sqlgGraph, this.topology, globalUniqueIndexName, properties);
                this.uncommittedGlobalUniqueIndexes.put(globalUniqueIndexName, globalUniqueIndex);
                this.getTopology().fire(globalUniqueIndex, "", TopologyChangeAction.CREATE);
                return globalUniqueIndex;
            } else {
                return globalIndexOptional.get();
            }
        } else {
            return globalIndexOptional.get();
        }
    }

    public Optional<GlobalUniqueIndex> getGlobalUniqueIndex(String name) {
        Objects.requireNonNull(name, () -> "name may not be null for getGlobalUniqueIndex");
        GlobalUniqueIndex globalUniqueIndex = getGlobalUniqueIndexes().get(name);
        return Optional.ofNullable(globalUniqueIndex);
    }

    public Map<String, GlobalUniqueIndex> getGlobalUniqueIndexes() {
        Map<String, GlobalUniqueIndex> result = new HashMap<>();
        result.putAll(this.globalUniqueIndexes);
        if (this.getTopology().isSqlWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedGlobalUniqueIndexes);
            for (String s : this.uncommittedRemovedGlobalUniqueIndexes) {
                result.remove(s);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    void afterCommit() {
        Preconditions.checkState(this.getTopology().isSqlWriteLockHeldByCurrentThread(), "Schema.afterCommit must hold the write lock");
        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            this.vertexLabels.put(entry.getKey(), entry.getValue());
            it.remove();
        }
        if (getName().equals(GLOBAL_UNIQUE_INDEX_SCHEMA)) {
            for (Iterator<Map.Entry<String, GlobalUniqueIndex>> it = this.uncommittedGlobalUniqueIndexes.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, GlobalUniqueIndex> entry = it.next();
                this.globalUniqueIndexes.put(entry.getKey(), entry.getValue());
                it.remove();
            }
        }
        for (Iterator<String> it = uncommittedRemovedVertexLabels.iterator(); it.hasNext(); ) {
            String s = it.next();
            VertexLabel lbl = this.vertexLabels.remove(s);
            if (lbl != null) {
                this.getTopology().removeVertexLabel(lbl);
            }
            it.remove();
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterCommit();
        }
        if (getName().equals(GLOBAL_UNIQUE_INDEX_SCHEMA)) {
            for (GlobalUniqueIndex globalUniqueIndex : this.globalUniqueIndexes.values()) {
                globalUniqueIndex.afterCommit();
            }
            for (Iterator<String> it = uncommittedRemovedGlobalUniqueIndexes.iterator(); it.hasNext(); ) {
                String s = it.next();
                this.globalUniqueIndexes.remove(s);
                it.remove();
            }
        }
        for (Iterator<String> it = uncommittedRemovedEdgeLabels.iterator(); it.hasNext(); ) {
            String s = it.next();
            this.outEdgeLabels.remove(s);
            it.remove();
        }

        this.uncommittedOutEdgeLabels.clear();
        this.committed = true;
    }

    void afterRollback() {
        Preconditions.checkState(this.getTopology().isSqlWriteLockHeldByCurrentThread(), "Schema.afterRollback must hold the write lock");
        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            entry.getValue().afterRollbackForInEdges();
            it.remove();
        }
        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            entry.getValue().afterRollbackForOutEdges();
            it.remove();
        }
        if (getName().equals(GLOBAL_UNIQUE_INDEX_SCHEMA)) {
            for (Iterator<Map.Entry<String, GlobalUniqueIndex>> it = this.uncommittedGlobalUniqueIndexes.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, GlobalUniqueIndex> entry = it.next();
                entry.getValue().afterRollback();
                it.remove();
            }
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterRollbackForInEdges();
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterRollbackForOutEdges();
        }
        if (getName().equals(GLOBAL_UNIQUE_INDEX_SCHEMA)) {
            for (GlobalUniqueIndex globalUniqueIndex : this.globalUniqueIndexes.values()) {
                globalUniqueIndex.afterRollback();
            }
        }
        this.uncommittedOutEdgeLabels.clear();
        this.uncommittedRemovedEdgeLabels.clear();
        this.uncommittedRemovedVertexLabels.clear();
        this.uncommittedRemovedGlobalUniqueIndexes.clear();
    }

    boolean isSqlgSchema() {
        return this.name.equals(SQLG_SCHEMA);
    }

    void loadVertexOutEdgesAndProperties(GraphTraversalSource traversalSource, Vertex schemaVertex) {
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
                        case MARKER:
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\" and \"property\" is expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            if (vertexLabel == null) {
                vertexLabel = new VertexLabel(this, tableName);
                this.vertexLabels.put(schemaName + "." + VERTEX_PREFIX + tableName, vertexLabel);
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
                        case MARKER:
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\", \"outEdgeVertex\" and \"property\" is expected as a label. Found \"%s\"", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            Preconditions.checkState(vertexLabel != null, "vertexLabel must be present when loading outEdges. Not found for \"%s\"", schemaName + "." + VERTEX_PREFIX + tableName);
            if (outEdgeVertex != null) {
                //load the EdgeLabel
                String edgeLabelName = outEdgeVertex.value(SQLG_SCHEMA_EDGE_LABEL_NAME);
                Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
                EdgeLabel edgeLabel;
                if (!edgeLabelOptional.isPresent()) {
                    edgeLabel = EdgeLabel.loadFromDb(vertexLabel.getSchema().getTopology(), edgeLabelName);
                    vertexLabel.addToOutEdgeLabels(schemaName, edgeLabel);
                } else {
                    edgeLabel = edgeLabelOptional.get();
                    vertexLabel.addToOutEdgeLabels(schemaName, edgeLabel);
                }
                if (edgePropertyVertex != null) {
                    //load the property
                    edgeLabel.addProperty(edgePropertyVertex);
                }
                this.outEdgeLabels.put(schemaName + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
            }
        }

    }

    /**
     * load indices for all vertices in schema
     *
     * @param traversalSource
     * @param schemaVertex
     */
    void loadVertexIndices(GraphTraversalSource traversalSource, Vertex schemaVertex) {
    	 List<Path> indices = traversalSource
                 .V(schemaVertex)
                 .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                 .out(SQLG_SCHEMA_VERTEX_INDEX_EDGE).as("index")
                 .outE(SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                 .order().by(SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE)
                 .inV().as("property")
                 .path()
                 .toList();
        for (Path vertexIndices : indices) {
            Vertex vertexVertex = null;
            Vertex vertexIndex = null;
            Vertex propertyIndex = null;
            List<Set<String>> labelsList = vertexIndices.labels();
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = vertexIndices.get("vertex");
                            break;
                        case "index":
                            vertexIndex = vertexIndices.get("index");
                            break;
                        case "property":
                            propertyIndex = vertexIndices.get("property");
                            break;
                        case BaseStrategy.SQLG_PATH_FAKE_LABEL:
                        case BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL:
                        case Schema.MARKER:
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\",\"index\" and \"property\" is expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            if (vertexLabel == null) {
                vertexLabel = new VertexLabel(this, tableName);
                this.vertexLabels.put(schemaName + "." + VERTEX_PREFIX + tableName, vertexLabel);
            }
            if (vertexIndex != null) {
                String indexName = vertexIndex.value(SQLG_SCHEMA_INDEX_NAME);
                Optional<Index> oidx = vertexLabel.getIndex(indexName);
                Index idx;
                if (oidx.isPresent()) {
                    idx = oidx.get();
                } else {
                    idx = new Index(indexName, IndexType.fromString(vertexIndex.value(SQLG_SCHEMA_INDEX_INDEX_TYPE)), vertexLabel);
                    vertexLabel.addIndex(idx);
                }
                if (propertyIndex != null) {
                    String propertyName = propertyIndex.value(SQLG_SCHEMA_PROPERTY_NAME);
                    vertexLabel.getProperty(propertyName).ifPresent((PropertyColumn pc) -> idx.addProperty(pc));
                }

            }
        }
    }

    void loadInEdgeLabels(GraphTraversalSource traversalSource, Vertex schemaVertex) {
        //Load the in edges via the out edges. This is necessary as the out vertex is needed to know the schema the edge is in.
        //As all edges are already loaded via the out edges this will only set the in edge association.
        List<Path> inEdges = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                //a vertex does not necessarily have properties so use optional.
                .optional(
                        __.out(SQLG_SCHEMA_OUT_EDGES_EDGE).as("outEdgeVertex")
                                .in(SQLG_SCHEMA_IN_EDGES_EDGE).as("inVertex")
                                .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("inSchema")
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
                        case MARKER:
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\", \"outEdgeVertex\" and \"inVertex\" are expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            Preconditions.checkState(vertexLabel != null, "vertexLabel must be present when loading inEdges. Not found for %s", schemaName + "." + VERTEX_PREFIX + tableName);
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

    /**
     * load indices for (out) edges on all vertices of schema
     *
     * @param traversalSource
     * @param schemaVertex
     */
    void loadEdgeIndices(GraphTraversalSource traversalSource, Vertex schemaVertex) {
        List<Path> indices = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                .out(SQLG_SCHEMA_OUT_EDGES_EDGE).as("outEdgeVertex")
                .out(SQLG_SCHEMA_EDGE_INDEX_EDGE).as("index")
                .outE(SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .order().by(SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE)
                .inV().as("property")
                .path()
                .toList();
        for (Path vertexIndices : indices) {
            Vertex vertexVertex = null;
            Vertex vertexEdge = null;
            Vertex vertexIndex = null;
            Vertex propertyIndex = null;
            List<Set<String>> labelsList = vertexIndices.labels();
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = vertexIndices.get("vertex");
                            break;
                        case "outEdgeVertex":
                            vertexEdge = vertexIndices.get("outEdgeVertex");
                            break;
                        case "index":
                            vertexIndex = vertexIndices.get("index");
                            break;
                        case "property":
                            propertyIndex = vertexIndices.get("property");
                            break;
                        case BaseStrategy.SQLG_PATH_FAKE_LABEL:
                        case BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL:
                        case MARKER:
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\",\"outEdgeVertex\",\"index\" and \"property\" is expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            if (vertexLabel == null) {
                vertexLabel = new VertexLabel(this, tableName);
                this.vertexLabels.put(schemaName + "." + VERTEX_PREFIX + tableName, vertexLabel);
            }
            if (vertexEdge != null) {
                String edgeName = vertexEdge.value(SQLG_SCHEMA_EDGE_LABEL_NAME);
                Optional<EdgeLabel> oel = vertexLabel.getOutEdgeLabel(edgeName);
                if (oel.isPresent()) {
                    EdgeLabel edgeLabel = oel.get();
                    if (vertexIndex != null) {
                        String indexName = vertexIndex.value(SQLG_SCHEMA_INDEX_NAME);
                        Optional<Index> oidx = edgeLabel.getIndex(indexName);
                        Index idx;
                        if (oidx.isPresent()) {
                            idx = oidx.get();
                        } else {
                            idx = new Index(indexName, IndexType.fromString(vertexIndex.value(SQLG_SCHEMA_INDEX_INDEX_TYPE)), edgeLabel);
                            edgeLabel.addIndex(idx);
                        }
                        if (propertyIndex != null) {
                            String propertyName = propertyIndex.value(SQLG_SCHEMA_PROPERTY_NAME);
                            edgeLabel.getProperty(propertyName).ifPresent((PropertyColumn pc) -> idx.addProperty(pc));
                        }

                    }
                }
            }
        }
    }

    JsonNode toJson() {
        ObjectNode schemaNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        schemaNode.put("name", this.getName());
        ArrayNode vertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (VertexLabel vertexLabel : this.getVertexLabels().values()) {
            vertexLabelArrayNode.add(vertexLabel.toJson());
        }
        schemaNode.set("vertexLabels", vertexLabelArrayNode);
        return schemaNode;
    }

    Optional<JsonNode> toNotifyJson() {
        boolean foundVertexLabels = false;
        ObjectNode schemaNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        schemaNode.put("name", this.getName());
        if (this.getTopology().isSqlWriteLockHeldByCurrentThread() && !this.getUncommittedVertexLabels().isEmpty()) {
            ArrayNode vertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
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
        if (this.getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedRemovedVertexLabels.isEmpty()) {
            ArrayNode vertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String s : this.uncommittedRemovedVertexLabels) {
                vertexLabelArrayNode.add(s);
            }
            schemaNode.set("uncommittedRemovedVertexLabels", vertexLabelArrayNode);
            foundVertexLabels = true;
        }
        if (this.getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedRemovedEdgeLabels.isEmpty()) {
            ArrayNode edgeLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String s : this.uncommittedRemovedEdgeLabels) {
                edgeLabelArrayNode.add(s);
            }
            schemaNode.set("uncommittedRemovedEdgeLabels", edgeLabelArrayNode);
            foundVertexLabels = true;
        }
        if (this.getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedGlobalUniqueIndexes.isEmpty()) {
            ArrayNode unCommittedGlobalUniqueIndexesArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
            for (GlobalUniqueIndex globalUniqueIndex : this.uncommittedGlobalUniqueIndexes.values()) {

                Optional<JsonNode> jsonNodeOptional = globalUniqueIndex.toNotifyJson();
                if (jsonNodeOptional.isPresent()) {
                    unCommittedGlobalUniqueIndexesArrayNode.add(jsonNodeOptional.get());
                }

            }
            schemaNode.set("uncommittedGlobalUniqueIndexes", unCommittedGlobalUniqueIndexesArrayNode);
        }
        if (this.getTopology().isSqlWriteLockHeldByCurrentThread() && !this.uncommittedRemovedGlobalUniqueIndexes.isEmpty()) {
            ArrayNode unCommittedGlobalUniqueIndexesArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
            for (String globalUniqueIndex : this.uncommittedRemovedGlobalUniqueIndexes) {
                unCommittedGlobalUniqueIndexesArrayNode.add(globalUniqueIndex);
            }

            schemaNode.set("uncommittedRemovedGlobalUniqueIndexes", unCommittedGlobalUniqueIndexesArrayNode);
        }

        if (!this.getVertexLabelsOnly().isEmpty()) {
            ArrayNode vertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel vertexLabel : this.getVertexLabelsOnly().values()) {
                Optional<JsonNode> notifyJsonOptional = vertexLabel.toNotifyJson();
                if (notifyJsonOptional.isPresent()) {
                    JsonNode notifyJson = notifyJsonOptional.get();
                    if (notifyJson.get("uncommittedProperties") != null ||
                            notifyJson.get("uncommittedOutEdgeLabels") != null ||
                            notifyJson.get("uncommittedInEdgeLabels") != null ||
                            notifyJson.get("outEdgeLabels") != null ||
                            notifyJson.get("inEdgeLabels") != null ||
                            notifyJson.get("uncommittedRemovedOutEdgeLabels") != null ||
                            notifyJson.get("uncommittedRemovedInEdgeLabels") != null
                            ) {

                        vertexLabelArrayNode.add(notifyJsonOptional.get());
                        foundVertexLabels = true;
                    }
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

    void fromNotifyJsonOutEdges(JsonNode jsonSchema) {
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
                        this.vertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
                        this.getTopology().fire(vertexLabel, "", TopologyChangeAction.CREATE);
                    }
                    //The order of the next two statements matter.
                    //fromNotifyJsonOutEdge needs to happen first to ensure the properties are on the VertexLabel
                    // fire only if we didn't create the vertex label
                    vertexLabel.fromNotifyJsonOutEdge(vertexLabelJson, vertexLabelOptional.isPresent());
                    this.getTopology().addToAllTables(this.getName() + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel.getPropertyTypeMap());
                }
            }
        }
        JsonNode rem = jsonSchema.get("uncommittedRemovedVertexLabels");
        if (rem != null && rem.isArray()) {
            ArrayNode an = (ArrayNode) rem;
            for (int a = 0; a < an.size(); a++) {
                String s = an.get(a).asText();
                VertexLabel lbl = this.vertexLabels.remove(s);
                if (lbl != null) {
                    this.getTopology().removeVertexLabel(lbl);
                    for (EdgeRole er : lbl.getOutEdgeRoles().values()) {
                        er.getEdgeLabel().outVertexLabels.remove(lbl);
                    }
                    for (EdgeRole er : lbl.getInEdgeRoles().values()) {
                        er.getEdgeLabel().inVertexLabels.remove(lbl);
                    }
                    this.getTopology().fire(lbl, "", TopologyChangeAction.DELETE);

                }
            }
        }

        rem = jsonSchema.get("uncommittedRemovedEdgeLabels");
        if (rem != null && rem.isArray()) {
            ArrayNode an = (ArrayNode) rem;
            for (int a = 0; a < an.size(); a++) {
                String s = an.get(a).asText();
                EdgeLabel edgeLabel = this.outEdgeLabels.remove(s);
                if (edgeLabel != null) {
                    for (VertexLabel lbl : edgeLabel.getOutVertexLabels()) {
                        if (edgeLabel.isValid()) {
                            lbl.outEdgeLabels.remove(edgeLabel.getFullName());
                        }
                    }
                    for (VertexLabel lbl : edgeLabel.getInVertexLabels()) {
                        if (edgeLabel.isValid()) {
                            lbl.inEdgeLabels.remove(edgeLabel.getFullName());
                        }
                    }
                    this.getTopology().fire(edgeLabel, "", TopologyChangeAction.DELETE);
                }
            }
        }

        ArrayNode globalUniqueIndexes = (ArrayNode) jsonSchema.get("uncommittedGlobalUniqueIndexes");
        if (globalUniqueIndexes != null) {
            for (JsonNode jsonGlobalUniqueIndex : globalUniqueIndexes) {
                String globalUniqueIndexName = jsonGlobalUniqueIndex.get("name").asText();
                GlobalUniqueIndex globalUniqueIndex = this.globalUniqueIndexes.get(globalUniqueIndexName);
                if (globalUniqueIndex == null) {
                    globalUniqueIndex = GlobalUniqueIndex.instantiateGlobalUniqueIndex(getTopology(), globalUniqueIndexName);
                    getTopology().fire(globalUniqueIndex, "", TopologyChangeAction.CREATE);
                }
                Set<PropertyColumn> properties = new HashSet<>();
                ArrayNode jsonProperties = (ArrayNode) jsonGlobalUniqueIndex.get("uncommittedProperties");
                for (JsonNode jsonProperty : jsonProperties) {
                    ObjectNode propertyObjectNode = (ObjectNode) jsonProperty;
                    String propertyName = propertyObjectNode.get("name").asText();
                    String schemaName = propertyObjectNode.get("schemaName").asText();
                    Optional<Schema> schemaOptional = getTopology().getSchema(schemaName);
                    Preconditions.checkState(schemaOptional.isPresent(), "Schema must be present for GlobalUniqueIndexes fromNotifyJson");
                    Schema schema = schemaOptional.get();
                    String abstractLabelName = propertyObjectNode.get("abstractLabelLabel").asText();
                    AbstractLabel abstractLabel;
                    Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(abstractLabelName);
                    if (!vertexLabelOptional.isPresent()) {
                        Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(abstractLabelName);
                        Preconditions.checkState(edgeLabelOptional.isPresent(), "VertexLabel or EdgeLabl must be present for GlobalUniqueIndex fromNotifyJson");
                        abstractLabel = edgeLabelOptional.get();
                    } else {
                        abstractLabel = vertexLabelOptional.get();
                    }
                    Optional<PropertyColumn> propertyColumnOptional = abstractLabel.getProperty(propertyName);
                    Preconditions.checkState(propertyColumnOptional.isPresent(), "PropertyColumn must be present for GlobalUniqueIndex fromNotifyJson");
                    properties.add(propertyColumnOptional.get());
                }
                globalUniqueIndex.addGlobalUniqueProperties(properties);
                this.globalUniqueIndexes.put(globalUniqueIndexName, globalUniqueIndex);
            }

        }
        ArrayNode remGlobalUniqueIndexes = (ArrayNode) jsonSchema.get("uncommittedRemovedGlobalUniqueIndexes");
        if (remGlobalUniqueIndexes != null) {
            for (JsonNode jsonGlobalUniqueIndex : remGlobalUniqueIndexes) {
                String globalUniqueIndexName = jsonGlobalUniqueIndex.asText();
                GlobalUniqueIndex gui = this.globalUniqueIndexes.remove(globalUniqueIndexName);
                if (gui != null) {
                    this.getTopology().fire(gui, "", TopologyChangeAction.DELETE);
                }
            }
        }


    }

    void fromNotifyJsonInEdges(JsonNode jsonSchema) {
        JsonNode rem = jsonSchema.get("uncommittedRemovedVertexLabels");
        Set<String> removed = new HashSet<>();
        if (rem != null && rem.isArray()) {
            ArrayNode an = (ArrayNode) rem;
            for (int a = 0; a < an.size(); a++) {
                String s = an.get(a).asText();
                removed.add(s);
            }
        }
        for (String s : Arrays.asList("vertexLabels", "uncommittedVertexLabels")) {
            JsonNode vertexLabels = jsonSchema.get(s);
            if (vertexLabels != null) {
                for (JsonNode vertexLabelJson : vertexLabels) {
                    String vertexLabelName = vertexLabelJson.get("label").asText();
                    if (!removed.contains(this.name + "." + VERTEX_PREFIX + vertexLabelName)) {
                        Optional<VertexLabel> vertexLabelOptional = getVertexLabel(vertexLabelName);
                        Preconditions.checkState(vertexLabelOptional.isPresent(), "VertexLabel " + vertexLabelName + " must be present");
                        @SuppressWarnings("OptionalGetWithoutIsPresent")
                        VertexLabel vertexLabel = vertexLabelOptional.get();
                        vertexLabel.fromNotifyJsonInEdge(vertexLabelJson);
                    }
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Schema)) {
            return false;
        }
        //only equals on the name of the schema. it is assumed that the VertexLabels (tables) are the same.
        Schema other = (Schema) o;
        return this.name.equals(other.name);
    }

    boolean deepEquals(Schema other) {
        Preconditions.checkState(this.name.equals(other.name), "deepEquals is called after the regular equals. i.e. the names must be equals");
        if (!(this.vertexLabels.equals(other.getVertexLabels()))) {
            return false;
        } else {
            if (!this.vertexLabels.equals(other.getVertexLabels())) {
                return false;
            }
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
                VertexLabel vertexLabel = vertexLabelEntry.getValue();
                VertexLabel otherVertexLabel = other.getVertexLabels().get(vertexLabelEntry.getKey());
                if (!vertexLabel.deepEquals(otherVertexLabel)) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public String toString() {
        return "schema: " + this.name;
    }

    void cacheEdgeLabels() {
        for (Map.Entry<String, VertexLabel> entry : this.vertexLabels.entrySet()) {
            for (EdgeLabel edgeLabel : entry.getValue().getOutEdgeLabels().values()) {
                this.outEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel);
            }
        }
    }

    void addToAllEdgeCache(EdgeLabel edgeLabel) {
        this.outEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel);
    }

    List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException {
        List<Topology.TopologyValidationError> validationErrors = new ArrayList<>();
        for (VertexLabel vertexLabel : getVertexLabels().values()) {
            try (ResultSet tableRs = metadata.getTables(null, this.getName(), "V_" + vertexLabel.getLabel(), null)) {
                if (!tableRs.next()) {
                    validationErrors.add(new Topology.TopologyValidationError(vertexLabel));
                } else {
                    validationErrors.addAll(vertexLabel.validateTopology(metadata));
                    //validate edges
                    for (EdgeLabel edgeLabel : vertexLabel.getOutEdgeLabels().values()) {
                        try (ResultSet edgeRs = metadata.getTables(null, this.getName(), "E_" + edgeLabel.getLabel(), null)) {
                            if (!edgeRs.next()) {
                                validationErrors.add(new Topology.TopologyValidationError(edgeLabel));
                            } else {
                                validationErrors.addAll(edgeLabel.validateTopology(metadata));
                            }
                        }
                    }
                }
            }
        }
        return validationErrors;
    }


    @Override
    public void remove(boolean preserveData) {
        /*if (this.getName().equals(sqlgGraph.getSqlDialect().getPublicSchema()) && !preserveData){
            throw new IllegalArgumentException("Public schema cannot be deleted");
    	}*/
        getTopology().removeSchema(this, preserveData);
    }

    /**
     * remove a given edge label
     *
     * @param edgeLabel    the edge label
     * @param preserveData should we keep the SQL data
     */
    void removeEdgeLabel(EdgeLabel edgeLabel, boolean preserveData) {
        getTopology().lock();
        String fn = this.name + "." + EDGE_PREFIX + edgeLabel.getName();

        if (!uncommittedRemovedEdgeLabels.contains(fn)) {
            uncommittedRemovedEdgeLabels.add(fn);
            TopologyManager.removeEdgeLabel(this.sqlgGraph, edgeLabel);
            for (VertexLabel lbl : edgeLabel.getOutVertexLabels()) {
                lbl.removeOutEdge(edgeLabel);
            }
            for (VertexLabel lbl : edgeLabel.getInVertexLabels()) {
                lbl.removeInEdge(edgeLabel);
            }

            if (!preserveData) {
                edgeLabel.delete();
            }
            getTopology().fire(edgeLabel, "", TopologyChangeAction.DELETE);
        }
    }

    /**
     * remove a given vertex label
     *
     * @param vertexLabel  the vertex label
     * @param preserveData should we keep the SQL data
     */
    void removeVertexLabel(VertexLabel vertexLabel, boolean preserveData) {
        getTopology().lock();
        String fn = this.name + "." + VERTEX_PREFIX + vertexLabel.getName();
        if (!uncommittedRemovedVertexLabels.contains(fn)) {
            uncommittedRemovedVertexLabels.add(fn);
            TopologyManager.removeVertexLabel(this.sqlgGraph, vertexLabel);
            for (EdgeRole er : vertexLabel.getOutEdgeRoles().values()) {
                er.remove(preserveData);
            }
            for (EdgeRole er : vertexLabel.getInEdgeRoles().values()) {
                er.remove(preserveData);
            }
            if (!preserveData) {
                vertexLabel.delete();
            }
            getTopology().fire(vertexLabel, "", TopologyChangeAction.DELETE);
        }

    }

    /**
     * remove the given global unique index
     *
     * @param index        the index to remove
     * @param preserveData should we keep the sql data?
     */
    void removeGlobalUniqueIndex(GlobalUniqueIndex index, boolean preserveData) {
        getTopology().lock();
        String fn = index.getName();
        if (!uncommittedRemovedGlobalUniqueIndexes.contains(fn)) {
            uncommittedRemovedGlobalUniqueIndexes.add(fn);
            TopologyManager.removeGlobalUniqueIndex(sqlgGraph, fn);
            if (!preserveData) {
                getVertexLabel(index.getName()).ifPresent(
                        (VertexLabel vl) -> vl.remove(false));
            }
            getTopology().fire(index, "", TopologyChangeAction.DELETE);
        }
    }

    /**
     * delete schema in DB
     */
    void delete() {
        StringBuilder sql = new StringBuilder();
        sql.append("DROP SCHEMA ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        if (this.sqlgGraph.getSqlDialect().supportsCascade()) {
            sql.append(" CASCADE");
        }
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            logger.error("schema deletion failed " + this.sqlgGraph.toString(), e);
            throw new RuntimeException(e);
        }
    }

    public void createTempTable(String tableName, Map<String, PropertyType> columns) {
        this.sqlgGraph.getSqlDialect().assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlgGraph.getSqlDialect().createTemporaryTableStatement());
        if (this.sqlgGraph.getSqlDialect().needsTemporaryTablePrefix()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                    this.sqlgGraph.getSqlDialect().temporaryTablePrefix() +
                            tableName));
        } else {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(tableName));
        }
        sql.append("(");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlgGraph.getSqlDialect().getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        AbstractLabel.buildColumns(this.sqlgGraph, columns, sql);
        sql.append(") ");
        sql.append(this.sqlgGraph.getSqlDialect().afterCreateTemporaryTableStatement());
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void removeTemporaryTables() {
        if (!this.sqlgGraph.getSqlDialect().supportsTemporaryTableOnCommitDrop()) {
            for (Map.Entry<String, Map<String, PropertyType>> temporaryTableEntry : this.threadLocalTemporaryTables.get().entrySet()) {
                String tableName = temporaryTableEntry.getKey();
                Connection conn = this.sqlgGraph.tx().getConnection();
                try (Statement stmt = conn.createStatement()) {
                    String sql = "DROP TEMPORARY TABLE " +
                            this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                            this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(tableName);
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql);
                    }
                    stmt.execute(sql);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        this.threadLocalTemporaryTables.remove();
    }

    public Map<String, PropertyType> getTemporaryTable(String tableName) {
        return this.threadLocalTemporaryTables.get().get(tableName);
    }
}
