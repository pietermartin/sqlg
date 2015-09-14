package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.sql.parse.GremlinParser;
import org.umlg.sqlg.strategy.SqlgGraphStepStrategy;
import org.umlg.sqlg.process.SqlgTraverserGeneratorFactory;
import org.umlg.sqlg.strategy.SqlgVertexStepStrategy;
import org.umlg.sqlg.util.SqlgUtil;

import java.lang.reflect.Constructor;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:38 AM
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileTest$Traversals",
        method = "g_V_out_out_profile_modern",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileTest$Traversals",
        method = "g_V_out_out_profile_grateful",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileTest$Traversals",
        method = "testProfileStrategyCallback",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyProfileTest$Traversals",
        method = "g_V_out_out_profile_modern",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyProfileTest$Traversals",
        method = "g_V_out_out_profile_grateful",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroovyProfileTest$Traversals",
        method = "testProfileStrategyCallback",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileTest",
        method = "g_V_repeat_both_modern_profile",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONTest",
        method = "shouldSerializeTraversalMetrics",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Takes too long")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Takes too long")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCountTest$Traversals",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Takes too long")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCountTest$Traversals",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Takes too long")
public class SqlgGraph implements Graph {

    private Logger logger = LoggerFactory.getLogger(SqlgGraph.class.getName());
    private final SqlgTransaction sqlgTransaction;
    private SchemaManager schemaManager;
    private GremlinParser gremlinParser;
    private SqlDialect sqlDialect;
    private String jdbcUrl;
    private ObjectMapper mapper = new ObjectMapper();
    private boolean implementForeignKeys;
    private Configuration configuration = new BaseConfiguration();

    public static <G extends Graph> G open(final Configuration configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        if (!configuration.containsKey("jdbc.url"))
            throw new IllegalArgumentException(String.format("SqlgGraph configuration requires that the %s be set", "jdbc.url"));

        SqlgGraph sqlgGraph = new SqlgGraph(configuration);
        //The removals are here for unit test, as its stored statically
        TraversalStrategies.GlobalCache.getStrategies(Graph.class).removeStrategies(SqlgVertexStepStrategy.class);
        TraversalStrategies.GlobalCache.getStrategies(Graph.class).removeStrategies(SqlgGraphStepStrategy.class);
        TraversalStrategies.GlobalCache.registerStrategies(Graph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(new SqlgVertexStepStrategy(sqlgGraph)));
        TraversalStrategies.GlobalCache.registerStrategies(Graph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(new SqlgGraphStepStrategy(sqlgGraph)));

        TraversalStrategies.GlobalCache.getStrategies(Graph.class).setTraverserGeneratorFactory(new SqlgTraverserGeneratorFactory());

        return (G) sqlgGraph;
    }

    public static <G extends Graph> G open(final String pathToSqlgProperties) {
        if (null == pathToSqlgProperties) throw Graph.Exceptions.argumentCanNotBeNull("pathToSqlgProperties");

        Configuration configuration;
        try {
            configuration = new PropertiesConfiguration(pathToSqlgProperties);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        return open(configuration);
    }

    private SqlgGraph(final Configuration configuration) {
        try {
            Class<?> sqlDialectClass = findSqlGDialect();
            logger.debug(String.format("Initializing Sqlg with %s dialect", sqlDialectClass.getSimpleName()));
            Constructor<?> constructor = sqlDialectClass.getConstructor(Configuration.class);
            this.sqlDialect = (SqlDialect) constructor.newInstance(configuration);
            this.implementForeignKeys = configuration.getBoolean("implement.foreign.keys", true);
            this.configuration = configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            this.jdbcUrl = configuration.getString("jdbc.url");
            SqlgDataSource.INSTANCE.setupDataSource(
                    sqlDialect.getJdbcDriver(),
                    configuration);
            this.sqlDialect.prepareDB(SqlgDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.sqlgTransaction = new SqlgTransaction(this);
        this.tx().readWrite();
        this.schemaManager = new SchemaManager(this, sqlDialect, configuration);
        this.schemaManager.loadSchema();
        this.gremlinParser = new GremlinParser(this);
        if (!this.sqlDialect.supportSchemas() && !this.schemaManager.schemaExist(this.sqlDialect.getPublicSchema())) {
            //This is for mariadb. Need to make sure a db called public exist
            this.schemaManager.createSchema(this.sqlDialect.getPublicSchema());
        }
        this.tx().commit();
    }

    Configuration getConfiguration() {
        return configuration;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public GremlinParser getGremlinParser() {
        return gremlinParser;
    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    public Vertex addVertex(String label, Map<String, Object> keyValues) {
        Map<Object, Object> tmp = new HashMap<>(keyValues);
        tmp.put(T.label, label);
        return addVertex(SqlgUtil.mapTokeyValues(tmp));
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        validateVertexKeysValues(keyValues);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        SchemaTable schemaTablePair = SchemaTable.from(this, label, this.getSqlDialect().getPublicSchema());
        this.tx().readWrite();
        this.schemaManager.ensureVertexTableExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
        return new SqlgVertex(this, false, schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
    }

    /**
     * Only to be called when batchMode is on
     *
     * @param label
     * @param keyValues
     * @return
     */
    public Vertex addCompleteVertex(String label, Map<String, Object> keyValues) {
        if (!this.tx().isInBatchMode()) {
            throw new IllegalStateException("Transaction must be in batch mode for addCompleteVertex");
        }
        if (!this.tx().isInBatchModeComplete()) {
            throw new IllegalStateException("Transaction must be in COMPLETE batch mode for addCompleteVertex");
        }
        Map<Object, Object> tmp = new HashMap<>(keyValues);
        tmp.put(T.label, label);
        return addCompleteVertex(SqlgUtil.mapTokeyValues(tmp));
    }

    private Vertex addCompleteVertex(Object... keyValues) {
        if (!this.tx().isInBatchMode()) {
            throw new IllegalStateException("Transaction must be in batch mode for addCompleteVertex");
        }
        if (!this.tx().isInBatchModeComplete()) {
            throw new IllegalStateException("Transaction must be in COMPLETE batch mode for addCompleteVertex");
        }
        validateVertexKeysValues(keyValues);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        SchemaTable schemaTablePair = SchemaTable.from(this, label, this.getSqlDialect().getPublicSchema());
        this.tx().readWrite();
        this.schemaManager.ensureVertexTableExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
        return new SqlgVertex(this, true, schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
    }

    private void validateVertexKeysValues(Object[] keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        int i = 0;
        Object key = null;
        Object value;
        for (Object keyValue : keyValues) {
            if (i++ % 2 == 0) {
                key = keyValue;
            } else {
                value = keyValue;
                if (!key.equals(T.label)) {
                    ElementHelper.validateProperty((String) key, value);
                    this.sqlDialect.validateProperty(key, value);
                }

            }
        }
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        this.tx().readWrite();
        return createElementIterator(Vertex.class, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        this.tx().readWrite();
        return createElementIterator(Edge.class, edgeIds);
    }

    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz, final Object... ids) {
        if (0 == ids.length) {
            return (Iterator<T>) elements(Vertex.class.isAssignableFrom(clazz), Collections.EMPTY_LIST).iterator();
        } else {
            if (clazz.isAssignableFrom(ids[0].getClass())) {
                // based on the first item assume all vertices in the argument list
                if (!Stream.of(ids).allMatch(id -> clazz.isAssignableFrom(id.getClass())))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();

                return Stream.of(ids).map(id -> (T) id).iterator();
            } else {
                final Class<?> firstClass = ids[0].getClass();
                if (!Stream.of(ids).map(Object::getClass).allMatch(firstClass::equals))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();

                List<RecordId> recordIds = RecordId.from(ids);
                Iterable<T> elementIterable = elements(Vertex.class.isAssignableFrom(clazz), recordIds);
                return elementIterable.iterator();
            }
        }
    }

    public Vertex v(final Object id) {
        Iterator<Vertex> t = this.vertices(id);
        return t.hasNext() ? t.next() : null;
    }

    public Edge e(final Object id) {
        Iterator<Edge> t = this.edges(id);
        return t.hasNext() ? t.next() : null;
    }

    @Override
    public SqlgTransaction tx() {
        return this.sqlgTransaction;
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public void close() throws Exception {
        if (this.tx().isOpen())
            this.tx().close();
        this.schemaManager.close();
        SqlgDataSource.INSTANCE.close(this.getJdbcUrl());
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).registry(new SqlgIoRegistry()).create();
    }

    public String toString() {
        return StringFactory.graphString(this, "SqlGraph");
    }

    public ISqlGFeatures features() {
        return new SqlGFeatures();
    }


    public interface ISqlGFeatures extends Features {
        boolean supportsBatchMode();
    }

    public class SqlGFeatures implements ISqlGFeatures {
        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsComputer() {
                    return false;
                }

                @Override
                public VariableFeatures variables() {
                    return new SqlVariableFeatures();
                }

                @Override
                public boolean supportsThreadedTransactions() {
                    return false;
                }

            };
        }

        @Override
        public VertexFeatures vertex() {
            return new SqlVertexFeatures();
        }

        @Override
        public EdgeFeatures edge() {
            return new SqlEdgeFeatures();
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

        @Override
        public boolean supportsBatchMode() {
            return getSqlDialect().supportsBatchMode();
        }

        public class SqlVertexFeatures implements VertexFeatures {

            /**
             * Determines if a {@link Vertex} can support multiple properties with the same key.
             */
            @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
            public boolean supportsMultiProperties() {
                return false;
            }

            /**
             * Determines if a {@link Vertex} can support properties on vertex properties.  It is assumed that a
             * graph will support all the same data types for meta-properties that are supported for regular
             * properties.
             */
            @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
            public boolean supportsMetaProperties() {
                return false;
            }

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public VertexPropertyFeatures properties() {
                return new SqlGVertexPropertyFeatures();
            }

            @Override
            public boolean supportsNumericIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            public VertexProperty.Cardinality getCardinality(final String key) {
                return VertexProperty.Cardinality.single;
            }
        }

        public class SqlEdgeFeatures implements EdgeFeatures {
            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public EdgePropertyFeatures properties() {
                return new SqlEdgePropertyFeatures();
            }

            @Override
            public boolean supportsNumericIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }
        }

        public class SqlGVertexPropertyFeatures implements VertexPropertyFeatures {

            @Override
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteValues();
            }

            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            public boolean supportsByteArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            public boolean supportsLongArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            public boolean supportsStringArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
            }


        }

        public class SqlEdgePropertyFeatures implements EdgePropertyFeatures {
            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            public boolean supportsByteArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            public boolean supportsLongArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            public boolean supportsStringArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
            }
        }

        public class SqlVariableFeatures implements VariableFeatures {
            @Override
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleValues() {
                return false;
            }

            @Override
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerValues() {
                return false;
            }

            @Override
            public boolean supportsLongValues() {
                return false;
            }

            @Override
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            public boolean supportsStringValues() {
                return false;
            }

            @Override
            public boolean supportsUniformListValues() {
                return false;
            }
        }

    }

    /**
     * This is executes a sql query and returns the result as a json string.
     *
     * @param query The sql to execute.
     * @return The query result as json.
     */
    public String query(String query) {
        try {
            Connection conn = this.tx().getConnection();
            ObjectNode result = this.mapper.createObjectNode();
            ArrayNode dataNode = this.mapper.createArrayNode();
            ArrayNode metaNode = this.mapper.createArrayNode();
            Statement statement = conn.createStatement();
            if (logger.isDebugEnabled()) {
                logger.debug(query);
            }
            ResultSet rs = statement.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            boolean first = true;
            while (rs.next()) {
                int numColumns = rsmd.getColumnCount();
                ObjectNode obj = this.mapper.createObjectNode();
                for (int i = 1; i < numColumns + 1; i++) {
                    String columnName = rsmd.getColumnLabel(i);
                    Object o = rs.getObject(columnName);
                    int type = rsmd.getColumnType(i);
                    this.sqlDialect.putJsonObject(obj, columnName, type, o);
                    if (first) {
                        this.sqlDialect.putJsonMetaObject(this.mapper, metaNode, columnName, type, o);
                    }
                }
                first = false;
                dataNode.add(obj);
            }
            result.put("data", dataNode);
            result.put("meta", metaNode);
            return result.toString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            this.tx().rollback();
        }
    }

    //indexing
    public void createUniqueConstraint(String label, String propertyKey) {
        this.tx().readWrite();
//        this.getSchemaManager().createUniqueConstraint(label, propertyKey);
    }

    public void createVertexLabeledIndex(String label, Object... dummykeyValues) {
        int i = 0;
        String key = "";
        Object value;
        for (Object keyValue : dummykeyValues) {
            if (i++ % 2 == 0) {
                key = (String) keyValue;
            } else {
                value = keyValue;
                if (!key.equals(T.label)) {
                    ElementHelper.validateProperty(key, value);
                    this.sqlDialect.validateProperty(key, value);
                }

            }
        }
        this.tx().readWrite();
        SchemaTable schemaTablePair = SchemaTable.from(this, label, this.getSqlDialect().getPublicSchema());
        this.getSchemaManager().createVertexIndex(schemaTablePair, dummykeyValues);
    }

    public void createEdgeLabeledIndex(String label, Object... dummykeyValues) {
        int i = 0;
        String key = "";
        Object value;
        for (Object keyValue : dummykeyValues) {
            if (i++ % 2 == 0) {
                key = (String) keyValue;
            } else {
                value = keyValue;
                if (!key.equals(T.label)) {
                    ElementHelper.validateProperty(key, value);
                    this.sqlDialect.validateProperty(key, value);
                }

            }
        }
        this.tx().readWrite();
        SchemaTable schemaTablePair = SchemaTable.from(this, label, this.getSqlDialect().getPublicSchema());
        this.getSchemaManager().createEdgeIndex(schemaTablePair, dummykeyValues);
    }

    public long countVertices() {
        this.tx().readWrite();
        return countElements(true);
    }

    public long countEdges() {
        this.tx().readWrite();
        return countElements(false);
    }

    private long countElements(boolean returnVertices) {
        long count = 0;
        Set<String> tables = this.getSchemaManager().getLocalTables().keySet();
        for (String table : tables) {
            SchemaTable schemaTable = SchemaTable.from(this, table, this.getSqlDialect().getPublicSchema());
            if (returnVertices ? schemaTable.isVertexTable() : !schemaTable.isVertexTable()) {
                StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM ");
                sql.append("\"");
                sql.append(schemaTable.getSchema());
                sql.append("\".\"");
                sql.append(schemaTable.getTable());
                sql.append("\"");
                if (this.getSqlDialect().needsSemicolon()) {
                    sql.append(";");
                }
                Connection conn = this.tx().getConnection();
                if (logger.isDebugEnabled()) {
                    logger.debug(sql.toString());
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                    ResultSet rs = preparedStatement.executeQuery();
                    rs.next();
                    count += rs.getLong(1);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return count;
    }

    boolean isImplementForeignKeys() {
        return implementForeignKeys;
    }

    private Class<?> findSqlGDialect() {
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.PostgresDialect");
        } catch (ClassNotFoundException e) {
        }
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.MariaDbDialect");
        } catch (ClassNotFoundException e) {
        }
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.HsqldbDialect");
        } catch (ClassNotFoundException e) {
        }
        throw new IllegalStateException("No sqlg dialect found!");
    }

    private <T extends Element> Iterable<T> elements(boolean returnVertices, final List<RecordId> elementIds) {
        List<T> sqlgElements = new ArrayList<>();
        if (elementIds.size() > 0) {
            Map<SchemaTable, List<Long>> distinctTableIdMap = RecordId.normalizeIds(elementIds);
            for (Map.Entry<SchemaTable, List<Long>> schemaTableListEntry : distinctTableIdMap.entrySet()) {
                SchemaTable schemaTable = schemaTableListEntry.getKey();
                String tableName = (returnVertices ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + schemaTable.getTable();
                if (this.getSchemaManager().getAllTables().containsKey(schemaTable.getSchema() + "." + tableName)) {
                    List<Long> schemaTableIds = schemaTableListEntry.getValue();
                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
                    sql.append("\"");
                    sql.append(schemaTable.getSchema());
                    sql.append("\".\"");
                    if (returnVertices) {
                        sql.append(SchemaManager.VERTEX_PREFIX);
                    } else {
                        sql.append(SchemaManager.EDGE_PREFIX);
                    }
                    sql.append(schemaTable.getTable());
                    sql.append("\" WHERE ");
                    sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
                    sql.append(" IN (");
                    int count = 1;
                    for (Long id : schemaTableIds) {
                        sql.append(id.toString());
                        if (count++ < schemaTableIds.size()) {
                            sql.append(",");
                        }
                    }
                    sql.append(")");
                    if (this.getSqlDialect().needsSemicolon()) {
                        sql.append(";");
                    }
                    Connection conn = this.tx().getConnection();
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    try (Statement statement = conn.createStatement()) {
                        statement.execute(sql.toString());
                        ResultSet resultSet = statement.getResultSet();
                        while (resultSet.next()) {
                            long id = resultSet.getLong("ID");
                            SqlgElement sqlgElement;
                            if (returnVertices) {
                                sqlgElement = SqlgVertex.of(this, id, schemaTable.getSchema(), schemaTable.getTable());
                            } else {
                                sqlgElement = new SqlgEdge(this, id, schemaTable.getSchema(), schemaTable.getTable());
                            }
                            sqlgElement.loadResultSet(resultSet);
                            sqlgElements.add((T) sqlgElement);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } else {
            //TODO use a union query
            Set<String> tables = this.getSchemaManager().getAllTables().keySet();
            for (String table : tables) {
                SchemaTable schemaTable = SchemaTable.from(this, table, this.getSqlDialect().getPublicSchema());
                if (returnVertices ? schemaTable.isVertexTable() : !schemaTable.isVertexTable()) {
                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
                    sql.append("\"");
                    sql.append(schemaTable.getSchema());
                    sql.append("\".\"");
                    sql.append(schemaTable.getTable());
                    sql.append("\"");
                    if (this.getSqlDialect().needsSemicolon()) {
                        sql.append(";");
                    }
                    Connection conn = this.tx().getConnection();
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    try (Statement statement = conn.createStatement()) {
                        statement.execute(sql.toString());
                        ResultSet resultSet = statement.getResultSet();
                        while (resultSet.next()) {
                            long id = resultSet.getLong("ID");
                            SqlgElement sqlgElement;
                            if (returnVertices) {
                                sqlgElement = SqlgVertex.of(this, id, schemaTable.getSchema(), schemaTable.getTable().substring(SchemaManager.VERTEX_PREFIX.length()));
                            } else {
                                sqlgElement = new SqlgEdge(this, id, schemaTable.getSchema(), schemaTable.getTable().substring(SchemaManager.EDGE_PREFIX.length()));
                            }
                            sqlgElement.loadResultSet(resultSet);
                            sqlgElements.add((T) sqlgElement);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return sqlgElements;
    }

}
