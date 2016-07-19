package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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
import org.umlg.sqlg.strategy.SqlgVertexStepStrategy;
import org.umlg.sqlg.strategy.TopologyStrategy;
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
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ExplainTest$Traversals",
        method = "g_V_outE_identity_inV_explain",
        reason = "Assertions assume that the strategies are in a particular order.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest$Traversals",
        method = "g_V_hasId_compilationEquality",
        reason = "Assertions are TinkerGraph specific.")

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "modern_V_out_out_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "grateful_V_out_out_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "g_V_repeat_both_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "grateful_V_out_out_profile",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "g_V_repeat_both_profile",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "modern_V_out_out_profile",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "testProfileStrategyCallback",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "testProfileStrategyCallbackSideEffect",
        reason = "Assertions are TinkerGraph specific.")

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyProfileTest$Traversals",
        method = "modern_V_out_out_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyProfileTest$Traversals",
        method = "grateful_V_out_out_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyProfileTest$Traversals",
        method = "g_V_repeat_both_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyProfileTest$Traversals",
        method = "grateful_V_out_out_profile",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyProfileTest$Traversals",
        method = "g_V_repeat_both_profile",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyProfileTest$Traversals",
        method = "modern_V_out_out_profile",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyProfileTest$Traversals",
        method = "testProfileStrategyCallback",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyProfileTest$Traversals",
        method = "testProfileStrategyCallbackSideEffect",
        reason = "Assertions are TinkerGraph specific.")

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONTest",
        method = "shouldSerializeTraversalMetrics",
        reason = "Assertions are TinkerGraph specific.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Takes too long, and too much memory at present.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_both_both_count",
        reason = "Travis times out.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count",
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
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GroovyCountTest$Traversals",
        method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count",
        reason = "Takes too long")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest$Traversals",
        method = "g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX",
        reason = "Takes too long")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutorPerformanceTest",
        method = "executorEval",
        reason = "Takes too long")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
        method = "shouldHaveStandardStringRepresentation",
        reason = "SQLGGRAPH INCLUDES THE JDBC CONNECTION URL.")

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
        method = "shouldHaveStandardStringRepresentation",
        reason = "SQLGGRAPH INCLUDES THE JDBC CONNECTION URL.")
public class SqlgGraph implements Graph {

    public static final String JDBC_URL = "jdbc.url";
    public static final String MODE_FOR_STREAM_VERTEX = " mode for streamVertex";
    public static final String TRANSACTION_MUST_BE_IN = "Transaction must be in ";
    private final SqlgDataSource sqlgDataSource;
    private Logger logger = LoggerFactory.getLogger(SqlgGraph.class.getName());
    private final SqlgTransaction sqlgTransaction;
    private SchemaManager schemaManager;
    private GremlinParser gremlinParser;
    private SqlDialect sqlDialect;
    private String jdbcUrl;
    private ObjectMapper mapper = new ObjectMapper();
    private boolean implementForeignKeys;
    private Configuration configuration = new BaseConfiguration();
    private final ISqlGFeatures features = new SqlGFeatures();

    static {
        TraversalStrategies.GlobalCache.registerStrategies(Graph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class)
                .addStrategies(new SqlgVertexStepStrategy())
                .addStrategies(new SqlgGraphStepStrategy())
                .addStrategies(TopologyStrategy.build().create()));
    }

    public static <G extends Graph> G open(final Configuration configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        if (!configuration.containsKey(JDBC_URL))
            throw new IllegalArgumentException(String.format("SqlgGraph configuration requires that the %s be set", JDBC_URL));

        SqlgGraph sqlgGraph = new SqlgGraph(configuration);
        sqlgGraph.schemaManager.loadSchema();
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
            Class<?> sqlDialectClass = findSqlgDialect();
            logger.debug(String.format("Initializing Sqlg with %s dialect", sqlDialectClass.getSimpleName()));
            Constructor<?> constructor = sqlDialectClass.getConstructor(Configuration.class);
            this.sqlDialect = (SqlDialect) constructor.newInstance(configuration);
            this.implementForeignKeys = configuration.getBoolean("implement.foreign.keys", true);
            this.configuration = configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            this.jdbcUrl = configuration.getString(JDBC_URL);
            if (jdbcUrl.startsWith(SqlgDataSource.JNDI_PREFIX)) {
                this.sqlgDataSource = SqlgDataSource
                        .setupDataSourceFromJndi(jdbcUrl.substring(SqlgDataSource.JNDI_PREFIX.length()), this.configuration);
            } else {
                this.sqlgDataSource = SqlgDataSource.setupDataSource(
                        sqlDialect.getJdbcDriver(),
                        this.configuration);
            }

            logger.info(String.format("Connection url = %s , maxPoolSize = %d ", this.configuration.getString(JDBC_URL), configuration.getInt("maxPoolSize", 100)));
            this.sqlDialect.prepareDB(this.sqlgDataSource.get(configuration.getString(JDBC_URL)).getConnection());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.sqlgTransaction = new SqlgTransaction(this, this.configuration.getBoolean("cache.vertices", false));
        this.tx().readWrite();
        this.schemaManager = new SchemaManager(this, sqlDialect, configuration);
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
    public GraphTraversalSource traversal() {
        return this.traversal(SqlgGraphTraversalSource.class);
    }

    public GraphTraversalSource topology() {
        return this.traversal().withStrategies(TopologyStrategy.build().selectFrom(SchemaManager.SQLG_SCHEMA_SCHEMA_TABLES).create());
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
        if (this.tx().isInStreamingBatchMode()) {
            throw SqlgExceptions.invalidMode("Transaction is in " + this.tx().getBatchModeType().toString() + ", use streamVertex(Object ... keyValues)");
        }
        if (this.tx().isInStreamingWithLockBatchMode()) {
            return internalStreamVertex(keyValues);
        } else {
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
    }

    public void streamVertex(String label) {
        this.streamVertex(label, new LinkedHashMap<>());
    }

    public void streamVertex(Object... keyValues) {
        if (!this.tx().isInStreamingBatchMode()) {
            throw SqlgExceptions.invalidMode(TRANSACTION_MUST_BE_IN + this.tx().getBatchModeType().toString() + MODE_FOR_STREAM_VERTEX);
        }
        internalStreamVertex(keyValues);
    }

    public void streamVertex(String label, LinkedHashMap<String, Object> keyValues) {
        if (!this.tx().isInStreamingBatchMode()) {
            throw SqlgExceptions.invalidMode(TRANSACTION_MUST_BE_IN + this.tx().getBatchModeType().toString() + MODE_FOR_STREAM_VERTEX);
        }
        Map<Object, Object> tmp = new LinkedHashMap<>(keyValues);
        tmp.put(T.label, label);
        Object[] keyValues1 = SqlgUtil.mapTokeyValues(tmp);
        streamVertex(keyValues1);
    }

    public void streamTemporaryVertex(String label, LinkedHashMap<String, Object> keyValues) {
        if (!this.tx().isInStreamingBatchMode()) {
            throw SqlgExceptions.invalidMode(TRANSACTION_MUST_BE_IN + this.tx().getBatchModeType().toString() + MODE_FOR_STREAM_VERTEX);
        }
        Map<Object, Object> tmp = new LinkedHashMap<>(keyValues);
        tmp.put(T.label, label);
        Object[] keyValues1 = SqlgUtil.mapTokeyValues(tmp);
        streamTemporaryVertex(keyValues1);
    }

    public void streamTemporaryVertex(Object... keyValues) {
        if (!this.tx().isInStreamingBatchMode()) {
            throw SqlgExceptions.invalidMode(TRANSACTION_MUST_BE_IN + this.tx().getBatchModeType().toString() + MODE_FOR_STREAM_VERTEX);
        }
        internalStreamTemporaryVertex(keyValues);
    }

    private SqlgVertex internalStreamTemporaryVertex(Object... keyValues) {
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        SchemaTable schemaTablePair = SchemaTable.from(this, label, this.getSqlDialect().getPublicSchema());

        SchemaTable streamingBatchModeVertexSchemaTable = this.tx().getBatchManager().getStreamingBatchModeVertexSchemaTable();
        if (streamingBatchModeVertexSchemaTable != null && !streamingBatchModeVertexSchemaTable.toString().equals(schemaTablePair.toString())) {
            throw new IllegalStateException("Streaming batch mode must occur for one label at a time. Expected \"" + streamingBatchModeVertexSchemaTable + "\" found \"" + label + "\". First commit the transaction or call SqlgGraph.flush() before streaming a different label");
        }
        List<String> keys = this.tx().getBatchManager().getStreamingBatchModeVertexKeys();
        validateVertexKeysValues(keyValues, keys);

        this.tx().readWrite();
        this.schemaManager.ensureVertexTemporaryTableExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
        return new SqlgVertex(this, schemaTablePair.getTable(), keyValues);
    }

    private SqlgVertex internalStreamVertex(Object... keyValues) {
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        SchemaTable schemaTablePair = SchemaTable.from(this, label, this.getSqlDialect().getPublicSchema());

        SchemaTable streamingBatchModeVertexSchemaTable = this.tx().getBatchManager().getStreamingBatchModeVertexSchemaTable();
        if (streamingBatchModeVertexSchemaTable != null && !streamingBatchModeVertexSchemaTable.toString().equals(schemaTablePair.toString())) {
            throw new IllegalStateException("Streaming batch mode must occur for one label at a time. Expected \"" + streamingBatchModeVertexSchemaTable + "\" found \"" + label + "\". First commit the transaction or call SqlgGraph.flush() before streaming a different label");
        }
        List<String> keys = this.tx().getBatchManager().getStreamingBatchModeVertexKeys();
        validateVertexKeysValues(keyValues, keys);

        this.tx().readWrite();
        this.schemaManager.ensureVertexTableExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
        return new SqlgVertex(this, true, schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValues);
    }

    public void bulkAddEdges(String inVertexLabel, String outVertexLabel, String edgeLabel, Pair<String, String> idFields, List<? extends Pair<String, String>> uids) {
        if (!this.tx().isInStreamingBatchMode() && !this.tx().isInStreamingWithLockBatchMode()) {
            throw SqlgExceptions.invalidMode(TRANSACTION_MUST_BE_IN + BatchManager.BatchModeType.STREAMING + " or " + BatchManager.BatchModeType.STREAMING_WITH_LOCK + " mode for bulkAddEdges");
        }
        SchemaTable inSchemaTable = SchemaTable.from(this, inVertexLabel, this.sqlDialect.getPublicSchema());
        SchemaTable outSchemaTable = SchemaTable.from(this, outVertexLabel, this.sqlDialect.getPublicSchema());
        this.sqlDialect.bulkAddEdges(this, inSchemaTable, outSchemaTable, edgeLabel, idFields, uids);
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

    private void validateVertexKeysValues(Object[] keyValues, List<String> previousBatchModeKeys) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        int i = 0;
        int keyCount = 0;
        Object key = null;
        Object value;

        for (Object keyValue : keyValues) {
            if (i++ % 2 == 0) {
                key = keyValue;
                if (!key.equals(T.label) && previousBatchModeKeys != null && !previousBatchModeKeys.isEmpty() && !key.equals(previousBatchModeKeys.get(keyCount++))) {
                    throw new IllegalStateException("Streaming batch mode must occur for the same keys in the same order. Expected " + previousBatchModeKeys.get(keyCount - 1) + " found " + key);
                }
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
        if (this.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        return createElementIterator(Vertex.class, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        this.tx().readWrite();
        if (this.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
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
        this.sqlgDataSource.close(this.getJdbcUrl());
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).registry(new SqlgIoRegistry()).create();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "SqlGraph") + " (" + configuration.getProperty(JDBC_URL) + ")";
    }

    public ISqlGFeatures features() {
        return this.features;
    }

    public <T> T gis() {
        return this.getSqlDialect().getGis(this);
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

            @Override
            @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
            public boolean supportsMultiProperties() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
            public boolean supportsMetaProperties() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            public boolean supportsNumericIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public boolean supportsStringIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public boolean supportsCustomIds() {
                return false;
            }

            @Override
            public VertexPropertyFeatures properties() {
                return new SqlGVertexPropertyFeatures();
            }

            @Override
            public VertexProperty.Cardinality getCardinality(final String key) {
                return VertexProperty.Cardinality.single;
            }
        }

        public class SqlEdgeFeatures implements EdgeFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            public boolean supportsNumericIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public boolean supportsStringIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public boolean supportsCustomIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            public EdgePropertyFeatures properties() {
                return new SqlEdgePropertyFeatures();
            }

        }

        public class SqlGVertexPropertyFeatures implements VertexPropertyFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_ADD_PROPERTY)
            public boolean supportsAddProperty() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
            public boolean supportsRemoveProperty() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            public boolean supportsNumericIds() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public boolean supportsStringIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public boolean supportsCustomIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public boolean supportsByteValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
            }


        }

        public class SqlEdgePropertyFeatures implements EdgePropertyFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public boolean supportsUniformListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public boolean supportsByteValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsByteArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsLongArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return SqlgGraph.this.getSchemaManager().getSqlDialect().supportsStringArrayValues();
            }
        }

        public class SqlVariableFeatures implements VariableFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
            public boolean supportsDoubleValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public boolean supportsIntegerValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_VALUES)
            public boolean supportsLongValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_VALUES)
            public boolean supportsStringValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public boolean supportsUniformListValues() {
                return false;
            }
        }

    }

    /**
     * This is executes a sql query and returns the result as a json string.
     *
     * @param query The sql to executeRegularQuery.
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
        throw new IllegalStateException("Not yet implemented!");
//        this.tx().readWrite();
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
        Set<String> tables = this.getSchemaManager().getAllTables().keySet();
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

    private Class<?> findSqlgDialect() {
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

    public SqlgDataSource getSqlgDataSource() {
        return sqlgDataSource;
    }

    public void drop() {
        Connection conn = this.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = "%";
            String[] types = {"TABLE"};
            ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
            while (result.next()) {
                String schema = result.getString(2);
                String table = result.getString(3);
                if (sqlDialect.getGisSchemas().contains(schema) || sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }
                StringBuilder sql = new StringBuilder("DROP TABLE ");
                sql.append(sqlDialect.maybeWrapInQoutes(schema));
                sql.append(".");
                sql.append(sqlDialect.maybeWrapInQoutes(table));
                sql.append(" CASCADE");
                if (sqlDialect.needsSemicolon()) {
                    sql.append(";");
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                    preparedStatement.executeUpdate();
                }
            }
            catalog = null;
            schemaPattern = null;
            result = metadata.getSchemas(catalog, schemaPattern);
            while (result.next()) {
                String schema = result.getString(1);
                if (!sqlDialect.getDefaultSchemas().contains(schema) && !sqlDialect.getGisSchemas().contains(schema)) {
                    StringBuilder sql = new StringBuilder("DROP SCHEMA ");
                    sql.append(sqlDialect.maybeWrapInQoutes(schema));
                    sql.append(" CASCADE");
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
