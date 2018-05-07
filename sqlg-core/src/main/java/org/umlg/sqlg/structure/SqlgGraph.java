package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.sql.dialect.SqlBulkDialect;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.sql.parse.GremlinParser;
import org.umlg.sqlg.strategy.*;
import org.umlg.sqlg.strategy.barrier.*;
import org.umlg.sqlg.structure.SqlgDataSourceFactory.SqlgDataSource;
import org.umlg.sqlg.structure.ds.C3p0DataSourceFactory;
import org.umlg.sqlg.structure.ds.JNDIDataSource;
import org.umlg.sqlg.structure.topology.IndexType;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.structure.Graph.OptIn;
import static org.apache.tinkerpop.gremlin.structure.Graph.OptOut;

/**
 * Date: 2014/07/12
 * Time: 5:38 AM
 */
@OptIn(OptIn.SUITE_STRUCTURE_STANDARD)
@OptIn(OptIn.SUITE_PROCESS_STANDARD)

//Start remove these for 3.2.6
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "shouldDetachVertexPropertyWhenRemoved",
        reason = "Tests assumes elements are auto synchronized.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "shouldDetachPropertyOfEdgeWhenNew",
        reason = "Tests assumes elements are auto synchronized.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "shouldDetachPropertyOfEdgeWhenRemoved",
        reason = "Tests assumes elements are auto synchronized.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "shouldDetachVertexPropertyWhenNew",
        reason = "Tests assumes elements are auto synchronized.")
//End remove these for 3.2.6

@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest",
        method = "*",
        reason = "Fails for HSQLDB. HSQLDB has its own interrupt logic that does not play well with TinkerPop's interrupt.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionTest",
        method = "shouldRollbackElementAutoTransactionByDefault",
        reason = "Fails for HSQLDB as HSQLDB commits the transaction on schema creation and buggers the rollback test logic.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionTest",
        method = "shouldSupportTransactionIsolationCommitCheck",
        reason = "Fails for as the schema creation deadlock because of unnatural locking in the test.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionTest",
        method = "shouldRollbackElementAutoTransactionByDefault",
        reason = "Fails for HSQLDB as HSQLDB commits the transaction on schema creation and buggers the rollback test logic.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionTest",
        method = "shouldAllowReferenceOfEdgeIdOutsideOfOriginalThreadManual",
        reason = "Fails as the test leaves multiple transactions open which causes a dead lock.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionTest",
        method = "shouldAllowReferenceOfVertexIdOutsideOfOriginalThreadManual",
        reason = "Fails as the test leaves multiple transactions open which causes a dead lock.")

@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ExplainTest$Traversals",
        method = "g_V_outE_identity_inV_explain",
        reason = "Assertions assume that the strategies are in a particular order.")

@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "modern_V_out_out_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "grateful_V_out_out_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "g_V_repeat_both_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "grateful_V_out_out_profile",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "g_V_repeat_both_profile",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "modern_V_out_out_profile",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "testProfileStrategyCallback",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "testProfileStrategyCallbackSideEffect",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "g_V_whereXinXcreatedX_count_isX1XX_name_profile",
        reason = "Assertions are TinkerGraph specific.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest$Traversals",
        method = "g_V_whereXinXcreatedX_count_isX1XX_name_profileXmetricsX",
        reason = "Assertions are TinkerGraph specific.")

@OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV1d0Test",
        method = "shouldSerializeTraversalMetrics",
        reason = "Assertions are TinkerGraph specific.")

@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX3X_count",
        reason = "Takes too long, and too much memory at present.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_both_both_count",
        reason = "Travis times out.")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count",
        reason = "Takes too long")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest$Traversals",
        method = "g_V_repeatXoutX_timesX8X_count",
        reason = "Takes too long")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest$Traversals",
        method = "g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX",
        reason = "Takes too long")
@OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
        method = "shouldHaveStandardStringRepresentation",
        reason = "SQLGGRAPH INCLUDES THE JDBC CONNECTION URL.")

@OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.GraphTest",
        method = "shouldHaveStandardStringRepresentation",
        reason = "SQLGGRAPH INCLUDES THE JDBC CONNECTION URL.")
public class SqlgGraph implements Graph {

    public static final String JDBC_URL = "jdbc.url";
    public static final String DISTRIBUTED = "distributed";
    public static final String MODE_FOR_STREAM_VERTEX = " mode for streamVertex";
    public static final String TRANSACTION_MUST_BE_IN = "Transaction must be in ";
    private final SqlgDataSource sqlgDataSource;
    private static Logger logger = LoggerFactory.getLogger(SqlgGraph.class);
    private final SqlgTransaction sqlgTransaction;
    private Topology topology;
    private GremlinParser gremlinParser;
    private SqlDialect sqlDialect;
    private String jdbcUrl;
    private ObjectMapper mapper = new ObjectMapper();
//    private boolean implementForeignKeys;
    private Configuration configuration = new BaseConfiguration();
    private final ISqlGFeatures features = new SqlGFeatures();

    /**
     * the build version of sqlg
     */
    private String buildVersion;
    
    //This has some static suckness
    static {
        TraversalStrategies.GlobalCache.registerStrategies(Graph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class)
                .addStrategies(
                        new SqlgGraphStepStrategy(),
                        new SqlgVertexStepStrategy(),
                        new SqlgLocalStepStrategy(),
                        new SqlgWhereStrategy(),
                        new SqlgRepeatStepStrategy(),
                        new SqlgOptionalStepStrategy(),
                        new SqlgChooseStepStrategy(),
                        new SqlgTraversalFilterStepStrategy<>(),
                        new SqlgWhereTraversalStepStrategy(),
                        new SqlgOrStepStepStrategy(),
                        new SqlgAndStepStepStrategy(),
                        new SqlgNotStepStepStrategy(),
                        new SqlgHasStepStrategy(),
                        new SqlgDropStepStrategy(),
                        new SqlgRestrictPropertiesStrategy(),
                        TopologyStrategy.build().create())
                .removeStrategies(
                        PathRetractionStrategy.class)
        );
    }

    public static <G extends Graph> G open(final Configuration configuration) {
        return open(configuration, createDataSourceFactory(configuration));
    }

    public static <G extends Graph> G open(final Configuration configuration, SqlgDataSourceFactory dataSourceFactory) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        if (!configuration.containsKey(JDBC_URL))
            throw new IllegalArgumentException(String.format("SqlgGraph configuration requires that the %s be set", JDBC_URL));

        SqlgGraph sqlgGraph = new SqlgGraph(configuration, dataSourceFactory);
        SqlgStartupManager sqlgStartupManager = new SqlgStartupManager(sqlgGraph);
        sqlgStartupManager.loadSqlgSchema();
        sqlgGraph.buildVersion=sqlgStartupManager.getBuildVersion();
        return (G) sqlgGraph;
    }

    public static <G extends Graph> G open(final String pathToSqlgProperties) {
        if (null == pathToSqlgProperties) throw Graph.Exceptions.argumentCanNotBeNull("pathToSqlgProperties");

        Configuration configuration;
        try {
            configuration = new PropertiesConfiguration(pathToSqlgProperties);
            return open(configuration, createDataSourceFactory(configuration));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public static SqlgDataSourceFactory createDataSourceFactory(Configuration configuration) {
        try {
            return (SqlgDataSourceFactory) Class.forName(configuration.getString("jdbc.factory", C3p0DataSourceFactory.class.getCanonicalName())).newInstance();
        } catch (Exception ex) {
            throw new IllegalStateException("Could not create sqlg factory", ex);
        }
    }

    private SqlgGraph(final Configuration configuration, SqlgDataSourceFactory dataSourceFactory) {
//        this.implementForeignKeys = configuration.getBoolean("implement.foreign.keys", true);
        this.configuration = configuration;

        try {
            this.jdbcUrl = this.configuration.getString(JDBC_URL);

            if (JNDIDataSource.isJNDIUrl(this.jdbcUrl)) {
                this.sqlgDataSource = JNDIDataSource.create(configuration);

                try (Connection conn = this.getConnection()) {
                    SqlgPlugin p = findSqlgPlugin(conn.getMetaData());
                    if (p == null) {
                        throw new IllegalStateException("Could not find suitable sqlg plugin for the JDBC URL: " + this.jdbcUrl);
                    }
                    this.sqlDialect = p.instantiateDialect();
                }
            } else {
                SqlgPlugin p = findSqlgPlugin(this.jdbcUrl);
                if (p == null) {
                    throw new IllegalStateException("Could not find suitable sqlg plugin for the JDBC URL: " + this.jdbcUrl);
                }
                this.sqlDialect = p.instantiateDialect();
                this.sqlgDataSource = dataSourceFactory.setup(p.getDriverFor(jdbcUrl), this.configuration);
            }

            logger.debug(String.format("Opening graph. Connection url = %s, maxPoolSize = %d", this.getJdbcUrl(), configuration.getInt("maxPoolSize", 100)));
            try (Connection conn = this.getConnection()) {
                //This is used by Hsqldb to set the transaction semantics. MVCC and cache
                this.sqlDialect.prepareDB(conn);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.sqlgTransaction = new SqlgTransaction(this, this.configuration.getBoolean("cache.vertices", false));
        
        // read fetch size from configuration, use default as specified in the dialect
        // this can be very useful for Postgres since according to < https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor>
        // Postgres JDBC will load the whole result in memory
        // so if there are massive queries, setting the fetch size will avoid out of memory errors
        this.sqlgTransaction.setDefaultFetchSize(this.configuration.getInteger("fetch.size", this.sqlDialect.getDefaultFetchSize()));
        
        this.tx().readWrite();
        //Instantiating Topology will create the 'public' schema if it does not exist.
        this.topology = new Topology(this);
        this.gremlinParser = new GremlinParser(this);
        if (!this.sqlDialect.supportsSchemas() && !this.getTopology().getSchema(this.sqlDialect.getPublicSchema()).isPresent()) {
            //This is for mariadb. Need to make sure a db called public exist
            this.getTopology().ensureSchemaExist(this.sqlDialect.getPublicSchema());
        }
        this.tx().commit();
    }

    Configuration getConfiguration() {
        return configuration;
    }


    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public Topology getTopology() {
        return this.topology;
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
        return this.traversal().withStrategies(TopologyStrategy.build().selectFrom(this.getTopology().getSqlgSchemaAbstractLabels()).create());
    }

    public GraphTraversalSource globalUniqueIndexes() {
        return this.traversal().withStrategies(TopologyStrategy.build().selectFrom(this.getTopology().getGlobalUniqueIndexes()).create());
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
            throw SqlgExceptions.invalidMode(String.format("Transaction is in %s, use streamVertex(Object ... keyValues)", this.tx().getBatchModeType().toString()));
        }
        if (this.tx().isInStreamingWithLockBatchMode()) {
            return internalStreamVertex(keyValues);
        } else {
            Triple<Map<String, PropertyType>, Map<String, Object>, Map<String, Object>> keyValueMapTriple = SqlgUtil.validateVertexKeysValues(this.sqlDialect, keyValues);
            final Pair<Map<String, Object>, Map<String, Object>> keyValueMapPair = Pair.of(keyValueMapTriple.getMiddle(), keyValueMapTriple.getRight());
            final Map<String, PropertyType> columns = keyValueMapTriple.getLeft();
            final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
            SchemaTable schemaTablePair = SchemaTable.from(this, label);
            this.tx().readWrite();
            VertexLabel vertexLabel = this.getTopology().ensureVertexLabelExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), columns);
            if (!vertexLabel.hasIDPrimaryKey()) {
                Preconditions.checkArgument(columns.keySet().containsAll(vertexLabel.getIdentifiers()), "identifiers must be present %s", vertexLabel.getIdentifiers());
            }
            return new SqlgVertex(this, false, false, schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValueMapPair);
        }
    }

    public Vertex addTemporaryVertex(Object... keyValues) {
        if (this.tx().isInStreamingBatchMode()) {
            throw SqlgExceptions.invalidMode(String.format("Transaction is in %s, use streamVertex(Object ... keyValues)", this.tx().getBatchModeType().toString()));
        }
        Triple<Map<String, PropertyType>, Map<String, Object>, Map<String, Object>> keyValueMapTriple = SqlgUtil.validateVertexKeysValues(this.sqlDialect, keyValues);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        SchemaTable schemaTablePair = SchemaTable.from(this, label, true);
        final Map<String, PropertyType> columns = keyValueMapTriple.getLeft();
        this.getTopology().ensureTemporaryVertexTableExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), columns);
        final Pair<Map<String, Object>, Map<String, Object>> keyValueMapPair = Pair.of(keyValueMapTriple.getMiddle(), keyValueMapTriple.getRight());
        return new SqlgVertex(this, true, false, schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValueMapPair);
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
        Preconditions.checkState(this.sqlDialect.supportsBatchMode());
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        SchemaTable schemaTablePair = SchemaTable.from(this, label);

        SchemaTable streamingBatchModeVertexSchemaTable = this.tx().getBatchManager().getStreamingBatchModeVertexSchemaTable();
        if (streamingBatchModeVertexSchemaTable != null && !streamingBatchModeVertexSchemaTable.toString().equals(schemaTablePair.toString())) {
            throw new IllegalStateException("Streaming batch mode must occur for one label at a time. Expected \"" + streamingBatchModeVertexSchemaTable + "\" found \"" + label + "\". First commit the transaction or call SqlgGraph.flush() before streaming a different label");
        }
        List<String> keys = this.tx().getBatchManager().getStreamingBatchModeVertexKeys();
        Triple<Map<String, PropertyType>, Map<String, Object>, Map<String, Object>> keyValuesTriple = SqlgUtil.validateVertexKeysValues(this.sqlDialect, keyValues, keys);
        final Map<String, Object> allKeyValueMap = keyValuesTriple.getMiddle();
        final Map<String, PropertyType> columns = keyValuesTriple.getLeft();
        this.tx().readWrite();
        this.getTopology().ensureTemporaryVertexTableExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), columns);
        return new SqlgVertex(this, schemaTablePair.getTable(), allKeyValueMap);
    }

    private SqlgVertex internalStreamVertex(Object... keyValues) {
        Preconditions.checkState(this.sqlDialect.supportsStreamingBatchMode(), "Streaming batch mode is not supported.");
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        SchemaTable schemaTablePair = SchemaTable.from(this, label);

        SchemaTable streamingBatchModeVertexSchemaTable = this.tx().getBatchManager().getStreamingBatchModeVertexSchemaTable();
        if (streamingBatchModeVertexSchemaTable != null && !streamingBatchModeVertexSchemaTable.toString().equals(schemaTablePair.toString())) {
            throw new IllegalStateException("Streaming batch mode must occur for one label at a time. Expected \"" + streamingBatchModeVertexSchemaTable + "\" found \"" + label + "\". First commit the transaction or call SqlgGraph.flush() before streaming a different label");
        }
        List<String> keys = this.tx().getBatchManager().getStreamingBatchModeVertexKeys();
        Triple<Map<String, PropertyType>, Map<String, Object>, Map<String, Object>> keyValueMapTriple = SqlgUtil.validateVertexKeysValues(this.sqlDialect, keyValues, keys);
        final Pair<Map<String, Object>, Map<String, Object>> keyValueMapPair = Pair.of(keyValueMapTriple.getMiddle(), keyValueMapTriple.getRight());
        final Map<String, PropertyType> columns = keyValueMapTriple.getLeft();
        this.tx().readWrite();
        this.getTopology().ensureVertexLabelExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), columns);
        return new SqlgVertex(this, false, true, schemaTablePair.getSchema(), schemaTablePair.getTable(), keyValueMapPair);
    }


    public <L, R> void bulkAddEdges(String outVertexLabel, String inVertexLabel, String edgeLabel, Pair<String, String> idFields, Collection<Pair<L, R>> uids) {
        if (!(this.sqlDialect instanceof SqlBulkDialect)) {
            throw new UnsupportedOperationException(String.format("Bulk mode is not supported for %s", this.sqlDialect.dialectName()));
        }
        SqlBulkDialect sqlBulkDialect = (SqlBulkDialect) this.sqlDialect;
        if (!this.tx().isInStreamingBatchMode() && !this.tx().isInStreamingWithLockBatchMode()) {
            throw SqlgExceptions.invalidMode(TRANSACTION_MUST_BE_IN + BatchManager.BatchModeType.STREAMING + " or " + BatchManager.BatchModeType.STREAMING_WITH_LOCK + " mode for bulkAddEdges");
        }
        if (!uids.isEmpty()) {
            SchemaTable outSchemaTable = SchemaTable.from(this, outVertexLabel);
            SchemaTable inSchemaTable = SchemaTable.from(this, inVertexLabel);
            sqlBulkDialect.bulkAddEdges(this, outSchemaTable, inSchemaTable, edgeLabel, idFields, uids);
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
        if (this.sqlDialect.supportsBatchMode() && this.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        return createElementIterator(Vertex.class, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        this.tx().readWrite();
        if (this.getSqlDialect().supportsBatchMode() && this.tx().getBatchManager().isStreaming()) {
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

                List<RecordId> recordIds = RecordId.from(this, ids);
                Iterable<T> elementIterable = elements(Vertex.class.isAssignableFrom(clazz), recordIds);
                return elementIterable.iterator();
            }
        }
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
    public void close() {
        logger.debug(String.format("Closing graph. Connection url = %s, maxPoolSize = %d", this.configuration.getString(JDBC_URL), configuration.getInt("maxPoolSize", 100)));
        if (this.tx().isOpen())
            this.tx().close();
        this.topology.close();
        this.sqlgDataSource.close();
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        if (builder.requiresVersion(GryoVersion.V1_0) || builder.requiresVersion(GraphSONVersion.V1_0))
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(SqlgIoRegistry.instance())).create();
        else if (builder.requiresVersion(GraphSONVersion.V2_0))   // there is no gryo v2
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(SqlgIoRegistry.instance())).create();
        else
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(SqlgIoRegistryV3.instance())).create();
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
                return SqlgGraph.this.getSqlDialect().supportsByteValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return SqlgGraph.this.getSqlDialect().supportsFloatValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsByteArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsLongArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsStringArrayValues();
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
                return SqlgGraph.this.getSqlDialect().supportsByteValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return SqlgGraph.this.getSqlDialect().supportsFloatValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsBooleanArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsByteArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsDoubleArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsFloatArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsIntegerArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsLongArrayValues();
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return SqlgGraph.this.getSqlDialect().supportsStringArrayValues();
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
                    int type = rsmd.getColumnType(i);
                    //make sure to obtain array using getArray()
                    //At least in H2, this makes a difference...
                    Object o = type == Types.ARRAY ? rs.getArray(i) : rs.getObject(i);
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

    /**
     * @deprecated Please use {@link Topology#ensureVertexLabelExist(String, Map)} and {@link VertexLabel#ensureIndexExists(IndexType, List)}.
     */
    @Deprecated
    public void createVertexLabeledIndex(String label, Object... dummykeyValues) {
        Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(dummykeyValues);
        SchemaTable schemaTablePair = SchemaTable.from(this, label);
        VertexLabel vertexLabel = this.getTopology().ensureVertexLabelExist(schemaTablePair.getSchema(), schemaTablePair.getTable(), columns);
        List<PropertyColumn> properties = new ArrayList<>();
        List<String> keys = SqlgUtil.transformToKeyList(dummykeyValues);
        for (String key : keys) {
            properties.add(vertexLabel.getProperty(key).get());
        }
        vertexLabel.ensureIndexExists(IndexType.NON_UNIQUE, properties);
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
        Set<String> tables = this.getTopology().getAllTables().keySet();
        for (String table : tables) {
            SchemaTable schemaTable = SchemaTable.from(this, table);
            if (returnVertices ? schemaTable.isVertexTable() : !schemaTable.isVertexTable()) {
                StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM ");
                sql.append(getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                sql.append(".");
                sql.append(getSqlDialect().maybeWrapInQoutes(schemaTable.getTable()));
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

//    public boolean isImplementForeignKeys() {
//        return this.implementForeignKeys;
//    }

    private SqlgPlugin findSqlgPlugin(DatabaseMetaData metadata) throws SQLException {
        for (SqlgPlugin p : ServiceLoader.load(SqlgPlugin.class, this.getClass().getClassLoader())) {
            logger.info("found plugin for SqlgPlugin.class");
            if (p.canWorkWith(metadata)) {
                return p;
            } else {
                logger.info("can not work with SqlgPlugin.class");
            }
        }
        return null;
    }

    private SqlgPlugin findSqlgPlugin(String connectionUri) {
        for (SqlgPlugin p : ServiceLoader.load(SqlgPlugin.class, this.getClass().getClassLoader())) {
            if (p.getDriverFor(connectionUri) != null) {
                return p;
            }
        }
        return null;
    }

    private <T extends Element> Iterable<T> elements(boolean returnVertices, final List<RecordId> elementIds) {


        if (returnVertices) {
            return (Iterable<T>) this.traversal().V(elementIds).toList();
        } else {
            return (Iterable<T>) this.traversal().E(elementIds).toList();
        }

//        List<T> sqlgElements = new ArrayList<>();
//        if (elementIds.size() > 0) {
//            Map<SchemaTable, List<RecordId.ID>> distinctTableIdMap = RecordId.normalizeIds(elementIds);
//            for (Map.Entry<SchemaTable, List<RecordId.ID>> schemaTableListEntry : distinctTableIdMap.entrySet()) {
//                SchemaTable schemaTable = schemaTableListEntry.getKey();
//                String tableName = (returnVertices ? VERTEX_PREFIX : EDGE_PREFIX) + schemaTable.getTable();
//                if (this.getTopology().getAllTables().containsKey(schemaTable.getSchema() + "." + tableName)) {
//                    List<RecordId.ID> schemaTableIds = schemaTableListEntry.getValue();
//                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
//                    sql.append(this.sqlDialect.maybeWrapInQoutes(schemaTable.getSchema()));
//                    sql.append(".");
//                    String table = "";
//                    if (returnVertices) {
//                        table += VERTEX_PREFIX;
//                    } else {
//                        table += EDGE_PREFIX;
//                    }
//                    table += schemaTable.getTable();
//                    sql.append(this.sqlDialect.maybeWrapInQoutes(table));
//                    sql.append(" WHERE ");
//                    sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
//                    sql.append(" IN (");
//                    int count = 1;
//                    for (RecordId.ID id : schemaTableIds) {
//                        sql.append(id.toString());
//                        if (count++ < schemaTableIds.size()) {
//                            sql.append(",");
//                        }
//                    }
//                    sql.append(")");
//                    if (this.getSqlDialect().needsSemicolon()) {
//                        sql.append(";");
//                    }
//                    Connection conn = this.tx().getConnection();
//                    if (logger.isDebugEnabled()) {
//                        logger.debug(sql.toString());
//                    }
//                    try (Statement statement = conn.createStatement()) {
//                        statement.execute(sql.toString());
//                        ResultSet resultSet = statement.getResultSet();
//                        while (resultSet.next()) {
//                            long id = resultSet.getLong("ID");
//                            SqlgElement sqlgElement;
//                            if (returnVertices) {
//                                sqlgElement = SqlgVertex.of(this, id, schemaTable.getSchema(), schemaTable.getTable());
//                            } else {
//                                sqlgElement = new SqlgEdge(this, id, schemaTable.getSchema(), schemaTable.getTable());
//                            }
//                            sqlgElement.loadResultSet(resultSet);
//                            sqlgElements.add((T) sqlgElement);
//                        }
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
//        } else {
//            //TODO use a union query
//            Set<String> tables = this.getTopology().getAllTables().keySet();
//            for (String table : tables) {
//                SchemaTable schemaTable = SchemaTable.from(this, table);
//                if (returnVertices ? schemaTable.isVertexTable() : !schemaTable.isVertexTable()) {
//                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
//                    sql.append(sqlDialect.maybeWrapInQoutes(schemaTable.getSchema()));
//                    sql.append(".");
//                    sql.append(sqlDialect.maybeWrapInQoutes(schemaTable.getTable()));
//                    if (this.getSqlDialect().needsSemicolon()) {
//                        sql.append(";");
//                    }
//                    Connection conn = this.tx().getConnection();
//                    if (logger.isDebugEnabled()) {
//                        logger.debug(sql.toString());
//                    }
//                    try (Statement statement = conn.createStatement()) {
//                        statement.execute(sql.toString());
//                        ResultSet resultSet = statement.getResultSet();
//                        while (resultSet.next()) {
//                            long id = resultSet.getLong("ID");
//                            SqlgElement sqlgElement;
//                            if (returnVertices) {
//                                sqlgElement = SqlgVertex.of(this, id, schemaTable.getSchema(), schemaTable.getTable().substring(VERTEX_PREFIX.length()));
//                            } else {
//                                sqlgElement = new SqlgEdge(this, id, schemaTable.getSchema(), schemaTable.getTable().substring(EDGE_PREFIX.length()));
//                            }
//                            sqlgElement.loadResultSet(resultSet);
//                            sqlgElements.add((T) sqlgElement);
//                        }
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
//        }
//        return sqlgElements;
    }

    public Connection getConnection() throws SQLException {
    	return this.sqlgDataSource.getDatasource().getConnection();
    }

    public SqlgDataSource getSqlgDataSource() {
        return sqlgDataSource;
    }

    /**
     * get the sqlg build version
     * @return the build version
     */
    public String getBuildVersion() {
		return buildVersion;
	}
    
}
