package org.umlg.sqlg.structure;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.util.VersionUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.topology.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * This class contains the logic to ensure all is well on startup.
 * <p>
 * Date: 2016/11/14
 * Time: 10:22 AM
 */
class SqlgStartupManager {

    private static final String APPLICATION_VERSION = "application.version";
    private static final String SQLG_APPLICATION_PROPERTIES = "sqlg.application.properties";
    private static final Logger logger = LoggerFactory.getLogger(SqlgStartupManager.class);
    private final SqlgGraph sqlgGraph;
    private final SqlDialect sqlDialect;

    private String buildVersion;

    SqlgStartupManager(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.sqlDialect = sqlgGraph.getSqlDialect();
    }

    void loadSqlgSchema() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("SchemaManager.loadSqlgSchema()...");
            }
            //check if the topology schema exists, if not createVertexLabel it
            boolean existSqlgSchema = existSqlgSchema();
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            if (!existSqlgSchema) {
                //This exist separately because Hsqldb and H2 do not support "if exist" in the schema creation sql.
                createSqlgSchema();
            }
            if (!existGuiSchema()) {
                createGuiSchema();
            }
            if (!existSqlgSchema) {
                createSqlgSchemaTablesAndIndexes();
            }
            //The default schema is generally called 'public' and is created upfront by the db.
            //But what if its been deleted, so check.
            if (!existDefaultSchema()) {
                createDefaultSchema();
            }
            //committing here will ensure that sqlg creates the tables.
            this.sqlgGraph.tx().commit();
            stopWatch.stop();
            logger.debug("Time to createVertexLabel sqlg topology: " + stopWatch.toString());
            if (!existSqlgSchema) {
                addPublicSchema();
                this.sqlgGraph.tx().commit();
            }
            if (!existSqlgSchema) {
                //old versions of sqlg needs the topology populated from the information_schema table.
                logger.debug("Upgrading sqlg from pre sqlg_schema version to sqlg_schema version");
                StopWatch stopWatch2 = new StopWatch();
                stopWatch2.start();
                loadSqlgSchemaFromInformationSchema();
                String version = getBuildVersion();
                TopologyManager.addGraph(this.sqlgGraph, version);
                stopWatch2.stop();
                logger.debug("Time to upgrade sqlg from pre sqlg_schema: " + stopWatch2.toString());
                logger.debug("Done upgrading sqlg from pre sqlg_schema version to sqlg_schema version");
            } else {
                // make sure the index edge index property exist, this if for upgrading from 1.3.4 to 1.4.0
                upgradeIndexEdgeSequenceToExist();
                //make sure the sqlg_schema.graph exists.
                String version = getBuildVersion();
                String oldVersion = createOrUpdateGraph(version);
                if (oldVersion == null || !oldVersion.equals(version)) {
                    updateTopology(oldVersion);
                }
            }
            cacheTopology();
            if (this.sqlgGraph.configuration().getBoolean("validate.topology", false)) {
                validateTopology();
            }
            this.sqlgGraph.tx().commit();
        } catch (Exception e) {
            this.sqlgGraph.tx().rollback();
            throw e;
        }
    }

    private void updateTopology(String oldVersion) {
        Version v = Version.unknownVersion();
        if (oldVersion != null) {
            v = VersionUtil.parseVersion(oldVersion, null, null);
        }
        if (v.isUnknownVersion() || v.compareTo(new Version(1, 5, 0, null, null, null)) < 0) {
            if (this.sqlDialect.supportsDeferrableForeignKey()) {
                upgradeForeignKeysToDeferrable();
            }
        }
        if (v.isUnknownVersion() || v.compareTo(new Version(2, 0, 0, null, null, null)) < 0) {
            addPartitionSupportToSqlgSchema();
        }
    }

    private void addPartitionSupportToSqlgSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        List<String> addPartitionTables = this.sqlDialect.addPartitionTables();
        for (String addPartitionTable : addPartitionTables) {
            try (Statement s = conn.createStatement()) {
                s.execute(addPartitionTable);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void upgradeForeignKeysToDeferrable() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement s = conn.prepareStatement(this.sqlDialect.sqlToGetAllForeignKeys())) {
            ResultSet rs = s.executeQuery();
            while (rs.next()) {
                String schema = rs.getString(1);
                String table = rs.getString(2);
                String fk = rs.getString(3);

                try (Statement statement = conn.createStatement()) {
                    statement.execute(this.sqlDialect.alterForeignKeyToDeferrable(schema, table, fk));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void cacheTopology() {
        this.sqlgGraph.getTopology().cacheTopology();
    }

    private void validateTopology() {
        this.sqlgGraph.getTopology().validateTopology();
        if (!this.sqlgGraph.getTopology().getValidationErrors().isEmpty()) {
            for (Topology.TopologyValidationError topologyValidationError : this.sqlgGraph.getTopology().getValidationErrors()) {
                logger.warn(topologyValidationError.toString());
            }
        }
    }

    /**
     * create or update the graph metadata
     *
     * @param version the new version of the graph
     * @return the old version of the graph, or null if there was no graph
     */
    private String createOrUpdateGraph(String version) {
        String oldVersion = null;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            String[] types = new String[]{"TABLE"};
            //load the vertices
            try (ResultSet vertexRs = metadata.getTables(null, Schema.SQLG_SCHEMA, Topology.VERTEX_PREFIX + Topology.GRAPH, types)) {
                if (!vertexRs.next()) {
                    try (Statement statement = conn.createStatement()) {
                        String sql = this.sqlDialect.sqlgCreateTopologyGraph();
                        statement.execute(sql);
                        TopologyManager.addGraph(this.sqlgGraph, version);
                    }
                } else {
                    //Need to check if dbVersion has been added
                    try (ResultSet columnRs = metadata.getColumns(null, Schema.SQLG_SCHEMA, Topology.VERTEX_PREFIX + Topology.GRAPH, Topology.SQLG_SCHEMA_GRAPH_DB_VERSION)) {
                        if (!columnRs.next()) {
                            try (Statement statement = conn.createStatement()) {
                                statement.execute(sqlDialect.addDbVersionToGraph(metadata));
                            }
                        }
                    }
                    oldVersion = TopologyManager.updateGraph(this.sqlgGraph, version);
                }
                return oldVersion;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void upgradeIndexEdgeSequenceToExist() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = null;
            @SuppressWarnings("ConstantConditions")
            List<Triple<String, Integer, String>> columns = this.sqlDialect.getTableColumns(metadata, catalog, Topology.SQLG_SCHEMA, Topology.EDGE_PREFIX + "index_property", SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE);
            if (columns.isEmpty()) {
                try (Statement statement = conn.createStatement()) {
                    String sql = this.sqlDialect.sqlgAddIndexEdgeSequenceColumn();
                    statement.execute(sql);
                }
            }
        } catch (SQLException e) {
            logger.error("Error upgrading index edge property to include a sequence column. Error swallowed.", e);
        }
    }

    @SuppressWarnings("ConstantConditions")
    private void loadSqlgSchemaFromInformationSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            List<String> schemaNames = this.sqlDialect.getSchemaNames(metadata);
            for (String schemaName : schemaNames) {
                if (schemaName.equals(SQLG_SCHEMA) ||
                        schemaName.equals(this.sqlDialect.getPublicSchema()) ||
                        this.sqlDialect.getGisSchemas().contains(schemaName)) {
                    continue;
                }
                TopologyManager.addSchema(this.sqlgGraph, schemaName);
            }
            Map<String, Set<IndexRef>> indices = this.sqlDialect.extractIndices(conn, catalog, schemaPattern);

            //load the vertices
            List<Triple<String, String, String>> vertexTables = this.sqlDialect.getVertexTables(metadata);
            for (Triple<String, String, String> vertexTable : vertexTables) {

                String tblCat = vertexTable.getLeft();
                String schema = vertexTable.getMiddle();
                String table = vertexTable.getRight();

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getInternalSchemas());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }
                Map<String, PropertyType> columns = new ConcurrentHashMap<>();
                List<Triple<String, Integer, String>> metaDatas = this.sqlDialect.getTableColumns(metadata, tblCat, schema, table, null);
                ListIterator<Triple<String, Integer, String>> metaDataIter = metaDatas.listIterator();
                while (metaDataIter.hasNext()) {
                    Triple<String, Integer, String> tripple = metaDataIter.next();
                    String columnName = tripple.getLeft();
                    int columnType = tripple.getMiddle();
                    String typeName = tripple.getRight();
                    if (!columnName.equals(Topology.ID)) {
                        extractProperty(schema, table, columnName, columnType, typeName, columns, metaDataIter);
                    }
                }
                String label = table.substring(Topology.VERTEX_PREFIX.length());
                List<String> primaryKeys = this.sqlDialect.getPrimaryKeys(metadata, tblCat, schema, table);
                if (primaryKeys.size() == 1 && primaryKeys.get(0).equals(Topology.ID)) {
                    TopologyManager.addVertexLabel(this.sqlgGraph, schema, label, columns, new ListOrderedSet<>());
                } else {
                    //partitioned tables have no pk and must have identifiers.
                    //however we can not tell which columns are the identifiers so ahem???
                    //we do a little hardcoding. ID,uid and uuid are determined to be identifiers.
                    if (primaryKeys.isEmpty()) {
                        ListOrderedSet<String> identifiers = new ListOrderedSet<>();
                        for (String s : columns.keySet()) {
                            if (s.equalsIgnoreCase("ID") || s.equalsIgnoreCase("uid") || s.equalsIgnoreCase("uuid")) {
                                identifiers.add(s);
                            }
                        }
                        TopologyManager.addVertexLabel(this.sqlgGraph, schema, label, columns, identifiers);
                    } else {
                        TopologyManager.addVertexLabel(this.sqlgGraph, schema, label, columns, ListOrderedSet.listOrderedSet(primaryKeys));
                    }
                }
                if (indices != null) {
                    String key = tblCat + "." + schema + "." + table;
                    Set<IndexRef> idxs = indices.get(key);
                    if (idxs != null) {
                        for (IndexRef ir : idxs) {
                            TopologyManager.addIndex(sqlgGraph, schema, label, true, ir.getIndexName(), ir.getIndexType(), ir.getColumns());
                        }
                    }
                } else {
                    extractIndices(metadata, tblCat, schema, table, label, true);
                }
            }
            //load the edges without their properties
            List<Triple<String, String, String>> edgeTables = this.sqlDialect.getEdgeTables(metadata);
            for (Triple<String, String, String> edgeTable : edgeTables) {
                String edgCat = edgeTable.getLeft();
                String schema = edgeTable.getMiddle();
                String table = edgeTable.getRight();

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getInternalSchemas());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }
                List<Triple<String, Integer, String>> edgeColumns = this.sqlDialect.getTableColumns(metadata, edgCat, schema, table, null);
                List<String> primaryKeys = this.sqlDialect.getPrimaryKeys(metadata, edgCat, schema, table);
                Vertex edgeVertex;
                if (hasIDPrimaryKey(primaryKeys)) {
                    edgeVertex = TopologyManager.addEdgeLabel(this.sqlgGraph, table, Collections.emptyMap(), new ListOrderedSet<>(), PartitionType.NONE, null);
                } else {
                    //partitioned tables have no pk and must have identifiers.
                    //however we can not tell which columns are the identifiers so ahem???
                    //we do a little hardcoding. ID,uid and uuid are determined to be identifiers.
                    if (primaryKeys.isEmpty()) {
                        ListOrderedSet<String> identifiers = new ListOrderedSet<>();
                        for (Triple<String, Integer, String> s : edgeColumns) {
                            if (s.getLeft().equalsIgnoreCase("ID") || s.getLeft().equalsIgnoreCase("uid") || s.getLeft().equalsIgnoreCase("uuid")) {
                                identifiers.add(s.getLeft());
                            }
                        }
                        edgeVertex = TopologyManager.addEdgeLabel(this.sqlgGraph, table, Collections.emptyMap(), identifiers, PartitionType.NONE, null);
                    } else {
                        edgeVertex = TopologyManager.addEdgeLabel(this.sqlgGraph, table, Collections.emptyMap(), ListOrderedSet.listOrderedSet(primaryKeys), PartitionType.NONE, null);
                    }
                }
                for (Triple<String, Integer, String> edgeColumn : edgeColumns) {
                    String column = edgeColumn.getLeft();
                    if (table.startsWith(EDGE_PREFIX) && (column.endsWith(Topology.IN_VERTEX_COLUMN_END) || column.endsWith(Topology.OUT_VERTEX_COLUMN_END))) {
                        String[] split = column.split("\\.");
                        SchemaTable foreignKey;
                        if (hasIDPrimaryKey(primaryKeys)) {
                            foreignKey = SchemaTable.of(split[0], split[1]);
                        } else {
                            //There could be no ID pk because of user defined pk or because partioned tables have no pk.
                            //This logic is because in TopologyManager.addLabelToEdge the '__I' or '__O' is assumed to be present and gets trimmed.
                            if (column.endsWith(Topology.IN_VERTEX_COLUMN_END)) {
                                if (split.length == 3) {
                                    //user defined pk
                                    foreignKey = SchemaTable.of(split[0], split[1] + Topology.IN_VERTEX_COLUMN_END);
                                } else {
                                    foreignKey = SchemaTable.of(split[0], split[1]);
                                }
                            } else {
                                if (split.length == 3) {
                                    //user defined pk
                                    foreignKey = SchemaTable.of(split[0], split[1] + Topology.OUT_VERTEX_COLUMN_END);
                                } else {
                                    foreignKey = SchemaTable.of(split[0], split[1]);
                                }
                            }
                        }
                        if (column.endsWith(Topology.IN_VERTEX_COLUMN_END)) {
                            TopologyManager.addLabelToEdge(this.sqlgGraph, edgeVertex, schema, table, true, foreignKey);
                        } else if (column.endsWith(Topology.OUT_VERTEX_COLUMN_END)) {
                            TopologyManager.addLabelToEdge(this.sqlgGraph, edgeVertex, schema, table, false, foreignKey);
                        }
                    }
                }
            }

            //load the edges without their in and out vertices
            for (Triple<String, String, String> edgeTable : edgeTables) {

                String edgCat = edgeTable.getLeft();
                String schema = edgeTable.getMiddle();
                String table = edgeTable.getRight();
                List<String> primaryKeys = this.sqlDialect.getPrimaryKeys(metadata, edgCat, schema, table);

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getInternalSchemas());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }

                Map<String, PropertyType> columns = new HashMap<>();
                //get the columns

                List<Triple<String, Integer, String>> metaDatas = this.sqlDialect.getTableColumns(metadata, edgCat, schema, table, null);
                ListIterator<Triple<String, Integer, String>> metaDataIter = metaDatas.listIterator();
                while (metaDataIter.hasNext()) {
                    Triple<String, Integer, String> tripple = metaDataIter.next();
                    String columnName = tripple.getLeft();
                    String typeName = tripple.getRight();
                    int columnType = tripple.getMiddle();
                    if (!columnName.equals(Topology.ID)) {
                        extractProperty(schema, table, columnName, columnType, typeName, columns, metaDataIter);
                    }
                }
                TopologyManager.addEdgeColumn(this.sqlgGraph, schema, table, columns, ListOrderedSet.listOrderedSet(primaryKeys));
                String label = table.substring(Topology.EDGE_PREFIX.length());
                if (indices != null) {
                    String key = edgCat + "." + schema + "." + table;
                    Set<IndexRef> idxs = indices.get(key);
                    if (idxs != null) {
                        for (IndexRef ir : idxs) {
                            TopologyManager.addIndex(sqlgGraph, schema, label, false, ir.getIndexName(), ir.getIndexType(), ir.getColumns());
                        }
                    }
                } else {
                    extractIndices(metadata, edgCat, schema, table, label, false);
                }

            }

            if (this.sqlDialect.supportsPartitioning()) {
                //load the partitions
                conn = this.sqlgGraph.tx().getConnection();
                List<Map<String, String>> partitions = this.sqlDialect.getPartitions(conn);
                List<PartitionTree> roots = PartitionTree.build(partitions);
                for (PartitionTree root : roots) {
                    root.createPartitions(this.sqlgGraph);
                }
            }
            this.sqlgGraph.tx().commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void extractIndices(DatabaseMetaData metadata,
                                String catalog,
                                String schema,
                                String table,
                                String label,
                                boolean isVertex) {

        String lastIndexName = null;
        IndexType lastIndexType = null;
        List<String> lastColumns = new LinkedList<>();
        List<Triple<String, Boolean, String>> indexes = this.sqlDialect.getIndexInfo(metadata, catalog, schema, table, false, true);
        for (Triple<String, Boolean, String> index : indexes) {

            String indexName = index.getLeft();
            boolean nonUnique = index.getMiddle();
            String columnName = index.getRight();

            if (lastIndexName == null) {
                lastIndexName = indexName;
                lastIndexType = nonUnique ? IndexType.NON_UNIQUE : IndexType.UNIQUE;
            } else if (!lastIndexName.equals(indexName)) {
                if (!this.sqlDialect.isSystemIndex(lastIndexName)) {
                    if (!Schema.GLOBAL_UNIQUE_INDEX_SCHEMA.equals(schema)) {
                        TopologyManager.addIndex(sqlgGraph, schema, label, isVertex, lastIndexName, lastIndexType, lastColumns);
                    }
                }
                lastColumns.clear();
                lastIndexName = indexName;
                lastIndexType = nonUnique ? IndexType.NON_UNIQUE : IndexType.UNIQUE;
            }
            lastColumns.add(columnName);
        }
        if (!this.sqlDialect.isSystemIndex(lastIndexName)) {
            if (!Schema.GLOBAL_UNIQUE_INDEX_SCHEMA.equals(schema)) {
                TopologyManager.addIndex(sqlgGraph, schema, label, isVertex, lastIndexName, lastIndexType, lastColumns);
            }
        }
    }

    private void extractProperty(String schema, String table, String columnName, Integer columnType, String typeName, Map<String, PropertyType> columns, ListIterator<Triple<String, Integer, String>> metaDataIter) {
        //check for ZONEDDATETIME, PERIOD, DURATION as they use more than one field to represent the type
        PropertyType propertyType = null;
        if (metaDataIter.hasNext()) {
            Triple<String, Integer, String> column2MetaData = metaDataIter.next();
            String column2Name = column2MetaData.getLeft();
            String typeName2 = column2MetaData.getRight();
            int column2Type = column2MetaData.getMiddle();
            if (column2Name.startsWith(columnName + "~~~")) {
                if (column2Type == Types.VARCHAR) {
                    propertyType = PropertyType.ZONEDDATETIME;
                } else if ((column2Type == Types.ARRAY && this.sqlDialect.sqlArrayTypeNameToPropertyType(typeName2, this.sqlgGraph, schema, table, column2Name, metaDataIter) == PropertyType.STRING_ARRAY)) {
                    propertyType = PropertyType.ZONEDDATETIME_ARRAY;
                } else {
                    if (metaDataIter.hasNext()) {
                        Triple<String, Integer, String> column3MetaData = metaDataIter.next();
                        String column3Name = column3MetaData.getLeft();
                        String typeName3 = column3MetaData.getRight();
                        int column3Type = column3MetaData.getMiddle();
                        if (column3Name.startsWith(columnName + "~~~")) {
                            if (column3Type == Types.ARRAY) {
                                Preconditions.checkState(sqlDialect.sqlArrayTypeNameToPropertyType(typeName3, this.sqlgGraph, schema, table, column3Name, metaDataIter) == PropertyType.INTEGER_ARRAY, "Only Period have a third column and it must be a Integer");
                                propertyType = PropertyType.PERIOD_ARRAY;
                            } else {
                                Preconditions.checkState(column3Type == Types.INTEGER, "Only Period have a third column and it must be a Integer");
                                propertyType = PropertyType.PERIOD;
                            }
                        } else {
                            metaDataIter.previous();
                            if (column2Type == Types.ARRAY) {
                                Preconditions.checkState(sqlDialect.sqlArrayTypeNameToPropertyType(typeName2, this.sqlgGraph, schema, table, column2Name, metaDataIter) == PropertyType.INTEGER_ARRAY, "Only Period have a third column and it must be a Integer");
                                propertyType = PropertyType.DURATION_ARRAY;
                            } else {
                                Preconditions.checkState(column2Type == Types.INTEGER, "Only Duration and Period have a second column and it must be a Integer");
                                propertyType = PropertyType.DURATION;
                            }
                        }
                    }
                }
            } else {
                metaDataIter.previous();
            }
        }
        if (propertyType == null) {
            propertyType = this.sqlDialect.sqlTypeToPropertyType(this.sqlgGraph, schema, table, columnName, columnType, typeName, metaDataIter);
        }
        columns.put(columnName, propertyType);
    }

    private void addPublicSchema() {
        this.sqlgGraph.addVertex(
                T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", this.sqlDialect.getPublicSchema(),
                Topology.CREATED_ON, LocalDateTime.now()
        );
    }

    private void createGuiSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute(this.sqlDialect.sqlgGuiSchemaCreationScript());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createSqlgSchemaTablesAndIndexes() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            List<String> creationScripts = this.sqlDialect.sqlgTopologyCreationScripts();
            //Hsqldb can not do this in one go
            for (String creationScript : creationScripts) {
                if (logger.isDebugEnabled()) {
                    logger.debug(creationScript);
                }
                statement.execute(creationScript);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean existSqlgSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            return this.sqlDialect.schemaExists(metadata, SQLG_SCHEMA);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean existDefaultSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            return this.sqlDialect.schemaExists(metadata, this.sqlDialect.getPublicSchema());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean existGuiSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            return this.sqlDialect.schemaExists(metadata, Schema.GLOBAL_UNIQUE_INDEX_SCHEMA);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createSqlgSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            String creationScript = this.sqlDialect.sqlgSqlgSchemaCreationScript();
            statement.execute(creationScript);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createDefaultSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute(this.sqlDialect.createSchemaStatement(this.sqlDialect.getPublicSchema()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * get the build version
     *
     * @return the build version, or null if unknown
     */
    String getBuildVersion() {
        if (this.buildVersion == null) {
            Properties prop = new Properties();
            try {
                // try system
                URL u = ClassLoader.getSystemResource(SQLG_APPLICATION_PROPERTIES);
                if (u == null) {
                    // try own class loader
                    u = getClass().getClassLoader().getResource(SQLG_APPLICATION_PROPERTIES);
                }
                if (u != null) {
                    try (InputStream is = u.openStream()) {
                        prop.load(is);
                    }
                    this.buildVersion = prop.getProperty(APPLICATION_VERSION);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return this.buildVersion;
    }

    private boolean hasIDPrimaryKey(List<String> primaryKeys) {
        return primaryKeys.size() == 1 && primaryKeys.get(0).equals(Topology.ID);
    }
}
