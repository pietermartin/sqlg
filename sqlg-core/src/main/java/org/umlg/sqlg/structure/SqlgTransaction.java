package org.umlg.sqlg.structure;

import org.umlg.sqlg.util.Preconditions;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlBulkDialect;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.EdgeRole;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is a singleton. Instantiated and owned by SqlGraph.
 * It manages the opening, commit, rollback and close of the java.sql.Connection in a threadvar.
 * Date: 2014/07/12
 * Time: 2:18 PM
 */
public class SqlgTransaction extends AbstractThreadLocalTransaction {

    private static final String BATCH_MODE_NOT_SUPPORTED = "Batch mode not supported!";
    @SuppressWarnings("WeakerAccess")
    public static final String QUERY_LAZY = "query.lazy";

    private final SqlgGraph sqlgGraph;
    private BeforeCommit beforeCommitFunction;
    private AfterCommit afterCommitFunction;
    private AfterRollback afterRollbackFunction;
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlgTransaction.class);

    private final ThreadLocal<TransactionCache> threadLocalTx = ThreadLocal.withInitial(() -> null);

    private final ThreadLocal<PreparedStatementCache> threadLocalPreparedStatementTx = ThreadLocal.withInitial(PreparedStatementCache::new);

    private final ThreadLocal<AtomicBoolean> threadLocalTopologyLocked = ThreadLocal.withInitial(() -> new AtomicBoolean(true));

    /**
     * default fetch size
     */
    private Integer defaultFetchSize = null;


    SqlgTransaction(Graph sqlgGraph) {
        super(sqlgGraph);
        this.sqlgGraph = (SqlgGraph) sqlgGraph;
    }

    @Override
    protected void doOpen() {
        if (isOpen())
            throw Transaction.Exceptions.transactionAlreadyOpen();
        else {
            try {
                Connection connection = this.sqlgGraph.getConnection();
                connection.setAutoCommit(false);
                if (this.sqlgGraph.getSqlDialect().supportsClientInfo()) {
                    String applicationName = Thread.currentThread().getName();
                    if (applicationName.length() > 63) {
                        String first = applicationName.substring(0, 30);
                        String last = applicationName.substring(applicationName.length() - 30);
                        applicationName = first + "..." + last;
                    }
                    connection.setClientInfo("ApplicationName", applicationName);
                }
                // read default setting for laziness
                boolean lazy = this.sqlgGraph.getConfiguration().getBoolean(QUERY_LAZY, true);
                TransactionCache transactionCache;
                if (supportsBatchMode()) {
                    transactionCache = TransactionCache.of(
                            connection,
                            new BatchManager(this.sqlgGraph, ((SqlBulkDialect) this.sqlgGraph.getSqlDialect())),
                            lazy
                    );
                } else {
                    transactionCache = TransactionCache.of(connection, lazy);
                }
                transactionCache.setFetchSize(getDefaultFetchSize());
                this.threadLocalTx.set(transactionCache);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void doCommit() throws TransactionException {
        if (!isOpen()) {
            return;
        }
        Connection connection = null;
        try {
            this.threadLocalTopologyLocked.get().set(true);
            if (supportsBatchMode() && this.threadLocalTx.get().getBatchManager().isInBatchMode()) {
                getBatchManager().flush();
            }
            connection = this.threadLocalTx.get().getConnection();
            if (this.beforeCommitFunction != null) {
                this.beforeCommitFunction.doBeforeCommit();
            }

            connection.commit();
            connection.setAutoCommit(true);
            if (this.afterCommitFunction != null) {
                this.afterCommitFunction.doAfterCommit();
            }
            this.threadLocalPreparedStatementTx.get().close();
        } catch (Exception e) {
            this.rollback();
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                LOGGER.error("Failed to close the connection on commit.", e);
            }
            if (this.threadLocalTx.get() != null) {
                this.threadLocalTx.get().clear();
                this.threadLocalTx.remove();
            }
            this.threadLocalPreparedStatementTx.remove();
        }
    }

    @Override
    protected void doRollback() throws TransactionException {
        if (!isOpen()) {
            return;
        }
        Connection connection = null;
        try {
            this.threadLocalTopologyLocked.get().set(true);
            if (supportsBatchMode() && this.threadLocalTx.get().getBatchManager().isInBatchMode()) {
                try {
                    this.threadLocalTx.get().getBatchManager().close();
                } catch (Exception e) {
                    //swallow
                    LOGGER.debug("exception closing streams on rollback", e);
                }
            }
            connection = threadLocalTx.get().getConnection();
            connection.setAutoCommit(false);
            connection.rollback();
            if (this.afterRollbackFunction != null) {
                this.afterRollbackFunction.doAfterRollback();
            }
            //noinspection Convert2streamapi
            for (ElementPropertyRollback elementPropertyRollback : threadLocalTx.get().getElementPropertyRollback().keySet()) {
                elementPropertyRollback.clearProperties();
            }
            this.threadLocalPreparedStatementTx.get().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                LOGGER.error("Failed to close the connection on rollback.", e);
            }
            if (isOpen()) {
                this.threadLocalTx.get().clear();
                this.threadLocalTx.remove();
                this.threadLocalPreparedStatementTx.remove();
            }
        }
    }

    /**
     * If {@link Topology#isLocked()} then this method will unlock the {@link Topology} for the duration of the transaction.
     * It will automatically be locked again on commit or rollback.
     */
    public void unlockTopology() {
        this.threadLocalTopologyLocked.get().set(false);
    }

    /**
     * @return Returns false if the topology is unlocked or the current transaction has not unlocked it.
     */
    public boolean isTopologyLocked() {
        return this.sqlgGraph.getTopology().isLocked() && this.threadLocalTopologyLocked.get().get();
    }

    public void streamingWithLockBatchModeOn() {
        if (supportsBatchMode()) {
            readWrite();
            this.threadLocalTx.get().getBatchManager().batchModeOn(BatchManager.BatchModeType.STREAMING_WITH_LOCK);
        } else {
            throw new IllegalStateException(BATCH_MODE_NOT_SUPPORTED);
        }
    }

    public void streamingBatchModeOn() {
        if (supportsBatchMode()) {
            readWrite();
            this.threadLocalTx.get().getBatchManager().batchModeOn(BatchManager.BatchModeType.STREAMING);
        } else {
            throw new IllegalStateException(BATCH_MODE_NOT_SUPPORTED);
        }
    }

    public void batchMode(BatchManager.BatchModeType batchModeType) {
        if (supportsBatchMode()) {
            switch (batchModeType) {
                case NONE -> {
                    readWrite();
                    this.threadLocalTx.get().getBatchManager().batchModeOn(BatchManager.BatchModeType.NONE);
                }
                case NORMAL -> this.normalBatchModeOn();
                case STREAMING -> this.streamingBatchModeOn();
                case STREAMING_WITH_LOCK -> this.streamingWithLockBatchModeOn();
                default -> throw new IllegalStateException("unhandled BatchModeType " + batchModeType.name());
            }
        }
    }

    public void normalBatchModeOn() {
        if (supportsBatchMode()) {
            readWrite();
            this.threadLocalTx.get().getBatchManager().batchModeOn(BatchManager.BatchModeType.NORMAL);
        } else {
            throw new IllegalStateException(BATCH_MODE_NOT_SUPPORTED);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public boolean isInBatchMode() {
        return supportsBatchMode() && isInNormalBatchMode() || isInStreamingBatchMode() || isInStreamingWithLockBatchMode();
    }

    public boolean isInNormalBatchMode() {
        return supportsBatchMode() && isOpen() && this.threadLocalTx.get().getBatchManager().isInNormalMode();
    }

    public boolean isInStreamingBatchMode() {
        return supportsBatchMode() && isOpen() && this.threadLocalTx.get().getBatchManager().isInStreamingMode();
    }

    public boolean isInStreamingWithLockBatchMode() {
        return supportsBatchMode() && isOpen() && this.threadLocalTx.get().getBatchManager().isInStreamingModeWithLock();
    }

    public BatchManager.BatchModeType getBatchModeType() {
        Preconditions.checkState(isOpen(), "SqlgTransaction.getBatchModeType() must be called within a transaction.");
        if (supportsBatchMode()) {
            return this.threadLocalTx.get().getBatchManager().getBatchModeType();
        } else {
            return BatchManager.BatchModeType.NONE;
        }
    }

    private boolean supportsBatchMode() {
        return this.sqlgGraph.getSqlDialect().supportsBatchMode();
    }

    public BatchManager getBatchManager() {
        return this.threadLocalTx.get().getBatchManager();
    }

    public Connection getConnection() {
        if (!isOpen()) {
            readWrite();
        }
        return this.threadLocalTx.get().getConnection();
    }

    public void flush() {
        if (!this.isInBatchMode()) {
            throw new IllegalStateException("Transaction must be in batch mode to flush");
        }
        LOGGER.debug("flushing transaction!!!");
        if (!this.getBatchManager().isBusyFlushing()) {
            this.getBatchManager().flush();
        }
    }

    void addElementPropertyRollback(ElementPropertyRollback elementPropertyRollback) {
        if (!isOpen()) {
            throw new IllegalStateException("A transaction must be in progress to add a elementPropertyRollback function!");
        }
        this.threadLocalTx.get().getElementPropertyRollback().put(elementPropertyRollback, null);
    }

    public void beforeCommit(BeforeCommit beforeCommitFunction) {
        this.beforeCommitFunction = beforeCommitFunction;
    }

    public void afterCommit(AfterCommit afterCommitFunction) {
        this.afterCommitFunction = afterCommitFunction;
    }

    public void afterRollback(AfterRollback afterCommitFunction) {
        this.afterRollbackFunction = afterCommitFunction;
    }

    @Override
    public boolean isOpen() {
        return this.threadLocalTx.get() != null;
    }

    public void add(PreparedStatement preparedStatement) {
        this.threadLocalPreparedStatementTx.get().add(preparedStatement);
    }

    // only used for tests
    public PreparedStatementCache getPreparedStatementCache() {
        return threadLocalPreparedStatementTx.get();
    }

    /**
     * are we reading the SQL query results lazily?
     *
     * @return true if we are processing the results lazily, false otherwise
     */
    @SuppressWarnings("WeakerAccess")
    public boolean isLazyQueries() {
        return this.threadLocalTx.get().isLazyQueries();
    }

    /**
     * set the laziness on query result reading
     *
     * @param lazy boolean to set the query as lazy or not.
     */
    public void setLazyQueries(boolean lazy) {
        readWrite();
        this.threadLocalTx.get().setLazyQueries(lazy);
    }

    /**
     * get default fetch size
     *
     * @return The default jdbc fetch size
     */
    private Integer getDefaultFetchSize() {
        return defaultFetchSize;
    }

    /**
     * set default fetch size
     *
     * @param fetchSize Set the jdbc fetch size
     */
    public void setDefaultFetchSize(Integer fetchSize) {
        this.defaultFetchSize = fetchSize;
    }

    /**
     * get fetch size for current transaction
     *
     * @return The jdbc fetch for the current transaction
     */
    public Integer getFetchSize() {
        readWrite();
        return this.threadLocalTx.get().getFetchSize();
    }

    /**
     * set fetch size for current transaction
     *
     * @param fetchSize Set the current transaction's jdbc fetch size
     */
    public void setFetchSize(Integer fetchSize) {
        readWrite();
        this.threadLocalTx.get().setFetchSize(fetchSize);
    }

    public boolean isWriteTransaction() {
        return this.threadLocalTx.get().isWriteTransaction();
    }

    public void setWriteTransaction(boolean b) {
        this.threadLocalTx.get().setWriteTransaction(b);
    }

    public void checkMultiplicity(Vertex vertex, Direction direction, EdgeLabel edgeLabel, VertexLabel otherSide) {
        EdgeRole edgeRole = null;
        switch (direction) {
            case OUT -> edgeRole = edgeLabel.getInEdgeRoles(otherSide);
            case IN -> edgeRole = edgeLabel.getOutEdgeRoles(otherSide);
            case BOTH -> throw new IllegalStateException("checkMultiplicity requires IN or OUT Direction, not BOTH");
        }
        Preconditions.checkNotNull(edgeRole, String.format("'%s' EdgeRole not found for '%s' and '%s'", direction.name(), edgeLabel.getLabel(), otherSide.getLabel()));
        Multiplicity multiplicity = edgeRole.getMultiplicity();
        if (multiplicity.hasLimits()) {
            long upper = multiplicity.upper();
            long lower = multiplicity.lower();
            long count = this.sqlgGraph.traversal().V(vertex).to(direction, edgeLabel.getLabel())
                    .has(T.label, otherSide.getFullName())
                    .count().next();
            if (upper != -1 && count > upper) {
                throw new IllegalStateException(String.format("Multiplicity check for EdgeLabel '%s' fails.\nUpper multiplicity is %d current upper multiplicity is %d", edgeLabel.getLabel(), upper, count));
            } else if (count < lower) {
                throw new IllegalStateException(String.format("Multiplicity check for EdgeLabel '%s' fails.\nLower multiplicity is %d current lower multiplicity is %d", edgeLabel.getLabel(), lower, count));
            }
        }
    }

    /**
     * Utilizes LEFT JOIN but COUNT on the far side.
     */
    public void checkMultiplicity(VertexLabel vertexLabel, Direction direction, EdgeLabel edgeLabel, VertexLabel otherSide) {
        EdgeRole edgeRole = null;
        switch (direction) {
            case OUT -> edgeRole = edgeLabel.getInEdgeRoles(otherSide);
            case IN -> edgeRole = edgeLabel.getOutEdgeRoles(otherSide);
            case BOTH -> throw new IllegalStateException("checkMultiplicity requires IN or OUT Direction, not BOTH");
        }
        Preconditions.checkNotNull(edgeRole, String.format("'%s' EdgeRole not found for '%s' and '%s'", direction.name(), edgeLabel.getLabel(), otherSide.getLabel()));
        Multiplicity multiplicity = edgeRole.getMultiplicity();
        if (multiplicity.hasLimits()) {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT\n\t");
            if (vertexLabel.hasIDPrimaryKey()) {
                sql.append("a.");
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
            } else {
                int count = 1;
                for (String identifier : vertexLabel.getIdentifiers()) {
                    sql.append("a.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    if (count++ < vertexLabel.getIdentifiers().size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(", COUNT(");
            if (otherSide.hasIDPrimaryKey()) {
                sql.append("b.");
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                sql.append(")\n");
            } else {
                String identifier = otherSide.getIdentifiers().get(0);
                sql.append("b.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                sql.append(")\n");
            }
            sql.append("FROM\n\t");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getSchema().getName()));
            sql.append(".");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.VERTEX_PREFIX + vertexLabel.getName()));
            sql.append(" a LEFT JOIN\n\t");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeLabel.getSchema().getName()));
            sql.append(".");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()));
            if (vertexLabel.hasIDPrimaryKey()) {
                sql.append(" ab ON a.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                sql.append(" = ab.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getFullName() +
                        (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
            } else {
                sql.append(" ab ON ");
                int count = 1;
                for (String identifier : vertexLabel.getIdentifiers()) {
                    sql.append("a.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    sql.append(" = ab.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getFullName() + "." + identifier +
                            (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                    if (count++ < vertexLabel.getIdentifiers().size()) {
                        sql.append(" AND ");
                    }
                }
            }
            sql.append(" LEFT JOIN\n\t");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(otherSide.getSchema().getName()));
            sql.append(".");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.VERTEX_PREFIX + otherSide.getName()));
            if (otherSide.hasIDPrimaryKey()) {
                sql.append(" b ON ab.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(otherSide.getFullName() +
                        (direction == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                sql.append(" = b.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
            } else {
                sql.append(" b ON ");
                int count = 1;
                for (String identifier : otherSide.getIdentifiers()) {
                    sql.append("ab.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(otherSide.getFullName() + "." + identifier +
                            (direction == Direction.OUT ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
                    sql.append(" = b.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    if (count++ < otherSide.getIdentifiers().size()) {
                        sql.append(" AND ");
                    }
                }
            }
            sql.append("\nGROUP BY\n\t");
            if (vertexLabel.hasIDPrimaryKey()) {
                sql.append("a.");
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
            } else {
                StringBuilder sb = new StringBuilder();
                int count = 1;
                for (String identifier : vertexLabel.getIdentifiers()) {
                    sb.append("a.").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    if (count++ < vertexLabel.getIdentifiers().size()) {
                        sb.append(", ");
                    }
                }
                sql.append(sb);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql.toString());
            }
            long upper = multiplicity.upper();
            long lower = multiplicity.lower();
            Connection connection = this.sqlgGraph.tx().getConnection();
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(sql.toString());
                if (vertexLabel.hasIDPrimaryKey()) {
                    while (rs.next()) {
                        long id = rs.getLong(1);
                        long count = rs.getLong(2);
                        RecordId recordId = RecordId.from(SchemaTable.of(vertexLabel.getSchema().getName(), vertexLabel.getName()), id);
                        if (upper  != -1 && count > upper) {
                            throw new IllegalStateException(String.format("Multiplicity check for EdgeLabel '%s' fails for '%s'.\nUpper multiplicity is [%d] current multiplicity is [%d]", edgeLabel.getLabel(), recordId, upper, count));
                        } else if (count < lower) {
                            throw new IllegalStateException(String.format("Multiplicity check for EdgeLabel '%s' fails for '%s'.\nLower multiplicity is [%d] current multiplicity is [%d]", edgeLabel.getLabel(), recordId, lower, count));
                        }
                    }
                } else {
                    while (rs.next()) {
                        List<Comparable> identifiers = new ArrayList<>();
                        for (String identifier : vertexLabel.getIdentifiers()) {
                            Comparable id = (Comparable) rs.getObject(identifier);
                            identifiers.add(id);
                        }
                        long count = rs.getLong(vertexLabel.getIdentifiers().size() + 1);
                        RecordId recordId = RecordId.from(SchemaTable.of(vertexLabel.getSchema().getName(), vertexLabel.getName()), identifiers);
                        if (upper != -1 && count > upper) {
                            throw new IllegalStateException(String.format("Multiplicity check for EdgeLabel '%s' fails for '%s'.\nUpper multiplicity is [%d] current upper multiplicity is [%d]", edgeLabel.getLabel(), recordId, upper, count));
                        } else if (count < lower) {
                            throw new IllegalStateException(String.format("Multiplicity check for EdgeLabel '%s' fails for '%s'.\nLower multiplicity is [%d] current lower multiplicity is [%d]", edgeLabel.getLabel(), recordId, lower, count));
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
