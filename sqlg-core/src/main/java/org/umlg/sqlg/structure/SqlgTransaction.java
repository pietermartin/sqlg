package org.umlg.sqlg.structure;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/**
 * This class is a singleton. Instantiated and owned by SqlG.
 * It manages the opening, commit, rollback and close of the java.sql.Connection in a threadvar.
 * Date: 2014/07/12
 * Time: 2:18 PM
 */
public class SqlgTransaction extends AbstractThreadLocalTransaction {

    public static final String BATCH_MODE_NOT_SUPPORTED = "Batch mode not supported!";
    private SqlgGraph sqlgGraph;
    private AfterCommit afterCommitFunction;
    private AfterRollback afterRollbackFunction;
    private Logger logger = LoggerFactory.getLogger(SqlgTransaction.class.getName());

    protected final ThreadLocal<TransactionCache> threadLocalTx = new ThreadLocal<TransactionCache>() {
        protected TransactionCache initialValue() {
            return null;
        }
    };


    public SqlgTransaction(Graph sqlgGraph) {
        super(sqlgGraph);
        this.sqlgGraph = (SqlgGraph) sqlgGraph;
    }

    @Override
    protected void doOpen() {
        if (isOpen())
            throw Transaction.Exceptions.transactionAlreadyOpen();
        else {
            try {
                Connection connection = this.sqlgGraph.getSqlgDataSource().get(this.sqlgGraph.getJdbcUrl()).getConnection();
                connection.setAutoCommit(false);
                //this does not seem needed
//                if (this.sqlgGraph.getSqlDialect().isPostgresql() && this.sqlgGraph.configuration().getBoolean("sqlg.postgres.gis", false)) {
//                    this.sqlgGraph.getSqlDialect().registerGisDataTypes(connection);
//                }
                if (this.sqlgGraph.getSqlDialect().supportsClientInfo()) {
                    connection.setClientInfo("ApplicationName", Thread.currentThread().getName());
                }
                //Not supported by postgres, //TODO add dialect indirection
//                connection.setClientInfo("ClientUser", "//TODO");
//                try {
//                    connection.setClientInfo("ClientHostname", InetAddress.getLocalHost().getHostAddress());
//                } catch (UnknownHostException e) {
//                    connection.setClientInfo("ClientHostname", "failed");
//                }
                threadLocalTx.set(TransactionCache.of(connection, new ArrayList<>(), new BatchManager(this.sqlgGraph, this.sqlgGraph.getSqlDialect())));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void doCommit() throws TransactionException {
        if (!isOpen())
            return;

        try {
            if (this.threadLocalTx.get().getBatchManager().isInBatchMode()) {
                this.threadLocalTx.get().getBatchManager().flush();
            }
            Connection connection = threadLocalTx.get().getConnection();
            connection.commit();
            connection.setAutoCommit(true);
            if (this.afterCommitFunction != null) {
                this.afterCommitFunction.doAfterCommit();
            }
            connection.close();
        } catch (Exception e) {
            this.rollback();
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            if (this.threadLocalTx.get() != null) {
                this.threadLocalTx.get().clear();
                this.threadLocalTx.remove();
            }
        }
    }

    @Override
    protected void doRollback() throws TransactionException {
        if (!isOpen())
            return;
        try {
            if (this.threadLocalTx.get().getBatchManager().isInBatchMode()) {
                try {
                    this.threadLocalTx.get().getBatchManager().close();
                } catch (Exception e) {
                    //swallow
                    logger.debug("exception closing streams on rollback", e);
                }
            }
            Connection connection = threadLocalTx.get().getConnection();
            connection.rollback();
            if (this.afterRollbackFunction != null) {
                this.afterRollbackFunction.doAfterRollback();
            }
            for (ElementPropertyRollback elementPropertyRollback : threadLocalTx.get().getElementPropertyRollback()) {
                elementPropertyRollback.clearProperties();
            }
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (isOpen()) {
                threadLocalTx.get().clear();
                threadLocalTx.remove();
            }
        }
    }

    public void streamingWithLockBatchModeOn() {
        if (this.sqlgGraph.features().supportsBatchMode()) {
            readWrite();
            threadLocalTx.get().getBatchManager().batchModeOn(BatchManager.BatchModeType.STREAMING_WITH_LOCK);
        } else {
            throw new IllegalStateException(BATCH_MODE_NOT_SUPPORTED);
        }
    }

    public void streamingBatchModeOn() {
        if (this.sqlgGraph.features().supportsBatchMode()) {
            readWrite();
            threadLocalTx.get().getBatchManager().batchModeOn(BatchManager.BatchModeType.STREAMING);
        } else {
            throw new IllegalStateException(BATCH_MODE_NOT_SUPPORTED);
        }
    }

    public void batchMode(BatchManager.BatchModeType batchModeType) {
        switch (batchModeType) {
            case NONE:
                readWrite();
                threadLocalTx.get().getBatchManager().batchModeOn(BatchManager.BatchModeType.NONE);
                break;
            case NORMAL:
                this.normalBatchModeOn();
                break;
            case STREAMING:
                this.streamingBatchModeOn();
                break;
            case STREAMING_WITH_LOCK:
                this.streamingWithLockBatchModeOn();
                break;
            default:
                throw new IllegalStateException("unhandled BatchModeType " + batchModeType.name());
        }
    }

    public void normalBatchModeOn() {
        if (this.sqlgGraph.features().supportsBatchMode()) {
            readWrite();
            threadLocalTx.get().getBatchManager().batchModeOn(BatchManager.BatchModeType.NORMAL);
        } else {
            throw new IllegalStateException(BATCH_MODE_NOT_SUPPORTED);
        }
    }

    public boolean isInBatchMode() {
        return isInNormalBatchMode() || isInStreamingBatchMode() || isInStreamingWithLockBatchMode();
    }

    public boolean isInNormalBatchMode() {
        return isOpen() && threadLocalTx.get().getBatchManager().isInNormalMode();
    }

    public boolean isInStreamingBatchMode() {
        return isOpen() && threadLocalTx.get().getBatchManager().isInStreamingMode();
    }

    public boolean isInStreamingWithLockBatchMode() {
        return isOpen() && threadLocalTx.get().getBatchManager().isInStreamingModeWithLock();
    }

    public BatchManager.BatchModeType getBatchModeType() {
        assert isOpen() : "SqlgTransaction.getBatchModeType() must be called within a transaction.";
        return threadLocalTx.get().getBatchManager().getBatchModeType();
    }

    public BatchManager getBatchManager() {
        return threadLocalTx.get().getBatchManager();
    }

    void setSchemaModification(boolean schemaModification) {
        threadLocalTx.get().setSchemaModification(schemaModification);
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
        this.logger.debug("flushing transaction!!!");
        if (!this.getBatchManager().isBusyFlushing()) {
            this.getBatchManager().flush();
        }
    }

    public Map<SchemaTable, Pair<Long, Long>> batchCommit() {
        if (!threadLocalTx.get().getBatchManager().isInBatchMode()) {
            throw new IllegalStateException("Must be in batch mode to batchCommit! Current mode is " + threadLocalTx.get().getBatchManager().getBatchModeType().name());
        }

        if (!isOpen()) {
            return Collections.emptyMap();
        }

        try {
            Map<SchemaTable, Pair<Long, Long>> verticesRange = this.threadLocalTx.get().getBatchManager().flush();
            Connection connection = this.threadLocalTx.get().getConnection();
            connection.commit();
            connection.setAutoCommit(true);
            if (this.afterCommitFunction != null) {
                this.afterCommitFunction.doAfterCommit();
            }
            connection.close();
            return verticesRange;
        } catch (Exception e) {
            this.rollback();
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            //this is null after a rollback.
            //might occur if sqlg has a bug and there is a SqlException
            if (this.threadLocalTx.get() != null) {
                this.threadLocalTx.get().clear();
                this.threadLocalTx.remove();
            }
        }
    }

    public void addElementPropertyRollback(ElementPropertyRollback elementPropertyRollback) {
        if (!isOpen()) {
            throw new IllegalStateException("A transaction must be in progress to add a elementPropertyRollback function!");
        }
        threadLocalTx.get().getElementPropertyRollback().add(elementPropertyRollback);
    }

    public void afterCommit(AfterCommit afterCommitFunction) {
        this.afterCommitFunction = afterCommitFunction;
    }

    public void afterRollback(AfterRollback afterCommitFunction) {
        this.afterRollbackFunction = afterCommitFunction;
    }

    @Override
    public boolean isOpen() {
        return threadLocalTx.get() != null;
    }

    SqlgVertex putVertexIfAbsent(SqlgGraph sqlgGraph, RecordId recordId) {
        return this.threadLocalTx.get().putVertexIfAbsent(sqlgGraph, recordId);
    }

    //Called for vertices that exist but are not yet in the transaction cache
    SqlgVertex putVertexIfAbsent(SqlgVertex sqlgVertex) {
        return this.threadLocalTx.get().putVertexIfAbsent(sqlgVertex);
    }

    //Called for new vertices
    void add(SqlgVertex sqlgVertex) {
        this.threadLocalTx.get().add(sqlgVertex);
    }
}
