package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Date: 2014/07/12
 * Time: 2:18 PM
 */
public class SqlgTransaction implements Transaction {

    private Consumer<Transaction> readWriteConsumer;
    private Consumer<Transaction> closeConsumer;
    private SqlG sqlG;
    private AfterCommit afterCommitFunction;
    private AfterRollback afterRollbackFunction;
    private BatchManager batchManager;
    private boolean inBatchMode = false;
    private Logger logger = LoggerFactory.getLogger(SqlgTransaction.class.getName());

    protected final ThreadLocal<Connection> threadLocalTx = new ThreadLocal<Connection>() {
        protected Connection initialValue() {
            return null;
        }
    };
    protected final ThreadLocal<Connection> threadLocalReadOnlyTx = new ThreadLocal<Connection>() {
        protected Connection initialValue() {
            return null;
        }
    };

    public SqlgTransaction(SqlG sqlG) {
        this.sqlG = sqlG;

        // auto transaction behavior
        readWriteConsumer = READ_WRITE_BEHAVIOR.AUTO;

        // commit on close
        closeConsumer = CLOSE_BEHAVIOR.COMMIT;
    }

    public void batchModeOn() {
        if (this.sqlG.features().supportsBatchMode()) {
            if (isOpen()) {
                throw new IllegalStateException("A transaction is already in progress. First commit or rollback before enabling batch mode.");
            }
            this.inBatchMode = true;
            this.batchManager = new BatchManager(this.sqlG, this.sqlG.getSqlDialect());
        } else {
            logger.warn("Batch mode not supported! Continuing in normal mode.");
        }
    }

    public boolean isInBatchMode() {
        return this.inBatchMode;
    }

    public BatchManager getBatchManager() {
        return batchManager;
    }

    public Connection getConnection() {
        return this.threadLocalTx.get();
    }

    public Connection getReadOnlyConnection() {
        return this.threadLocalReadOnlyTx.get();
    }

    @Override
    public void open() {
        if (isOpen())
            throw Transaction.Exceptions.transactionAlreadyOpen();
        else {
            try {
                Connection connection = SqlgDataSource.INSTANCE.get(this.sqlG.getJdbcUrl()).getConnection();
                connection.setAutoCommit(false);
                threadLocalTx.set(connection);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void commit() {
        if (!isOpen())
            return;

        try {
            try {
                if (this.inBatchMode) {
                    this.batchManager.flush();
                }
                Connection connection = threadLocalTx.get();
                connection.commit();
                connection.setAutoCommit(true);
                if (this.afterCommitFunction != null) {
                    this.afterCommitFunction.doAfterCommit();
                }
                this.inBatchMode = false;
                if (this.batchManager != null) {
                    this.batchManager.clear();
                    this.batchManager = null;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                threadLocalTx.get().close();
                threadLocalTx.remove();
            } catch (SQLException e) {
                threadLocalTx.remove();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void rollback() {
        if (!isOpen())
            return;
        try {
            threadLocalTx.get().rollback();
            if (this.afterRollbackFunction != null) {
                this.afterRollbackFunction.doAfterRollback();
            }
            this.inBatchMode = false;
            if (this.batchManager != null) {
                this.batchManager.clear();
                this.batchManager = null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                threadLocalTx.get().close();
                threadLocalTx.remove();
            } catch (SQLException e) {
                threadLocalTx.remove();
                throw new RuntimeException(e);
            }
        }
    }

    public void afterCommit(AfterCommit afterCommitFunction) {
        this.afterCommitFunction = afterCommitFunction;
    }

    public void afterRollback(AfterRollback afterCommitFunction) {
        this.afterRollbackFunction = afterCommitFunction;
    }

    @Override
    public <R> Workload<R> submit(final Function<Graph, R> work) {
        return new Workload<>(this.sqlG, work);
    }

    @Override
    public <G extends Graph> G create() {
        throw Transaction.Exceptions.threadedTransactionsNotSupported();
    }

    @Override
    public boolean isOpen() {
        return (threadLocalTx.get() != null);
    }

    @Override
    public void readWrite() {
        this.readWriteConsumer.accept(this);
    }

    @Override
    public void close() {
        this.closeConsumer.accept(this);
    }

    @Override
    public Transaction onReadWrite(final Consumer<Transaction> consumer) {
        this.readWriteConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull);
        return this;
    }

    @Override
    public Transaction onClose(final Consumer<Transaction> consumer) {
        this.closeConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onCloseBehaviorCannotBeNull);
        return this;
    }

}
