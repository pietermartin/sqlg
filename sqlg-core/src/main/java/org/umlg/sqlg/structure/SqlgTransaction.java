package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class is a singleton. Instantiated and owned by SqlG.
 * It manages the opening, commit, rollback and close of the java.sql.Connection in a threadvar.
 * Date: 2014/07/12
 * Time: 2:18 PM
 */
public class SqlgTransaction implements Transaction {

    private Consumer<Transaction> readWriteConsumer;
    private Consumer<Transaction> closeConsumer;
    private SqlG sqlG;
    private AfterCommit afterCommitFunction;
    private AfterRollback afterRollbackFunction;
//    private BatchManager batchManager;
//    private boolean inBatchMode = false;
    private Logger logger = LoggerFactory.getLogger(SqlgTransaction.class.getName());

    protected final ThreadLocal<Triple<Connection, List<ElementPropertyRollback>, BatchManager>> threadLocalTx = new ThreadLocal<Triple<Connection, List<ElementPropertyRollback>, BatchManager>>() {
        protected Triple<Connection, List<ElementPropertyRollback>, BatchManager> initialValue() {
            return null;
        }
    };
    protected final ThreadLocal<Connection> threadLocalReadOnlyTx = new ThreadLocal<Connection>() {
        protected Connection initialValue() {
            return null;
        }
    };

    SqlgTransaction(SqlG sqlG) {
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
            readWrite();
            threadLocalTx.get().getRight().batchModeOn();
        } else {
            logger.warn("Batch mode not supported! Continuing in normal mode.");
        }
    }

    public boolean isInBatchMode() {
        return threadLocalTx.get().getRight().isBatchModeOn();
    }

    public BatchManager getBatchManager() {
        return threadLocalTx.get().getRight();
    }

    public Connection getConnection() {
        if (!isOpen()) {
            readWrite();
        }
        return this.threadLocalTx.get().getLeft();
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
                threadLocalTx.set(Triple.of(connection, new ArrayList<>(), new BatchManager(this.sqlG, this.sqlG.getSqlDialect())));
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
            if (this.threadLocalTx.get().getRight().isBatchModeOn()) {
                this.threadLocalTx.get().getRight().flush();
            }
            Connection connection = threadLocalTx.get().getLeft();
            connection.commit();
            connection.setAutoCommit(true);
            if (this.afterCommitFunction != null) {
                this.afterCommitFunction.doAfterCommit();
            }
            this.threadLocalTx.get().getRight().clear();
        } catch (Exception e) {
            this.rollback();
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                //this is null after a rollback.
                //might occur if sqlg has a bug and there is a SqlException
                if (threadLocalTx.get() != null) {
                    threadLocalTx.get().getLeft().close();
                    threadLocalTx.get().getMiddle().clear();
                    threadLocalTx.get().getRight().clear();
                    threadLocalTx.remove();
                }
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
            threadLocalTx.get().getLeft().rollback();
            if (this.afterRollbackFunction != null) {
                this.afterRollbackFunction.doAfterRollback();
            }
            for (ElementPropertyRollback elementPropertyRollback : threadLocalTx.get().getMiddle()) {
                elementPropertyRollback.clearProperties();
            }
            threadLocalTx.get().getRight().clear();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                threadLocalTx.get().getLeft().close();
                threadLocalTx.get().getMiddle().clear();
                threadLocalTx.get().getRight().clear();
                threadLocalTx.remove();
            } catch (SQLException e) {
                threadLocalTx.remove();
                throw new RuntimeException(e);
            }
        }
    }

    public Map<SchemaTable, Pair<Long, Long>> batchCommit() {
        if (!threadLocalTx.get().getRight().isBatchModeOn())
            throw new IllegalStateException("Must be in batch mode to batchCommit!");

        if (!isOpen())
            return Collections.emptyMap();

        try {
            Map<SchemaTable, Pair<Long, Long>> verticesRange = threadLocalTx.get().getRight().flush();
            Connection connection = threadLocalTx.get().getLeft();
            connection.commit();
            connection.setAutoCommit(true);
            if (this.afterCommitFunction != null) {
                this.afterCommitFunction.doAfterCommit();
            }
            threadLocalTx.get().getRight().clear();
            return verticesRange;
        } catch (Exception e) {
            this.rollback();
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                //this is null after a rollback.
                //might occur if sqlg has a bug and there is a SqlException
                if (threadLocalTx.get() != null) {
                    threadLocalTx.get().getLeft().close();
                    threadLocalTx.get().getMiddle().clear();
                    threadLocalTx.get().getRight().clear();
                    threadLocalTx.remove();
                }
            } catch (SQLException e) {
                threadLocalTx.remove();
                throw new RuntimeException(e);
            }
        }
    }

    public void addElementPropertyRollback(ElementPropertyRollback elementPropertyRollback) {
        if (!isOpen()) {
            throw new IllegalStateException("A transaction must be in progress to add a elementPropertyRollback function!");
        }
        threadLocalTx.get().getMiddle().add(elementPropertyRollback);
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
