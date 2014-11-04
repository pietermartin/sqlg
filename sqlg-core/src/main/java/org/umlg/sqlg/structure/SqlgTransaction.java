package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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
    private SqlgGraph sqlgGraph;
    private AfterCommit afterCommitFunction;
    private AfterRollback afterRollbackFunction;

    //true if a schema modification statement has been executed.
    //it is important to know this as schema modification creates exclusive locks.
    //In particular it locks querying the schema itself.
    private boolean schemaModification = false;

    protected final ThreadLocal<TransactionCache> threadLocalTx = new ThreadLocal<TransactionCache>() {
        protected TransactionCache initialValue() {
            return null;
        }
    };


    SqlgTransaction(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;

        // auto transaction behavior
        readWriteConsumer = READ_WRITE_BEHAVIOR.AUTO;

        // commit on close
        closeConsumer = CLOSE_BEHAVIOR.COMMIT;
    }

    public void batchModeOn() {
        if (this.sqlgGraph.features().supportsBatchMode()) {
            if (isOpen()) {
                throw new IllegalStateException("A transaction is already in progress. First commit or rollback before enabling batch mode.");
            }
            readWrite();
            threadLocalTx.get().getBatchManager().batchModeOn();
        } else {
            throw new IllegalStateException("Batch mode not supported!");
        }
    }

    public boolean isInBatchMode() {
        return threadLocalTx.get() != null && threadLocalTx.get().getBatchManager().isBatchModeOn();
    }

    public BatchManager getBatchManager() {
        return threadLocalTx.get().getBatchManager();
    }

    public Connection getConnection() {
        if (!isOpen()) {
            readWrite();
        }
        return this.threadLocalTx.get().getConnection();
    }

    @Override
    public void open() {
        if (isOpen())
            throw Transaction.Exceptions.transactionAlreadyOpen();
        else {
            try {
                Connection connection = SqlgDataSource.INSTANCE.get(this.sqlgGraph.getJdbcUrl()).getConnection();
                connection.setAutoCommit(false);
                threadLocalTx.set(TransactionCache.of(connection, new ArrayList<>(), new BatchManager(this.sqlgGraph, this.sqlgGraph.getSqlDialect())));
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
            if (this.threadLocalTx.get().getBatchManager().isBatchModeOn()) {
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
            this.schemaModification = false;
            if (this.threadLocalTx.get() != null) {
                this.threadLocalTx.get().clear();
                this.threadLocalTx.remove();
            }
        }
    }

    @Override
    public void rollback() {
        if (!isOpen())
            return;
        try {
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
            this.schemaModification = false;
            if (threadLocalTx.get() != null) {
                threadLocalTx.get().clear();
                threadLocalTx.remove();
            }
        }
    }

    public Map<SchemaTable, Pair<Long, Long>> batchCommit() {
        if (!threadLocalTx.get().getBatchManager().isBatchModeOn())
            throw new IllegalStateException("Must be in batch mode to batchCommit!");

        if (!isOpen())
            return Collections.emptyMap();

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
            this.schemaModification = false;
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
    public <R> Workload<R> submit(final Function<Graph, R> work) {
        return new Workload<>(this.sqlgGraph, work);
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

    SqlgVertex putVertexIfAbsent(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        return this.threadLocalTx.get().putVertexIfAbsent(sqlgGraph, id, schema, table);
    }

    //Called for vertices that exist but are not yet in the transaction cache
    SqlgVertex putVertexIfAbsent(SqlgVertex sqlgVertex) {
        return this.threadLocalTx.get().putVertexIfAbsent(sqlgVertex);
    }

    //Called for new vertices
    void add(SqlgVertex sqlgVertex) {
        this.threadLocalTx.get().add(sqlgVertex);
    }

    boolean isSchemaModification() {
        return schemaModification;
    }

    void setSchemaModification(boolean schemaModification) {
        this.schemaModification = schemaModification;
    }
}
