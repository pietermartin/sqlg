package org.umlg.sqlgraph.sql.impl;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Date: 2014/07/12
 * Time: 2:18 PM
 */
public class SqlGraphTransaction implements Transaction {

    private Consumer<Transaction> readWriteConsumer;
    private Consumer<Transaction> closeConsumer;
    private SqlGraph sqlGraph;

    protected final ThreadLocal<Connection> threadLocalTx = new ThreadLocal<Connection>() {
        protected Connection initialValue() {
            return null;
        }
    };

    public SqlGraphTransaction(SqlGraph sqlGraph) {
        this.sqlGraph = sqlGraph;

        // auto transaction behavior
        readWriteConsumer = READ_WRITE_BEHAVIOR.AUTO;

        // commit on close
        closeConsumer = CLOSE_BEHAVIOR.COMMIT;
    }

    public Connection getConnection() {
        return this.threadLocalTx.get();
    }

    @Override
    public void open() {
        if (isOpen())
            throw Transaction.Exceptions.transactionAlreadyOpen();
        else {
            try {
                Connection connection = SqlGraphDataSource.INSTANCE.get(this.sqlGraph.getJdbcUrl()).getConnection();
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
                threadLocalTx.get().commit();
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

    @Override
    public <R> Workload<R> submit(final Function<Graph, R> work) {
        return new Workload<>(this.sqlGraph, work);
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
