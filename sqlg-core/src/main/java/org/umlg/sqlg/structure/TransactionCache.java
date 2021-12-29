package org.umlg.sqlg.structure;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * A transaction scoped cache.
 * Date: 2014/10/04
 * Time: 3:20 PM
 */
class TransactionCache {

    private final Connection connection;
    private final Map<ElementPropertyRollback, Object> elementPropertyRollbackFunctions = new WeakHashMap<>();
    private BatchManager batchManager;
    private boolean writeTransaction;

    /**
     * are query result processed lazily or not?
     */
    private boolean lazyQueries;

    /**
     * default fetch size
     */
    private Integer fetchSize = null;


    static TransactionCache of(Connection connection, BatchManager batchManager, boolean lazyQueries) {
        return new TransactionCache(connection, batchManager, lazyQueries);
    }

    static TransactionCache of(Connection connection, boolean lazyQueries) {
        return new TransactionCache(connection, lazyQueries);
    }

    private TransactionCache(
            Connection connection,
            boolean lazyQueries) {

        this.connection = connection;
        this.lazyQueries = lazyQueries;
    }

    private TransactionCache(
            Connection connection,
            BatchManager batchManager,
            boolean lazyQueries) {

        this(connection, lazyQueries);
        this.batchManager = batchManager;
    }

    Connection getConnection() {
        return this.connection;
    }

    public boolean isWriteTransaction() {
        return writeTransaction;
    }

    public void setWriteTransaction(boolean writeTransaction) {
        this.writeTransaction = writeTransaction;
    }

    Map<ElementPropertyRollback, Object> getElementPropertyRollback() {
        return this.elementPropertyRollbackFunctions;
    }

    BatchManager getBatchManager() {
        return this.batchManager;
    }

    void clear() {
        this.elementPropertyRollbackFunctions.clear();
        if (this.batchManager != null) {
            this.batchManager.clear();
        }
        try {
            if (!this.connection.isClosed()) {
                this.connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * are we reading the SQL query results lazily?
     *
     * @return true if we are processing the results lazily, false otherwise
     */
    public boolean isLazyQueries() {
        return lazyQueries;
    }

    /**
     * set the laziness on query result reading
     *
     * @param lazyQueries set the queries to being lazy.
     */
    public void setLazyQueries(boolean lazyQueries) {
        this.lazyQueries = lazyQueries;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

}
