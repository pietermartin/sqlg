package org.umlg.sqlg.structure;

import javax.sql.DataSource;

import org.umlg.sqlg.sql.dialect.SqlDialect;

public interface SqlgDataSource {
    DataSource getDatasource();

    SqlDialect getDialect();

    void close();

    /**
     * This is needed for Postgresql where after a ddl statement all prepared statements need to be 'DEALLOCATED'
     * This does not completely prevent the issue as currently active connections may still execute a prepared statement
     * that Postgresql will reject with 'cached plan must not change result type'.
     */
    default void softResetPool() {
        //Do nothing
    }

    String getPoolStatsAsJson();
}
