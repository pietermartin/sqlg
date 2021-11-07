package org.umlg.sqlg.structure;

import org.apache.commons.configuration2.Configuration;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import javax.sql.DataSource;

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

    static boolean isPostgres(Configuration configuration) {
        return configuration.getString("jdbc.url").contains("postgresql");
    }

    static boolean isMsSqlServer(Configuration configuration) {
        return configuration.getString("jdbc.url").contains("sqlserver");
    }

    static boolean isHsqldb(Configuration configuration) {
        return configuration.getString("jdbc.url").contains("hsqldb");
    }

    static boolean isH2(Configuration configuration) {
        return configuration.getString("jdbc.url").contains("h2");
    }

    static boolean isMariaDb(Configuration configuration) {
        return configuration.getString("jdbc.url").contains("mariadb");
    }

    static boolean isMysql(Configuration configuration) {
        return configuration.getString("jdbc.url").contains("mysql");
    }

}
