package org.umlg.sqlg;

import org.umlg.sqlg.sql.dialect.PostgresDialect;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Lukas Krejci
 * @since 1.2.0
 */
public class PostgresPlugin implements SqlgPlugin {

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return metaData.getDatabaseProductName().toLowerCase().contains("postgres");
    }

    @Override
    public String getDriverFor(String connectionUrl) {
        return connectionUrl.startsWith("jdbc:postgresql") ? "org.postgresql.xa.PGXADataSource" : null;
    }

    @Override
    public SqlDialect instantiateDialect() {
        return new PostgresDialect();
    }

}
