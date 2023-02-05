package org.umlg.sqlg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.PostgresDialect;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Lukas Krejci
 * @since 1.2.0
 */
public class PostgresPlugin implements SqlgPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresPlugin.class);

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return metaData.getDatabaseProductName().toLowerCase().contains("postgres");
    }

    @Override
    public String getDriverFor(String connectionUrl) {
        return connectionUrl.startsWith("jdbc:postgresql") ? "org.postgresql.Driver" : null;
    }

    @Override
    public SqlDialect instantiateDialect() {
        return new PostgresDialect();
    }

    /**
     * Append 'autosave=conservative' to the jdbc url. This will ensure that the driver will take care of stale prepared statements.
     * refer to <a href="https://github.com/pgjdbc/pgjdbc/pull/451">pgjdbc issue 451</a>
     *
     * @param jdbcUrl The jdbc url
     * @return The jdbc url with autosave=conservative appended.
     */
    @Override
    public String manageJdbcUrl(String jdbcUrl) {
        if (jdbcUrl.contains("autosave=conservative")) {
            return jdbcUrl;
        } else {
            if (jdbcUrl.contains("autosave=")) {
                LOGGER.warn("Postgres jdbc url must contain 'autosave=conservative' to prevent 'ERROR:  cached plan must not change result type' errors from postgrseql.\nGiven jdbc url '{}' already contains 'autosave'", jdbcUrl);
                return jdbcUrl;
            } else {
                if (jdbcUrl.contains("?")) {
                    return jdbcUrl + "&autosave=conservative";
                } else {
                    return jdbcUrl + "?autosave=conservative";
                }
            }
        }
    }
}
