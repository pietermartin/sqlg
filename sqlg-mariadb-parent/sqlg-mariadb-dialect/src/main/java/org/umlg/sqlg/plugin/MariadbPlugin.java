package org.umlg.sqlg.plugin;

import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.sql.dialect.impl.MariadbDialect;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 *         Date: 2017/07/07
 */
public class MariadbPlugin implements SqlgPlugin {

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return metaData.getDatabaseProductName().equalsIgnoreCase("mariadb");
    }

    @Override
    public String getDriverFor(String connectionUrl) {
        return connectionUrl.startsWith("jdbc:mariadb") ? "org.mariadb.jdbc.Driver" : null;
    }

    @Override
    public SqlDialect instantiateDialect() {
        return new MariadbDialect();
    }
}
