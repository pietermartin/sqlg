package org.umlg.sqlg;

import org.umlg.sqlg.sql.dialect.MariadbDialect;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/07/07
 */
public class MariadbPlugin implements SqlgPlugin{

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return metaData.getDatabaseProductName().toLowerCase().contains("mysql");
    }

    @Override
    public String getDriverFor(String connectionUrl) {
        return connectionUrl.startsWith("jdbc:mysql") ? "com.mysql.jc.jdbc.Driver" : null;
    }

    @Override
    public SqlDialect instantiateDialect() {
        return new MariadbDialect();
    }
}
