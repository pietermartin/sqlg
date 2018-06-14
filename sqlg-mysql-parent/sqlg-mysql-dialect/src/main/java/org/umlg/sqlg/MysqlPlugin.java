package org.umlg.sqlg;

import org.umlg.sqlg.sql.dialect.MysqlDialect;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * @author Vasili Pispanen
 *         Date: 2018/05/22
 */
public class MysqlPlugin implements SqlgPlugin{

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return metaData.getDatabaseProductName().toLowerCase().contains("mysql");
    }

    @Override
    public String getDriverFor(String connectionUrl) {
        return connectionUrl.startsWith("jdbc:mysql") ? "com.mysql.cj.jdbc.Driver" : null;
    }

    @Override
    public SqlDialect instantiateDialect() {
        return new MysqlDialect();
    }
}
