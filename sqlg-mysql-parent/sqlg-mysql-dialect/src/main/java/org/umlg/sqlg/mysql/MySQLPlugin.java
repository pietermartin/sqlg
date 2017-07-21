package org.umlg.sqlg.mysql;

import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.umlg.sqlg.SqlgPlugin;

/**
 * @author Kevin Schmidt
 * @since 1.3.2
 */
public class MySQLPlugin implements SqlgPlugin {
    @Override
    public String getDriverFor(String connectionUrl) {
        if (connectionUrl.startsWith("jdbc:mysql")) {
            return "com.mysql.jdbc.Driver";
        } else {
            return null;
        }
    }

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return "MySQL".equals(metaData.getDatabaseProductName());
    }

    @Override
    public SqlDialect instantiateDialect() {
        return new MySQLDialect();
    }
}
