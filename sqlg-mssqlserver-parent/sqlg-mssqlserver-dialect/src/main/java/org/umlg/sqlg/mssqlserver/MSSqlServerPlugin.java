package org.umlg.sqlg.mssqlserver;

import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.umlg.sqlg.SqlgPlugin;

/**
 * @author Kevin Schmidt
 * @since 1.3.2
 */
public class MSSqlServerPlugin implements SqlgPlugin {
    @Override
    public String getDriverFor(String connectionUrl) {
        if (connectionUrl.startsWith("jdbc:sqlserver")) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else {
            return null;
        }
    }

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return "Microsoft SQL Server".equals(metaData.getDatabaseProductName());
    }

    @Override
    public SqlDialect instantiateDialect() {
        return new MSSqlServerDialect();
    }
}
