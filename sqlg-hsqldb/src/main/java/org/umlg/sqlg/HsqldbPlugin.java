package org.umlg.sqlg;

import org.apache.commons.configuration.Configuration;
import org.umlg.sqlg.sql.dialect.HsqldbDialect;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Lukas Krejci
 * @since 1.2.0
 */
public class HsqldbPlugin implements SqlgPlugin {

    @Override
    public String getDriverFor(String connectionUrl) {
        return connectionUrl.startsWith("jdbc:hsqldb") ? "org.hsqldb.jdbc.JDBCDriver" : null;
    }

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return metaData.getDatabaseProductName().equals("HSQL Database Engine");
    }

    @Override
    public SqlDialect instantiateDialect(Configuration configuration) {
        return new HsqldbDialect(configuration);
    }
}
