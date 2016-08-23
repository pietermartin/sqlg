package org.umlg.sqlg;

import org.apache.commons.configuration.Configuration;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Lukas Krejci
 * @since 1.3.0
 */
public class H2Plugin implements SqlgPlugin {
    @Override
    public String getDriverFor(String connectionUrl) {
        if (connectionUrl.startsWith("jdbc:h2")) {
            return org.h2.Driver.class.getName();
        } else {
            return null;
        }
    }

    @Override
    public boolean canWorkWith(DatabaseMetaData metaData) throws SQLException {
        return "H2".equals(metaData.getDatabaseProductName());
    }

    @Override
    public SqlDialect instantiateDialect(Configuration configuration) {
        return new H2Dialect(configuration);
    }
}
