package org.umlg.sqlg;

import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Lukas Krejci
 * @since 1.2.0
 */
public interface SqlgPlugin {
    /**
     * Returns the fully qualified class name of the JDBC driver to use for given connection URL or null if this
     * dialect doesn't know how to handle that URL.
     *
     * @param connectionUrl the JDBC URL of the database to connect to
     * @return the FQCN of the driver on null.
     */
    String getDriverFor(String connectionUrl);

    /**
     * Returns true if this dialect can handle a connection to a database identified by the provided metadata, false
     * otherwise.
     *
     * <p>Note that this method is only used when Sqlg is provided the connection from JNDI and thus the connection and
     * pooling is handled externally.
     *
     * @param metaData the metadata identifying the database being connected to.
     * @return true if this dialect can work on this connection, false otherwise
     */
    boolean canWorkWith(DatabaseMetaData metaData) throws SQLException;

    /**
     * Instantiates the dialect based on the provided configuration. This only gets called if
     * {@link #getDriverFor(String)} returns non-null class name or {@link #canWorkWith(DatabaseMetaData)} returns true.
     *
     * @return the dialect to use, never null
     */
    SqlDialect instantiateDialect();
}
