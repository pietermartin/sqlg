package org.umlg.sqlg;

import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ServiceLoader;

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
    
    /**
     * Loads the plugin to use for the provided JDBC URL.
     * 
     * @param connectionUrl the JDBC URL of the database to connect to
     * @return the plugin to use, never null
     * @throws IllegalStateException if no suitable Sqlg plugin could be found
     */
    public static SqlgPlugin load(String connectionUrl) {
    	for (SqlgPlugin p : ServiceLoader.load(SqlgPlugin.class, SqlgPlugin.class.getClassLoader())) {
            if (p.getDriverFor(connectionUrl) != null) {
                return p;
            }
        }
    	throw new IllegalStateException("Could not find suitable Sqlg plugin for the JDBC URL: " + connectionUrl);
    }
    
    /**
     * Loads the plugin to use for the provided database meta data.
     * 
     * @param metaData the JDBC meta data from an established database connection
     * @return the plugin to use, never null
     * @throws IllegalStateException if no suitable Sqlg plugin could be found
     */
    public static SqlgPlugin load(DatabaseMetaData metaData) throws SQLException {
    	for (SqlgPlugin p : ServiceLoader.load(SqlgPlugin.class, SqlgPlugin.class.getClassLoader())) {
            if (p.canWorkWith(metaData)) {
                return p;
            }
        }
    	throw new IllegalStateException("Could not find suitable Sqlg plugin for the database: " + metaData.getDatabaseProductName());
    }
}
