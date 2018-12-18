package org.umlg.sqlg.structure.ds;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ServiceLoader;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * Created by petercipov on 27/02/2017.
 */
public class JNDIDataSource implements SqlgDataSource {

    private static final Logger logger = LoggerFactory.getLogger(JNDIDataSource.class);
    private static final String JNDI_PREFIX = "jndi:";

    private final DataSource dataSource;
    private final String jdbcUrl;
    private final SqlDialect sqlDialect;

    public static boolean isJNDIUrl(String url) {
        return url.startsWith(JNDI_PREFIX);
    }

    public static SqlgDataSource create(Configuration configuration) throws NamingException, SQLException {
        String url = configuration.getString(SqlgGraph.JDBC_URL);
        if (! isJNDIUrl(url)) {
            throw new IllegalArgumentException("Creating JNDI ds from invalid url: "+url);
        }

        String jndiName = url.substring(JNDI_PREFIX.length());

        InitialContext ctx = new InitialContext();
        DataSource ds = (DataSource) ctx.lookup(jndiName);

        SqlDialect sqlDialect;
        try (Connection conn = ds.getConnection()) {
            SqlgPlugin p = findSqlgPlugin(conn.getMetaData());
            if (p == null) {
                throw new IllegalStateException("Could not find suitable sqlg plugin for the JDBC URL: " + url);
            }

            sqlDialect = p.instantiateDialect();
        }

        return new JNDIDataSource(url, ds, sqlDialect);
    }

    private static SqlgPlugin findSqlgPlugin(DatabaseMetaData metadata) throws SQLException {
        for (SqlgPlugin p : ServiceLoader.load(SqlgPlugin.class, JNDIDataSource.class.getClassLoader())) {
            logger.info("found plugin for SqlgPlugin.class");
            if (p.canWorkWith(metadata)) {
                return p;
            } else {
                logger.info("can not work with SqlgPlugin.class");
            }
        }

        return null;
    }

    private JNDIDataSource(String jdbcUrl, DataSource dataSource, SqlDialect sqlDialect) {
        this.dataSource = dataSource;
        this.jdbcUrl = jdbcUrl;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public DataSource getDatasource() {
        return dataSource;
    }

    @Override
    public SqlDialect getDialect() {
        return sqlDialect;
    }

    @Override
    public void close() {
    }

    @Override
    public String getPoolStatsAsJson() {
        try {
            StringBuilder json = new StringBuilder();
            json.append("[");

            json.append("{\"jdbcUrl\":\"").append(jdbcUrl).append("\",");
            json.append("\"jndi\": true");
            json.append("}");


            json.append("]");
            return json.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Json generation failed", e);
        }
    }
}
