package org.umlg.sqlg.structure.ds;

import org.apache.commons.configuration2.Configuration;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by petercipov on 27/02/2017.
 */
public final class JNDIDataSource implements SqlgDataSource {

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
            sqlDialect = SqlgPlugin.load(conn.getMetaData()).instantiateDialect();
        }

        return new JNDIDataSource(url, ds, sqlDialect);
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
