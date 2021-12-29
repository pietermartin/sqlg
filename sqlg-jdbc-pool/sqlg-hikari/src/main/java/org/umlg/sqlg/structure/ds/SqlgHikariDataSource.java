package org.umlg.sqlg.structure.ds;

import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;

import javax.sql.DataSource;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Date: 2014/07/12
 * Time: 7:00 AM
 */
@SuppressWarnings({"unused"})
public final class SqlgHikariDataSource implements SqlgDataSource {

    private static final Logger logger = LoggerFactory.getLogger(SqlgHikariDataSource.class);

    private final HikariConfig config = new HikariConfig();
    private HikariDataSource dss;
    private final String jdbcUrl;
    private final SqlDialect sqlDialect;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static SqlgHikariDataSource create(final Configuration configuration) {
        Preconditions.checkState(configuration.containsKey(SqlgGraph.JDBC_URL));
        Preconditions.checkState(configuration.containsKey("jdbc.username"));
        Preconditions.checkState(configuration.containsKey("jdbc.password"));

        String jdbcUrl = configuration.getString(SqlgGraph.JDBC_URL);
        SqlgPlugin sqlgPlugin = SqlgPlugin.load(jdbcUrl);
        SqlDialect sqlDialect = sqlgPlugin.instantiateDialect();

        String driver = sqlgPlugin.getDriverFor(jdbcUrl);
        String username = configuration.getString("jdbc.username");
        String password = configuration.getString("jdbc.password");

        Properties props = new Properties();
        if (!StringUtils.isEmpty(username)) {
            props.setProperty("dataSource.user", username);
        }
        if (!StringUtils.isEmpty(password)) {
            props.setProperty("dataSource.password", password);
        }
        Iterator<String> keyIterator = configuration.getKeys();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.startsWith("dataSource")) {
                props.setProperty(key.substring("dataSource.".length()), configuration.getProperty(key).toString());
            }
        }
        HikariConfig config = new HikariConfig(props);
        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName(driver);
        HikariDataSource hikariDataSource = new HikariDataSource(config);
        return new SqlgHikariDataSource(jdbcUrl, hikariDataSource, sqlDialect);
    }

    private SqlgHikariDataSource(String jdbcUrl, HikariDataSource dss, SqlDialect sqlDialect) {
        this.dss = dss;
        this.jdbcUrl = jdbcUrl;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public DataSource getDatasource() {
        return this.dss;
    }

    @Override
    public SqlDialect getDialect() {
        return this.sqlDialect;
    }

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }
        try {
            int numBusyConnections = dss.getHikariPoolMXBean().getActiveConnections();
            if (numBusyConnections > 0) {
                logger.debug("Open connection on calling close. " + numBusyConnections);
            }
        } finally {
            this.dss.close();
            this.dss = null;
        }
    }

    /**
     * This is only invoked for Postgresql on ddl statements.
     * It will force the pool to close all connections which in turn will deallocate server side prepared statements.
     */
    @Override
    public void softResetPool() {
        //noop
    }

    @Override
    public String getPoolStatsAsJson() {
        try {
            return "[" +
                    "{\"jdbcUrl\":\"" + jdbcUrl + "\"," +
                    "\"jndi\": false," +
                    "\"numMaxPoolSize\":\"" + dss.getMaximumPoolSize() + "\"," +
                    "}" +
                    "]";
        } catch (Exception e) {
            throw new IllegalStateException("Json generation failed", e);
        }
    }

    @Override
    public boolean isHikari() {
        return true;
    }
}
