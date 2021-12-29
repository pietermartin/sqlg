package org.umlg.sqlg.structure.ds;

import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Date: 2014/07/12
 * Time: 7:00 AM
 */
@SuppressWarnings("unused")
public final class SqlgC3P0DataSource implements SqlgDataSource {

    private static final Logger logger = LoggerFactory.getLogger(SqlgC3P0DataSource.class);

    private ComboPooledDataSource dss;
    private final String jdbcUrl;
    private final SqlDialect sqlDialect;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static SqlgC3P0DataSource create(final Configuration configuration) throws Exception {
        Preconditions.checkState(configuration.containsKey(SqlgGraph.JDBC_URL));
        Preconditions.checkState(configuration.containsKey("jdbc.username"));
        Preconditions.checkState(configuration.containsKey("jdbc.password"));

        String jdbcUrl = configuration.getString(SqlgGraph.JDBC_URL);
        SqlgPlugin sqlgPlugin = SqlgPlugin.load(jdbcUrl);
        SqlDialect sqlDialect = sqlgPlugin.instantiateDialect();

        String driver = sqlgPlugin.getDriverFor(jdbcUrl);
        String username = configuration.getString("jdbc.username");
        String password = configuration.getString("jdbc.password");

        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setDriverClass(driver);
        comboPooledDataSource.setJdbcUrl(jdbcUrl);
        if (!StringUtils.isEmpty(username)) {
            comboPooledDataSource.setUser(username);
        }
        if (!StringUtils.isEmpty(password)) {
            comboPooledDataSource.setPassword(password);
        }
        //Only used for testing, under normal circumstances it can be set in c3p0.properties
        if (configuration.containsKey("c3p0.maxPoolSize")) {
            int maxPoolSize = configuration.getInt("c3p0.maxPoolSize");
            comboPooledDataSource.setMaxPoolSize(maxPoolSize);
        }
        comboPooledDataSource.setForceUseNamedDriverClass(true);
        if (SqlgDataSource.isPostgres(configuration) || SqlgDataSource.isHsqldb(configuration) || SqlgDataSource.isH2(configuration)) {
            comboPooledDataSource.setDataSourceName(jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1));
        } else if (SqlgDataSource.isMariaDb(configuration)) {
            comboPooledDataSource.setDataSourceName("mariadb");
        } else if (SqlgDataSource.isMysql(configuration)) {
            comboPooledDataSource.setDataSourceName("mysql");
        } else if (SqlgDataSource.isMsSqlServer(configuration)) {
            comboPooledDataSource.setDataSourceName(jdbcUrl.substring(jdbcUrl.lastIndexOf("databaseName=") + "databaseName=".length()));
        }
        return new SqlgC3P0DataSource(jdbcUrl, comboPooledDataSource, sqlDialect);
    }

    private SqlgC3P0DataSource(String jdbcUrl, ComboPooledDataSource dss, SqlDialect sqlDialect) {
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
            int numBusyConnections = dss.getNumBusyConnections();
            if (numBusyConnections > 0) {
                logger.debug("Open connection on calling close. " + numBusyConnections);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not close connection " + jdbcUrl, e);
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
        try {
            this.dss.softResetAllUsers();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getPoolStatsAsJson() {
        try {
            return "[" +
                    "{\"jdbcUrl\":\"" + jdbcUrl + "\"," +
                    "\"jndi\": false," +
                    "\"numConnections\":\"" + dss.getNumConnections() + "\"," +
                    "\"numBusyConnections\":\"" + dss.getNumConnections() + "\"," +
                    "\"numIdleConnections\":\"" + dss.getNumConnections() + "\"," +
                    "\"numUnclosedOrphanedConnections\":\"" + dss.getNumConnections() + "\"," +
                    "\"numMinPoolSize\":\"" + dss.getMinPoolSize() + "\"," +
                    "\"numMaxPoolSize\":\"" + dss.getMaxPoolSize() + "\"," +
                    "\"numMaxIdleTime\":\"" + dss.getMaxIdleTime() + "\"" +
                    "}" +
                    "]";
        } catch (Exception e) {
            throw new IllegalStateException("Json generation failed", e);
        }
    }

    @Override
    public boolean isC3p0() {
        return true;
    }
}
