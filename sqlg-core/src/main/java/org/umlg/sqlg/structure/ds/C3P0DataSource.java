package org.umlg.sqlg.structure.ds;

import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.SqlgPlugin;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Date: 2014/07/12
 * Time: 7:00 AM
 */
public class C3P0DataSource implements SqlgDataSource {

    private static final Logger logger = LoggerFactory.getLogger(C3P0DataSource.class);

    private final ComboPooledDataSource dss;
    private final String jdbcUrl;
    private final SqlDialect sqlDialect;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static C3P0DataSource create(final Configuration configuration) throws Exception {
        Preconditions.checkState(configuration.containsKey(SqlgGraph.JDBC_URL));
        Preconditions.checkState(configuration.containsKey("jdbc.username"));
        Preconditions.checkState(configuration.containsKey("jdbc.password"));

        String jdbcUrl = configuration.getString(SqlgGraph.JDBC_URL);
        SqlgPlugin p = findSqlgPlugin(jdbcUrl);
        if (p == null) {
            throw new IllegalStateException("Could not find suitable sqlg plugin for the JDBC URL: " + jdbcUrl);
        }

        SqlDialect sqlDialect = p.instantiateDialect();

        String driver = p.getDriverFor(jdbcUrl);
        String username = configuration.getString("jdbc.username");
        String password = configuration.getString("jdbc.password");

        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setDriverClass(driver);
        comboPooledDataSource.setJdbcUrl(jdbcUrl);
        comboPooledDataSource.setMaxPoolSize(configuration.getInt("maxPoolSize", 100));
        comboPooledDataSource.setMaxIdleTime(configuration.getInt("maxIdleTime", 3600));
        comboPooledDataSource.setForceUseNamedDriverClass(true);
        if (!StringUtils.isEmpty(username)) {
            comboPooledDataSource.setUser(username);
        }

        if (!StringUtils.isEmpty(password)) {
            comboPooledDataSource.setPassword(password);
        }

        return new C3P0DataSource(jdbcUrl, comboPooledDataSource, sqlDialect);
    }

    private static SqlgPlugin findSqlgPlugin(String connectionUri) {
        for (SqlgPlugin p : ServiceLoader.load(SqlgPlugin.class, C3P0DataSource.class.getClassLoader())) {
            if (p.getDriverFor(connectionUri) != null) {
                return p;
            }
        }

        return null;
    }

    private C3P0DataSource(String jdbcUrl, ComboPooledDataSource dss, SqlDialect sqlDialect) {
        this.dss = dss;
        this.jdbcUrl = jdbcUrl;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public final DataSource getDatasource() {
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
            dss.close();
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
            StringBuilder json = new StringBuilder();
            json.append("[");
            json.append("{\"jdbcUrl\":\"").append(jdbcUrl).append("\",");

            json.append("\"jndi\": false,");
            json.append("\"numConnections\":\"")
                    .append(String.valueOf(dss.getNumConnections())).append("\",");
            json.append("\"numBusyConnections\":\"")
                    .append(String.valueOf(dss.getNumConnections())).append("\",");
            json.append("\"numIdleConnections\":\"")
                    .append(String.valueOf(dss.getNumConnections())).append("\",");
            json.append("\"numUnclosedOrphanedConnections\":\"")
                    .append(String.valueOf(dss.getNumConnections())).append("\",");
            json.append("\"numMinPoolSize\":\"").append(String.valueOf(dss.getMinPoolSize()))
                    .append("\",");
            json.append("\"numMaxPoolSize\":\"").append(String.valueOf(dss.getMaxPoolSize()))
                    .append("\",");
            json.append("\"numMaxIdleTime\":\"").append(String.valueOf(dss.getMaxIdleTime()))
                    .append("\"");

            json.append("}");

            json.append("]");
            return json.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Json generation failed", e);
        }
    }

    /*
     final static Set<String> TO_STRING_IGNORE_PROPS = new HashSet<>(Arrays.asList(new String[]{
     "connection",
     "lastAcquisitionFailureDefaultUser",
     "lastCheckinFailureDefaultUser",
     "lastCheckoutFailureDefaultUser",
     "lastConnectionTestFailureDefaultUser",
     "lastIdleTestFailureDefaultUser",
     "logWriter",
     "loginTimeout",
     "numBusyConnections",
     "numBusyConnectionsAllUsers",
     "numBusyConnectionsDefaultUser",
     "numConnections",
     "numConnectionsAllUsers",
     "numConnectionsDefaultUser",
     "numFailedCheckinsDefaultUser",
     "numFailedCheckoutsDefaultUser",
     "numFailedIdleTestsDefaultUser",
     "numIdleConnections",
     "numIdleConnectionsAllUsers",
     "numThreadsAwaitingCheckoutDefaultUser",
     "numIdleConnectionsDefaultUser",
     "numUnclosedOrphanedConnections",
     "numUnclosedOrphanedConnectionsAllUsers",
     "numUnclosedOrphanedConnectionsDefaultUser",
     "numUserPools",
     "effectivePropertyCycleDefaultUser",
     "parentLogger",
     "startTimeMillisDefaultUser",
     "statementCacheNumCheckedOutDefaultUser",
     "statementCacheNumCheckedOutStatementsAllUsers",
     "statementCacheNumConnectionsWithCachedStatementsAllUsers",
     "statementCacheNumConnectionsWithCachedStatementsDefaultUser",
     "statementCacheNumStatementsAllUsers",
     "statementCacheNumStatementsDefaultUser",
     "statementDestroyerNumConnectionsInUseAllUsers",
     "statementDestroyerNumConnectionsWithDeferredDestroyStatementsAllUsers",
     "statementDestroyerNumDeferredDestroyStatementsAllUsers",
     "statementDestroyerNumConnectionsInUseDefaultUser",
     "statementDestroyerNumConnectionsWithDeferredDestroyStatementsDefaultUser",
     "statementDestroyerNumDeferredDestroyStatementsDefaultUser",
     "statementDestroyerNumThreads",
     "statementDestroyerNumActiveThreads",
     "statementDestroyerNumIdleThreads",
     "statementDestroyerNumTasksPending",
     "threadPoolSize",
     "threadPoolNumActiveThreads",
     "threadPoolNumIdleThreads",
     "threadPoolNumTasksPending",
     "threadPoolStackTraces",
     "threadPoolStatus",
     "overrideDefaultUser",
     "overrideDefaultPassword",
     "password",
     "reference",
     "upTimeMillisDefaultUser",
     "user",
     "userOverridesAsString",
     "allUsers",
     "connectionPoolDataSource",
     "propertyChangeListeners",
     "vetoableChangeListeners"
     }));
     */

}
