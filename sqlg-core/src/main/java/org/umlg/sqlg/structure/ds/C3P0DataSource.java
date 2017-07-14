package org.umlg.sqlg.structure.ds;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgDataSourceFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Date: 2014/07/12
 * Time: 7:00 AM
 */
public class C3P0DataSource implements SqlgDataSourceFactory.SqlgDataSource {

    private static Logger logger = LoggerFactory.getLogger(C3P0DataSource.class.getName());

    private final ComboPooledDataSource dss;
    private final String jdbcUrl;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    C3P0DataSource(String jdbcUrl, ComboPooledDataSource dss) {
        this.dss = dss;
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    public final DataSource getDatasource() {
        return this.dss;
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
