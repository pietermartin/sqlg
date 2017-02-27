package org.umlg.sqlg.structure.c3p0;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgDataSourceFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

/**
 * Date: 2014/07/12
 * Time: 7:00 AM
 */
public class C3P0DataSource implements SqlgDataSourceFactory.SqlgDataSource{

    private static Logger logger = LoggerFactory.getLogger(C3P0DataSource.class.getName());

    private final Map<String, DataSource> dss;

    C3P0DataSource(Map<String, DataSource> dss) {
        this.dss = dss;
    }

    @Override
    public final DataSource get(String jdbcUrl) {
        return this.dss.get(jdbcUrl);
    }

    @Override
    public void close(String jdbcUrl) {
        DataSource remove = this.dss.remove(jdbcUrl);
        ComboPooledDataSource managed = remove != null && (remove instanceof ComboPooledDataSource)
                ? (ComboPooledDataSource) remove
                : null;

        try {
            if (managed != null) {
                int numBusyConnections = managed.getNumBusyConnections();
                if (numBusyConnections > 0) {
                    logger.debug("Open connection on calling close. " + numBusyConnections);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not close connection " +jdbcUrl, e);
        } finally {
            if (managed != null)
                managed.close();
        }
    }

    @Override
    public String getPoolStatsAsJson() {
        try {
            StringBuilder json = new StringBuilder();
            json.append("[");
            int count = 1;
            for (Map.Entry<String, DataSource> entry : this.dss.entrySet()) {
                json.append("{\"jdbcUrl\":\"").append(entry.getKey()).append("\",");

                if (entry.getValue() instanceof ComboPooledDataSource) {
                    ComboPooledDataSource comboPooledDataSource = (ComboPooledDataSource) entry.getValue();
                    json.append("\"jndi\": false,");
                    json.append("\"numConnections\":\"")
                            .append(String.valueOf(comboPooledDataSource.getNumConnections())).append("\",");
                    json.append("\"numBusyConnections\":\"")
                            .append(String.valueOf(comboPooledDataSource.getNumConnections())).append("\",");
                    json.append("\"numIdleConnections\":\"")
                            .append(String.valueOf(comboPooledDataSource.getNumConnections())).append("\",");
                    json.append("\"numUnclosedOrphanedConnections\":\"")
                            .append(String.valueOf(comboPooledDataSource.getNumConnections())).append("\",");
                    json.append("\"numMinPoolSize\":\"").append(String.valueOf(comboPooledDataSource.getMinPoolSize()))
                            .append("\",");
                    json.append("\"numMaxPoolSize\":\"").append(String.valueOf(comboPooledDataSource.getMaxPoolSize()))
                            .append("\",");
                    json.append("\"numMaxIdleTime\":\"").append(String.valueOf(comboPooledDataSource.getMaxIdleTime()))
                            .append("\"");
                } else {
                    json.append("\"jndi\": true");
                }
                json.append("}");
                if (count++ < this.dss.size()) {
                    json.append(",");
                }
            }
            json.append("]");
            return json.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
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
