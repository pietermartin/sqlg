package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 7:00 AM
 */
public class SqlgDataSource {

    private static Logger logger = LoggerFactory.getLogger(SqlgGraph.class.getName());
    private Map<String, ComboPooledDataSource> cpdss = new HashMap<>();

    private SqlgDataSource() {
    }

    public final DataSource get(String jdbcUrl) {
        return this.cpdss.get(jdbcUrl);
    }

    public static SqlgDataSource setupDataSource(String driver, final Configuration configuration) throws PropertyVetoException {
        SqlgDataSource ds = new SqlgDataSource();
        Preconditions.checkState(configuration.containsKey("jdbc.url"));
        Preconditions.checkState(configuration.containsKey("jdbc.username"));
        Preconditions.checkState(configuration.containsKey("jdbc.password"));
        String connectURI = configuration.getString("jdbc.url");
        String username = configuration.getString("jdbc.username");
        String password = configuration.getString("jdbc.password");

        if (ds.cpdss.get(connectURI) != null) {
            return ds;
        }
        logger.debug(String.format("Setting up datasource to %s for user %s", connectURI, username));
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driver);
        cpds.setJdbcUrl(connectURI);
        cpds.setMaxPoolSize(configuration.getInt("maxPoolSize", 100));
        cpds.setMaxIdleTime(configuration.getInt("maxIdleTime", 500));
        if (!StringUtils.isEmpty(username)) {
            cpds.setUser(username);
        }
        if (!StringUtils.isEmpty(username)) {
            cpds.setPassword(password);
        }
        ds.cpdss.put(connectURI, cpds);
        return ds;
    }

    public void close(String jdbcUrl) {
        ComboPooledDataSource remove = this.cpdss.remove(jdbcUrl);
        try {
            if (remove != null) {
                int numBusyConnections = remove.getNumBusyConnections();
                if (numBusyConnections > 0) {
                    logger.debug("Open connection on calling close. " + numBusyConnections);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (remove != null)
                remove.close();
        }
    }

    public String getPoolStatsAsJson() {
        try {
            StringBuffer json = new StringBuffer();
            json.append("[");
            int count = 1;
            for (Map.Entry<String, ComboPooledDataSource> entry : this.cpdss.entrySet()) {
                ComboPooledDataSource comboPooledDataSource = entry.getValue();
                json.append("{\"jdbcUrl\":\"").append(entry.getKey()).append("\",");
                json.append("\"numConnections\":\"").append(String.valueOf(comboPooledDataSource.getNumConnections())).append("\",");
                json.append("\"numBusyConnections\":\"").append(String.valueOf(comboPooledDataSource.getNumConnections())).append("\",");
                json.append("\"numIdleConnections\":\"").append(String.valueOf(comboPooledDataSource.getNumConnections())).append("\",");
                json.append("\"numUnclosedOrphanedConnections\":\"").append(String.valueOf(comboPooledDataSource.getNumConnections())).append("\",");
                json.append("\"numMinPoolSize\":\"").append(String.valueOf(comboPooledDataSource.getMinPoolSize())).append("\",");
                json.append("\"numMaxPoolSize\":\"").append(String.valueOf(comboPooledDataSource.getMaxPoolSize())).append("\",");
                json.append("\"numMaxIdleTime\":\"").append(String.valueOf(comboPooledDataSource.getMaxIdleTime())).append("\"");
                json.append("}");
                if (count++ < this.cpdss.size()) {
                    json.append(",");
                }
            }
            json.append("]");
            return json.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    final static Set TO_STRING_IGNORE_PROPS = new HashSet(Arrays.asList(new String[]{
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

}
