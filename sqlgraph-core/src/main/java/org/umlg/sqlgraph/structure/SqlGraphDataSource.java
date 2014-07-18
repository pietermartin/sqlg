package org.umlg.sqlgraph.structure;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/12
 * Time: 7:00 AM
 */
public class SqlGraphDataSource {

    public static final SqlGraphDataSource INSTANCE = new SqlGraphDataSource();
    private Map<String, ComboPooledDataSource> cpdss = new HashMap<>();

    private SqlGraphDataSource() {
    }

    public final DataSource get(String jdbcUrl) {
        return this.cpdss.get(jdbcUrl);
    }

    public void setupDataSource(String driver, String connectURI, String username, String password) throws PropertyVetoException {
        if (this.cpdss.get(connectURI) != null) {
            return;
        }
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driver);
        cpds.setJdbcUrl(connectURI);
        cpds.setUser(username);
        cpds.setPassword(password);
        this.cpdss.put(connectURI, cpds);
    }

    public void close(String jdbcUrl) {
        ComboPooledDataSource remove = this.cpdss.remove(jdbcUrl);
        try {
            if (remove != null) {
                int numBusyConnections = remove.getNumBusyConnections();
                if (numBusyConnections > 0) {
                    System.out.println("Open connection on calling close. " + numBusyConnections);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (remove != null)
                remove.close();
        }
    }

}
