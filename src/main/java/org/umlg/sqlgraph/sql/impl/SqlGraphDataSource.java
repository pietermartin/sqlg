package org.umlg.sqlgraph.sql.impl;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
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

    public void setupDataSource(String driver, String connectURI) throws PropertyVetoException {
        if (this.cpdss.get(connectURI) != null) {
            return;
        }
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driver);
        cpds.setJdbcUrl(connectURI);
//        cpds.setUser("sa");
//        cpds.setPassword("sa");
        this.cpdss.put(connectURI, cpds);
    }

    public void close(String jdbcUrl) {
        this.cpdss.get(jdbcUrl).close();
    }

}
