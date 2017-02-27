package org.umlg.sqlg.structure;

import org.apache.commons.configuration.Configuration;
import javax.sql.DataSource;

/**
 * Created by petercipov on 27/02/2017.
 */
public interface SqlgDataSourceFactory {

    SqlgDataSource setup(String driver, final Configuration configuration) throws Exception;
    SqlgDataSource setupFromJndi(String jndiName, Configuration configuration) throws Exception;

    interface SqlgDataSource {
        DataSource get(String jdbcUrl);
        void close(String jdbcUrl);
        String getPoolStatsAsJson();
    }

}
