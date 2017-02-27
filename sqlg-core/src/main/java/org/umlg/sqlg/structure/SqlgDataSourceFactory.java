package org.umlg.sqlg.structure;

import org.apache.commons.configuration.Configuration;
import javax.sql.DataSource;

/**
 * Created by petercipov on 27/02/2017.
 */
public interface SqlgDataSourceFactory {

    SqlgDataSource setup(String driver, final Configuration configuration) throws Exception;

    interface SqlgDataSource {
        DataSource getDatasource();
        void close();
        String getPoolStatsAsJson();
    }

}
