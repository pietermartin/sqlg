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
        /**
         * This is needed for Postgresql where after a ddl statement all prepared statements need to be 'DEALLOCATED'
         * This does not completely prevent the issue as currently active connections may still execute a prepared statement
         * that Postgresql will reject with 'cached plan must not change result type'.
         */
        default void softResetPool() {
            //Do nothing
        }
        String getPoolStatsAsJson();
    }

}
