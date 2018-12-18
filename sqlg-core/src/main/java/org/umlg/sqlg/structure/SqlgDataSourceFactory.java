package org.umlg.sqlg.structure;

import static org.umlg.sqlg.structure.SqlgGraph.JDBC_URL;

import org.apache.commons.configuration.Configuration;
import org.umlg.sqlg.structure.ds.C3P0DataSource;
import org.umlg.sqlg.structure.ds.JNDIDataSource;

/**
 * Created by petercipov on 27/02/2017.
 */
public class SqlgDataSourceFactory {
    public static SqlgDataSource create(final Configuration configuration) throws Exception {
        final String jdbcUrl = configuration.getString(JDBC_URL);
        if (JNDIDataSource.isJNDIUrl(jdbcUrl)) {
            return JNDIDataSource.create(configuration);
        } else {
            return C3P0DataSource.create(configuration);
        }
    }
}
