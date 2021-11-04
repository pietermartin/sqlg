package org.umlg.sqlg.structure;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.umlg.sqlg.structure.ds.C3P0DataSource;
import org.umlg.sqlg.structure.ds.JNDIDataSource;

import java.lang.reflect.InvocationTargetException;

import static org.umlg.sqlg.structure.SqlgGraph.JDBC_URL;

/**
 * Created by petercipov on 27/02/2017.
 */
public class SqlgDataSourceFactory {
    public static SqlgDataSource create(final Configuration configuration) {
        if (null == configuration)
            throw Graph.Exceptions.argumentCanNotBeNull("configuration");
        
        try {
            
            final String clazz = configuration.getString(SqlgGraph.DATA_SOURCE, null);
            if (null == clazz) {
                final String jdbcUrl = configuration.getString(JDBC_URL, "");
                if (JNDIDataSource.isJNDIUrl(jdbcUrl)) {
                    return JNDIDataSource.create(configuration);
                } else {
                    return C3P0DataSource.create(configuration);
                }
            }
            
            final Class<?> dataSourceClass;
            try {
                dataSourceClass = Class.forName(clazz);
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(String.format("SqlgDataSourceFactory could not find [%s] - Ensure that the jar is in the classpath", clazz));
            }

            final SqlgDataSource dataSource;
            try {
                // will use create(Configuration c) to instantiate
                dataSource = (SqlgDataSource) dataSourceClass.getMethod("create", Configuration.class).invoke(null, configuration);
            } catch (final NoSuchMethodException e1) {
                throw new RuntimeException(String.format("SqlgDataSourceFactory can only instantiate SqlgDataSource implementations from classes that have a static create() method that takes a single Apache Commons Configuration argument - [%s] does not seem to have one", dataSourceClass.getName()));
            } catch (InvocationTargetException e2) {
                throw new RuntimeException(String.format("SqlgDataSourceFactory could not create this SqlgDataSource implementation [%s]", dataSourceClass.getName()), e2);
            } catch (final Exception e3) {
                throw new RuntimeException(String.format("SqlgDataSourceFactory could not instantiate this SqlgDataSource implementation [%s]", dataSourceClass.getName()), e3);
            }
            return dataSource;
            
        } catch (Exception ex) {
            // Exception handling preserves an existing behavior
            throw new IllegalStateException("Could not create sqlg data source.", ex);
        }
    }
}
