package org.umlg.sqlg.test.datasource;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.net.URL;
import java.util.Objects;

import javax.sql.DataSource;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.ds.C3P0DataSource;

/**
 * @author jgustie
 */
public class TestCustomDataSource {

    protected Configuration configuration;

    @Before
    public void before() throws ConfigurationException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        this.configuration = new PropertiesConfiguration(sqlProperties);
    }

    @Test
    public void testCustomDataSourceImplementation() {
        configuration.setProperty(SqlgGraph.DATA_SOURCE, TestSqlgDataSource.class.getName());
        try (SqlgGraph sqlgGraph = SqlgGraph.open(this.configuration)) {
            assertThat(sqlgGraph.getSqlgDataSource(), instanceOf(TestSqlgDataSource.class));
        }
    }

    /**
     * Sqlg data source implementation to use for testing.
     */
    public static class TestSqlgDataSource implements SqlgDataSource {

        public static TestSqlgDataSource create(Configuration configuration) throws Exception {
            // We cannot extend C3P0DataSoruce, but we can delegate everything to it
            return new TestSqlgDataSource(C3P0DataSource.create(configuration));
        }

        private final SqlgDataSource delegate;

        private TestSqlgDataSource(SqlgDataSource delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        public DataSource getDatasource() {
            return delegate.getDatasource();
        }

        public SqlDialect getDialect() {
            return delegate.getDialect();
        }

        public void close() {
            delegate.close();
        }

        public void softResetPool() {
            delegate.softResetPool();
        }

        public String getPoolStatsAsJson() {
            return delegate.getPoolStatsAsJson();
        }
    }
    
}
