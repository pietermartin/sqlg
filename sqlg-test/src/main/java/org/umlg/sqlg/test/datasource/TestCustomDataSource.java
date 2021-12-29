package org.umlg.sqlg.test.datasource;

import org.apache.commons.configuration2.Configuration;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import javax.sql.DataSource;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.instanceOf;

/**
 * @author jgustie
 */
public class TestCustomDataSource extends BaseTest {

    private static SqlgDataSource sqlgDataSource;

    @Before
    public void before() throws Exception {
        super.before();
        sqlgDataSource = this.sqlgGraph.getSqlgDataSource();
    }

    @Test
    public void testCustomDataSourceImplementation() {
        configuration.setProperty(SqlgGraph.DATA_SOURCE, TestSqlgDataSource.class.getName());
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            MatcherAssert.assertThat(sqlgGraph.getSqlgDataSource(), instanceOf(TestSqlgDataSource.class));
        }
    }

    /**
     * Sqlg data source implementation to use for testing.
     */
    public static class TestSqlgDataSource implements SqlgDataSource {

        @SuppressWarnings("unused")
        public static TestSqlgDataSource create(Configuration configuration) throws Exception {
            // We cannot extend C3P0DataSource, but we can delegate everything to it
            return new TestSqlgDataSource(sqlgDataSource);
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
