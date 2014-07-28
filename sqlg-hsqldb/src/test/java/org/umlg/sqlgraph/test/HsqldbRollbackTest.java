package org.umlg.sqlgraph.test;

import org.junit.Test;
import org.umlg.sqlgraph.structure.SqlGraphDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Date: 2014/07/17
 * Time: 4:30 PM
 */
public class HsqldbRollbackTest extends BaseTest {

    @Test
    public void testRollback() throws SQLException {
        Connection conn = SqlGraphDataSource.INSTANCE.get(this.sqlGraph.getJdbcUrl()).getConnection();
    }

}
