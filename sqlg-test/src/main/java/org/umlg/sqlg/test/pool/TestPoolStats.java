package org.umlg.sqlg.test.pool;

import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/11/17
 * Time: 12:56 PM
 */
public class TestPoolStats extends BaseTest {

    @Test
    public void testPoolStats() {
        System.out.println(this.sqlgGraph.getSqlgDataSource().getPoolStatsAsJson());
    }
}
