package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.TestTopologyUpgrade;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestSharding.class,
//        TestSimpleJoinGremlin.class,
//        TestShardingGremlin.class,
//        TestSimpleVertexEdgeGremlin.class,
//        TestUserSuppliedPKBulkMode.class,
//        TestMultipleIDQuery.class,

        TestTopologyUpgrade.class

})
public class AnyTest {
}
