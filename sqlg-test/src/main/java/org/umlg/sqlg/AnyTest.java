package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.sharding.TestShardingGremlin;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestSharding.class,
        TestShardingGremlin.class,
//        TestSimpleJoinGremlin.class,
//        TestSimpleVertexEdgeGremlin.class,
//        TestUserSuppliedPKBulkMode.class,
//        TestMultipleIDQuery.class,

//        TestTopologyUpgrade.class

})
public class AnyTest {
}
