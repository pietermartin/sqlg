package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.usersuppliedpk.topology.TestSimpleVertexEdgeGremlin;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestAndStepBarrier.class,
        TestSimpleVertexEdgeGremlin.class
//        TestSharding.class,
//        TestShardingGremlin.class,
//        TestSimpleJoinGremlin.class,
//        TestSimpleVertexEdgeGremlin.class,
//        TestUserSuppliedPKBulkMode.class,
//        TestMultipleIDQuery.class,
//        TestTopologyUpgrade.class,
//        TestGremlinCompileWithHas.class,
//        TestValidateTopology.class,
//        TestGremlinCompileTextPredicate.class
//        TestAndStepBarrier.class
})
public class AnyTest {
}
