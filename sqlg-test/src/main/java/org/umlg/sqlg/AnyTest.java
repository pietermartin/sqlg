package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.sharding.TestSharding;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestSharding.class,
//        TestShardingGremlin.class
//        TestDropStep.class
//        TestUserSuppliedPKTopology.class,
//        TestHashPartitioning.class,
//        TestJoinAcrossPartition.class,
//        TestPartitioning.class,
//        TestBatchServerSideEdgeCreation.class,
//        TestPartitionMultipleGraphs.class, TestSubSubPartition.class, TestIndexOnPartition.class,
//        TestPartitionedDrop.class, TestDropStepPartition.class, TestBatchUpdatePartitioning.class,
//        TestTopologyMultipleGraphs.class
})
public class AnyTest {
}
