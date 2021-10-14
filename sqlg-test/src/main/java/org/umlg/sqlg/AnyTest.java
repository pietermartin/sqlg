package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.TestBatchServerSideEdgeCreation;
import org.umlg.sqlg.test.batch.TestBatchUpdatePartitioning;
import org.umlg.sqlg.test.index.TestIndexOnPartition;
import org.umlg.sqlg.test.partition.TestJoinAcrossPartition;
import org.umlg.sqlg.test.process.dropstep.TestDropStepPartition;
import org.umlg.sqlg.test.process.dropstep.TestPartitionedDrop;
import org.umlg.sqlg.test.topology.TestPartitionMultipleGraphs;
import org.umlg.sqlg.test.topology.TestPartitioning;
import org.umlg.sqlg.test.topology.TestSubSubPartition;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestJoinAcrossPartition.class,
        TestPartitioning.class,
        TestBatchServerSideEdgeCreation.class,
        TestPartitionMultipleGraphs.class, TestSubSubPartition.class, TestIndexOnPartition.class,
        TestPartitionedDrop.class, TestDropStepPartition.class, TestBatchUpdatePartitioning.class
})
public class AnyTest {
}
