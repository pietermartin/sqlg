package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.index.TestIndexOnPartition;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestSubSubPartition.class,
//        TestPartitioning.class,
//        TestPartitionMultipleGraphs.class,
        TestIndexOnPartition.class
//        TestTopologyUpgrade.class
})
public class AnyTest {
}
