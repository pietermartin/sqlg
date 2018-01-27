package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.TestPartitionMultipleGraphs;
import org.umlg.sqlg.test.topology.TestPartitioning;
import org.umlg.sqlg.test.topology.TestSubSubPartition;
import org.umlg.sqlg.test.topology.TestTopologyUpgrade;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestSubSubPartition.class,
        TestPartitioning.class,
        TestPartitionMultipleGraphs.class,
        TestTopologyUpgrade.class
})
public class AnyTest {
}
