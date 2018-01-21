package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.TestPartitionMultipleGraphs;
import org.umlg.sqlg.test.topology.TestPartitioning;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestPartitioning.class,
        TestPartitionMultipleGraphs.class,
//        TestTopologyUpgrade.class
})
public class AnyTest {
}
