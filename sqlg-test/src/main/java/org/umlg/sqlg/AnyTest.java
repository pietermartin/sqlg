package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.TestTopologyDelete;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestHashPartitioning.class
//        TestUpgrade301.class
//        TestTopologyPropertyColumnUpdate.class
        TestTopologyDelete.class,
//        TestTopology.class
})
public class AnyTest {
}
