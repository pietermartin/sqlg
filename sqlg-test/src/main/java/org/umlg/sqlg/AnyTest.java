package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.TestTopologyPropertyColumnUpdateDistributed;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestHashPartitioning.class
//        TestUpgrade301.class
//        TestTopologyPropertyColumnUpdate.class,
        TestTopologyPropertyColumnUpdateDistributed.class
//        TestTopologyDelete.class,
//        TestTopology.class,
//        TestPropertyCheckConstraint.class
})
public class AnyTest {
}
