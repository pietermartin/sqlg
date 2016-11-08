package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.TestTopology;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestSchema.class,
//        TestIndex.class,
//        TestMultiThread.class
//        TestMultipleThreadMultipleJvm.class,
        TestTopology.class,
//        TestTopologyMultipleGraphs.class
})
public class AnyTest {
}
