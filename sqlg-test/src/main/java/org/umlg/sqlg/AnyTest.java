package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.edgeMultiplicity.TestEdgeMultiplicity;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestEdgeMultiplicity.class,
//        TestEdgeMultiplicityUnique.class,
//        TestEdgeMultiplicityDistributed.class,
//        TestMultiplicityAddRemoveEdge.class
//        DocTests.class
})
public class AnyTest {
}
