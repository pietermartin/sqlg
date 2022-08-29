package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.edgeMultiplicity.TestMultiplicityAddRemoveEdgeUserDefinedID;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestMultiplicityAddRemoveEdge.class,
        TestMultiplicityAddRemoveEdgeUserDefinedID.class
})
public class AnyTest {
}
