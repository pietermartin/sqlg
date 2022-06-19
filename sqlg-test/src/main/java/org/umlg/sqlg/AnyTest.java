package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.idstep.TestIdStep;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestEdgeMultiplicity.class,
//        TestEdgeMultiplicityDistributed.class,
//        TestEdgeRole.class,
//        TestPropertyCheckConstraint.class,
//        TestMultiplicityOnArrayTypes.class,
        TestIdStep.class
})
public class AnyTest {
}
