package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.repeatstep.TestUnoptimizedRepeatStep;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestReducing.class,
//        TestReducingUserSuppliedIds.class
//        TestUserSuppliedPKTopology.class
        TestUnoptimizedRepeatStep.class
})
public class AnyTest {
}
