package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.process.dropstep.TestDropStep;
import org.umlg.sqlg.test.process.dropstep.TestDropStepBarrier;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestDropStep.class,
        TestDropStepBarrier.class,
//        TestNotStepBarrier.class
})
public class AnyTest {
}
