package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.process.dropstep.TestDropStep;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestLocalVertexStepOptional.class
//        TestColumnRefactor.class,
        TestDropStep.class,
//        TestGraphStepOrderBy.class
})
public class AnyTest {
}
