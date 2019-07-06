package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.localvertexstep.TestLocalVertexStepOptional;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestLocalVertexStepOptional.class,
//        TestGremlinCompileWithAs.class,
//        TestDropStep.class,
//        TestReducing.class,
//        TestReducingVertexStep.class
})
public class AnyTest {
}
