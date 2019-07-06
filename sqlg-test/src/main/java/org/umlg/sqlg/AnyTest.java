package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithAs;
import org.umlg.sqlg.test.process.dropstep.TestDropStep;
import org.umlg.sqlg.test.reducing.TestReducing;
import org.umlg.sqlg.test.reducing.TestReducingVertexStep;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestGremlinCompileWithAs.class,
        TestDropStep.class,
        TestReducing.class,
        TestReducingVertexStep.class
})
public class AnyTest {
}
