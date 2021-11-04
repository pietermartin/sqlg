package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.complex.TestGithub;
import org.umlg.sqlg.test.event.TestTinkerPopEvent;
import org.umlg.sqlg.test.fold.TestFoldStep;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileChoose;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithHas;
import org.umlg.sqlg.test.gremlincompile.TestRepeatStepGraphBoth;
import org.umlg.sqlg.test.gremlincompile.TestRepeatStepOnEdges;
import org.umlg.sqlg.test.inject.TestInject;
import org.umlg.sqlg.test.reducing.TestReducing;
import org.umlg.sqlg.test.reducing.TestReducingVertexStep;
import org.umlg.sqlg.test.schema.TestMultiThread;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestMultiThread.class,
        TestFoldStep.class,
        TestTinkerPopEvent.class,
        TestGremlinCompileWithHas.class,
        TestGremlinCompileChoose.class,
        TestReducingVertexStep.class,
        TestRepeatStepGraphBoth.class,
        TestInject.class,
        TestGithub.class,
        TestRepeatStepOnEdges.class,
        TestReducing.class
})
public class AnyTest {
}
