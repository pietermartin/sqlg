package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.repeatstep.NetAggregateTest;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestRepeatStepGraphOut.class,
//        TestUnoptimizedRepeatStep.class,
//        TestSetProperty.class,
        NetAggregateTest.class
//        TestLocalVertexStepRepeatStep.class,
//        TestHas.class
//        TestRollback.class,
//        TestVertexCache.class,
//        TestEdgeCache.class
})
public class AnyTest {
}
