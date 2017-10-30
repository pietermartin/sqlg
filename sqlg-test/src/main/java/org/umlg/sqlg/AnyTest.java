package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.barrier.andstep.TestAndStepBarrier;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestTraversalFilterStepBarrier.class,
//        TestLocalVertexStepWithOrder.class,
//        TestGremlinCompileChoose.class,
//        TestVertexStepPerformance.class,
//        TestUnoptimizedRepeatStep.class
//        TestComplex.class,
//        TestUnoptimizedRepeatStep.class,
//        TestBatchNormalUpdateDateTime.class,
//        TestWhereWithSelect.class,
//        TestOrStepBarrier.class,
        TestAndStepBarrier.class,
//        TestNotStepBarrier.class
})
public class AnyTest {
}
