package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestRepeatStepVertexOut;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestLocalVertexStepLimit.class,
//        TestGremlinOptional.class,
//        TestLocalVertexStepOptional.class,
//        TestLocalVertexStepOptionalWithOrder.class,
//        TestGremlinCompileChoose.class,
//        TestOptionalWithOrder.class,
//        TestLocalVertexStepWithOrder.class,
//        TestSqlgBranchStep.class,
        TestRepeatStepVertexOut.class
})
public class AnyTest {
}
