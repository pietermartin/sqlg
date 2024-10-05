package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.filter.or.TestOrStep;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestMultiThread.class
//        TestRecursiveRepeatWithNotStep.class,
//        TestRecursiveRepeatWithoutNotStep.class,
//        TestRepeatStepIncludeEdgeWithoutNotStep.class
//        TestAndStep.class,
        TestOrStep.class
//        TestRepeatStepGraphOut.class
//        TestUnoptimizedRepeatStep.class
//        TestGremlinOptional.class,
//        TestHasLabelAndId.class,
//        TestPropertyValues.class
//        TestRepeatStepGraphOut.class
//        TestLtreeAsPrimaryKey.class
})
public class AnyTest {
}
