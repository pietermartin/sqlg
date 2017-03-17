package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.*;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestTraversalPerformance.class,
//        TestReplacedStepEmitComparator.class,
//        TestRangeLimit.class,
//        TestGraphStepOrderBy.class,
//        TestRepeatStepGraphOut.class,
//        TestOptionalWithOrder.class,
//        TestGremlinOptional.class
})
public class AnyTest {
}
