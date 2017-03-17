package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.index.TestIndex;

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
        TestIndex.class
})
public class AnyTest {
}
