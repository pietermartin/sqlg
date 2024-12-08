package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.recursive.TestRepeatStepIncludeEdgeWithoutNotStep;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestRepeatStepIncludeEdgeWithoutNotStep.class
//        TestTest.class,
//        TestPartitioning.class,
//        TestPartitionMultipleGraphs.class,
//        TestSubSubPartition.class,
//        TestPartitionRemove.class,
//        TestPartitionedDrop.class
})
public class AnyTest {
}
