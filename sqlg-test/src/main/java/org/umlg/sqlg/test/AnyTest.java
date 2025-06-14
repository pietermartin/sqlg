package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.filter.connectivestep.TestAndandOrStep;
import org.umlg.sqlg.test.topology.TestTopologyLock;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestAndandOrStep.class,
        TestTopologyLock.class
//        TestBatch.class
//        TestFunctions.class
//        TestGroupCount.class
//        TestRepeatStepIncludeEdgeWithoutNotStep.class,
//        TestFunctions.class
//        TestPostgresLtree.class,
//        TestGraphStepWithIds.class,
//        TestInject.class
//        TestMultiThread.class,
//        TestForeignSchema.class,
//        TestPostgresLtree.class
//        TestPostgresLtree.class
//        TestGremlinCompileArrayContains.class
})
public class AnyTest {
}
