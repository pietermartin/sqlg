package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.function.TestFunctions;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestFunctions.class
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
