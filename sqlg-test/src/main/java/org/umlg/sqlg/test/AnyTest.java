package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.services.SqlgGraphPGRoutingTest;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestRepeatStepIncludeEdgeWithoutNotStep.class,
        SqlgGraphPGRoutingTest.class
//        TestTest.class,
//        TestMultiThread.class,
//        TestForeignSchema.class,
//        TestPostgresLtree.class
//        TestPostgresLtree.class
//        TestGremlinCompileArrayContains.class
})
public class AnyTest {
}
