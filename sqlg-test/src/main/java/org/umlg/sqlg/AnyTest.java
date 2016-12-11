package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestTraversalPerformance;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestGlobalUniqueIndex.class,
        TestTraversalPerformance.class,
//        TestMultipleThreadMultipleJvm.class
//        TestMultiThread.class
})
public class AnyTest {
}
