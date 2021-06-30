package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.inject.TestInject;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestInject.class,
//        TestRepeatStepOnEdges.class
//        TestGremlinCompileChoose.class
//        TestMultipleThreadMultipleJvm.class,
//        TestDataSource.class
//        TestReducing.class
})
public class AnyTest {
}
