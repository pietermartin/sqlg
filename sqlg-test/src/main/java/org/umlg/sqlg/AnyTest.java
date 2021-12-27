package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.datasource.TestJNDIInitialization;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestDataSource.class
//        TestMultiThread.class
//        TestCustomDataSource.class,
        TestJNDIInitialization.class
})
public class AnyTest {
}
