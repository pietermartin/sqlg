package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.labels.TestLabelLength;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestMultiThread.class,
//        TestDeadLock.class
//        TestBatch.class
        TestLabelLength.class
})
public class AnyTest {
}
