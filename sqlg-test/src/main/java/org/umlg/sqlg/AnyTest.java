package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.filter.not.barrier.TestNotStepBarrier;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestOrStepBarrier.class
//    NetAggregateTest.class,
        TestNotStepBarrier.class
})
public class AnyTest {
}
