package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.reducing.TestReducingUserSuppliedIds;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestReducing.class,
        TestReducingUserSuppliedIds.class
//        TestUserSuppliedPKTopology.class
})
public class AnyTest {
}
