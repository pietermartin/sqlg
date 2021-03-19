package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.usersuppliedpk.topology.TestUserSuppliedPKTopology;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestUserSuppliedPKTopology.class
})
public class AnyTest {
}
