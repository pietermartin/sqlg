package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.ltree.TestLtreeAsPrimaryKey;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestMultiThread.class
//        TestTest.class
        TestLtreeAsPrimaryKey.class
})
public class AnyTest {
}
