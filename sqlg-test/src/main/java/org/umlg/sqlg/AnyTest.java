package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestBulkWithin;
import org.umlg.sqlg.test.gremlincompile.TestBulkWithout;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestBulkWithout.class,
        TestBulkWithin.class
        })
public class AnyTest {
}
