package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.schema.TestSqlgSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestSqlgSchema.class
})
public class AnyTest {
}
