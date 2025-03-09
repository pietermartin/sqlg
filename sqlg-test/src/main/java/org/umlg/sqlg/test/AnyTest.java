package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.ltree.TestPostgresLtree;
import org.umlg.sqlg.test.schema.TestMultiThread;
import org.umlg.sqlg.test.topology.TestForeignSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestTest.class,
        TestMultiThread.class,
        TestForeignSchema.class,
        TestPostgresLtree.class
//        TestPostgresLtree.class
//        TestGremlinCompileArrayContains.class
})
public class AnyTest {
}
