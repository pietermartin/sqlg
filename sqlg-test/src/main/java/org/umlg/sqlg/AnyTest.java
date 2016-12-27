package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.doc.DocTests;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        DocTests.class,
//        TestTraversals.class,
//        TestGremlinOptional.class
})
public class AnyTest {
}
