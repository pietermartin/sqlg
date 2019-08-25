package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileArrayContains;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileArrayOverlaps;
import org.umlg.sqlg.test.union.TestUnion;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestUnion.class,
        TestGremlinCompileArrayContains.class,
        TestGremlinCompileArrayOverlaps.class
})
public class AnyTest {
}
