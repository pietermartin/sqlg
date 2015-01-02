package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestEdgeToDifferentLabeledVertexes;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompile;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestGremlinCompile.class
})
public class AnyTest {
}
