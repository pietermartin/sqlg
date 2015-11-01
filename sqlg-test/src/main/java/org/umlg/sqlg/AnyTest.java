package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestEdgeToDifferentLabeledVertexes;
import org.umlg.sqlg.test.gremlincompile.TestPathStep;
import org.umlg.sqlg.test.gremlincompile.TestRepeatStep;
import org.umlg.sqlg.test.gremlincompile.TestTreeStep;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestTreeStep.class
        })
public class AnyTest {
}
