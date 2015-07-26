package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.*;
import org.umlg.sqlg.test.edges.TestForeignKeysAreOptional;
import org.umlg.sqlg.test.edges.TestLoadEdge;
import org.umlg.sqlg.test.edges.TestOutE;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.multithread.TestMultiThread;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.schema.TestSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestGremlinCompileWithHas.class,
})
public class AnyTest {
}
