package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestAllVertices;
import org.umlg.sqlg.test.TestEdgeToDifferentLabeledVertexes;
import org.umlg.sqlg.test.TestVertexNavToEdges;
import org.umlg.sqlg.test.gremlincompile.*;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestGremlinCompileE.class,
//        TestGremlinCompileGraphV.class,
//        TestGremlinCompileV.class,
//        TestGremlinCompileWithAs.class,
//        TestGremlinCompileWithHas.class,
//        TestGremlinCompileWithInOutV.class
        TestGremlinCompileWithHas.class,
        TestGremlinCompileWithAs.class
})
public class AnyTest {
}
