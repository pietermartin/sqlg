package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestAllVertices;
import org.umlg.sqlg.test.TestEdgeToDifferentLabeledVertexes;
import org.umlg.sqlg.test.TestVertexNavToEdges;
import org.umlg.sqlg.test.edges.TestLoadEdge;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.multithread.TestMultiThread;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestGremlinCompileE.class,
//        TestGremlinCompileGraphV.class,
//        TestGremlinCompileV.class,
//        TestGremlinCompileWithAs.class
//        TestGremlinCompileWithInOutV.class,
//        TestLoadEdge.class,
//        TestGremlinCompileWithHas.class
//        TestGremlinCompileGraphStep.class
        TestGremlinCompileWithAs.class
})
public class AnyTest {
}
