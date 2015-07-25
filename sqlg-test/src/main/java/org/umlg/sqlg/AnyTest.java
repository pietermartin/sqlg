package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestAllVertices;
import org.umlg.sqlg.test.TestEdgeToDifferentLabeledVertexes;
import org.umlg.sqlg.test.TestHas;
import org.umlg.sqlg.test.TestVertexNavToEdges;
import org.umlg.sqlg.test.edges.TestLoadEdge;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.multithread.TestMultiThread;
import org.umlg.sqlg.test.schema.TestLoadSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestLoadSchema.class
})
public class AnyTest {
}
