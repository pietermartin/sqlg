package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestEdgeToDifferentLabeledVertexes;
import org.umlg.sqlg.test.TestHas;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompile;
import org.umlg.sqlg.test.schema.TestCaptureSchemaTableEdges;
import org.umlg.sqlg.test.schema.TestSchema;
import org.umlg.sqlg.test.vertexout.TestVertexOutWithHas;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestVertexOutWithHas.class
})
public class AnyTest {
}
