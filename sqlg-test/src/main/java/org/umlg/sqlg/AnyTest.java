package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileE;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileV;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithHas;
import org.umlg.sqlg.test.vertexout.TestVertexOutWithHas;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestGremlinCompileWithHas.class,
        TestGremlinCompileV.class
//        TestGremlinCompileE.class,
//        TestVertexOutWithHas.class
})
public class AnyTest {
}
