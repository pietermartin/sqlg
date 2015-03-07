package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileV;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.vertex.TestVertexLabelSize;

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
