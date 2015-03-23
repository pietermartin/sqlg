package org.umlg.sqlg;

import org.apache.tinkerpop.gremlin.structure.BatchTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileV;
import org.umlg.sqlg.test.vertex.TestNewVertex;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestBatch.class
})
public class AnyTest {
}
