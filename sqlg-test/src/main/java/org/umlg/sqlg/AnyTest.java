package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.*;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestMultiThreadedBatch;
import org.umlg.sqlg.test.edges.TestOutE;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileE;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileGraphV;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileV;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithHas;
import org.umlg.sqlg.test.index.TestForeignKeyIndexPerformance;
import org.umlg.sqlg.test.index.TestIndex;
import org.umlg.sqlg.test.mod.TestDeletedVertex;
import org.umlg.sqlg.test.mod.TestRemoveElement;
import org.umlg.sqlg.test.mod.TestRemoveProperty;
import org.umlg.sqlg.test.multithread.TestMultiThread;
import org.umlg.sqlg.test.schema.TestLazyLoadSchema;
import org.umlg.sqlg.test.vertex.TestNewVertex;
import org.umlg.sqlg.test.vertex.TestRemovedVertex;
import org.umlg.sqlg.test.vertex.TestTinkerpopBug;
import org.umlg.sqlg.test.vertex.TestVertexCache;
import org.umlg.sqlg.test.vertexout.TestVertexOutWithHas;

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
