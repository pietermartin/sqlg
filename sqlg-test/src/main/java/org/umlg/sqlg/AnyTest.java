package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.*;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.edges.TestEdgeCache;
import org.umlg.sqlg.test.edges.TestEdgeSchemaCreation;
import org.umlg.sqlg.test.index.TestForeignKeyIndexPerformance;
import org.umlg.sqlg.test.multithread.TestMultiThread;
import org.umlg.sqlg.test.remove.TestRemoveEdge;
import org.umlg.sqlg.test.rollback.TestRollback;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.vertex.TestNewVertex;
import org.umlg.sqlg.test.vertex.TestPropertyOnRemovedVertex;
import org.umlg.sqlg.test.vertex.TestTinkerpopBug;
import org.umlg.sqlg.test.vertex.TestVertexCache;

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
