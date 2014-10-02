package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.*;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.edges.TestEdgeSchemaCreation;
import org.umlg.sqlg.test.index.TestForeignKeyIndexPerformance;
import org.umlg.sqlg.test.multithread.TestMultiThread;
import org.umlg.sqlg.test.remove.TestRemoveEdge;
import org.umlg.sqlg.test.rollback.TestRollback;
import org.umlg.sqlg.test.schema.TestLoadSchema;

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
