package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestAllVertices;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestBatchCompleteEdge;
import org.umlg.sqlg.test.batch.TestBatchCompleteVertex;
import org.umlg.sqlg.test.gremlincompile.*;
import org.umlg.sqlg.test.json.JsonTest;
import org.umlg.sqlg.test.localdate.LocalDateTest;
import org.umlg.sqlg.test.schema.TestLazyLoadSchema;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.schema.TestLockedByCurrentThreadPerformance;
import org.umlg.sqlg.test.schema.TestSchemaManagerGetTablesFor;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestAllVertices.class
//        TestLazyLoadSchema.class
//        TestBatch.class
//        TestBatchCompleteVertex.class
//        TestBatchCompleteEdge.class
})
public class AnyTest {
}
