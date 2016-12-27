package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.doc.DocTests;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestLoadSchemaViaNotify.class,
        DocTests.class,
//        TestBatch.class,
//        TestBatchedStreaming.class,
//        TestBatchEdgeDateTime.class,
//        TestBatchGlobalUniqueIndexes.class,
//        TestBatchJson.class,
//        TestBatchModeMultipleGraphs.class,
//        TestBatchServerSideEdgeCreation.class,
//        TestBatchTemporaryVertex.class,
//        TestTraversals.class,
//        TestGremlinOptional.class
})
public class AnyTest {
}
