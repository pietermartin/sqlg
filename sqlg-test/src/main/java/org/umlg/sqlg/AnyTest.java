package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.*;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestBatch.class,
        TestBatchDateTime.class,
        TestBatchEdgeDateTime.class,
        TestBatchedStreaming.class,
        TestBatchJson.class,
        TestBatchServerSideEdgeCreation.class,
        TestBatchTemporaryVertex.class,
        TestBatchUpdate.class,
        TestMultiThreadedBatch.class,
        TestNormalBatchPrimitive.class,
        TestNormalBatchPrimitiveArrays.class,
        TestNormalBatchUpdatePrimitiveArrays.class,
        TestStreamEdge.class,
        TestStreamVertex.class
})
public class AnyTest {
}
