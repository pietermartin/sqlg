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
        TestStreamingFromCsvImport.class,
//        TestBatchStreamEdge.class,
//        TestBatchedStreaming.class,
//        TestBatch.class,
//        TestBatchJson.class,
//        TestBatchEdgeDateTime.class,
//        TestBatchEdgeWithMultipleOutLabels.class,
//        TestBatchModeMultipleGraphs.class,
//        TestBatchNormalDateTime.class,
//        TestBatchNormalPrimitive.class
})
public class AnyTest {
}
