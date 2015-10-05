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
        TestBatchedStreaming.class,
        TestBatchServerSideEdgeCreation.class,
        TestMultiThreadedBatch.class,
        TestStreamingEdge.class,
        TestStreamVertex.class
        })
public class AnyTest {
}
