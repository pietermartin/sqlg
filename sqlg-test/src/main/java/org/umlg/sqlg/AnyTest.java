package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.TestBatchedStreaming;
import org.umlg.sqlg.test.batch.TestStreamVertex;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestStreamVertex.class,
        TestBatchedStreaming.class
})
public class AnyTest {
}
