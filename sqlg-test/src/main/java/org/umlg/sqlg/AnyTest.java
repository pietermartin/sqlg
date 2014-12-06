package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestMultiThreadedBatch;
import org.umlg.sqlg.test.edges.TestCreateEdgeBetweenVertices;
import org.umlg.sqlg.test.hidden.TestHidden;
import org.umlg.sqlg.test.index.TestIndex;
import org.umlg.sqlg.test.pool.TestPoolStats;
import org.umlg.sqlg.test.schema.TestLazyLoadSchema;
import org.umlg.sqlg.test.schema.TestLoadSchema;
import org.umlg.sqlg.test.schema.TestSchema;
import org.umlg.sqlg.test.vertex.TestRemovedVertex;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestRemovedVertex.class
})
public class AnyTest {
}
