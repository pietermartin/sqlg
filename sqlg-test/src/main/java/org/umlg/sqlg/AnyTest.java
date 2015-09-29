package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestBatchCompleteEdge;
import org.umlg.sqlg.test.batch.TestBatchCompleteVertex;
import org.umlg.sqlg.test.batch.TestServerSideEdgeCreation;
import org.umlg.sqlg.test.schema.TestLazyLoadSchema;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestServerSideEdgeCreation.class
        })
public class AnyTest {
}
