package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestSetProperty;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestBatchCompleteVertex;
import org.umlg.sqlg.test.schema.TestLazyLoadSchema;
import org.umlg.sqlg.test.vertex.TestNewVertex;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestBatchCompleteVertex.class,
        TestBatch.class
})
public class AnyTest {
}
