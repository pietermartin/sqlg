package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestSetProperty;
import org.umlg.sqlg.test.TinkerpopTest;
import org.umlg.sqlg.test.batch.TestBatch;
import org.umlg.sqlg.test.batch.TestSpeed;
import org.umlg.sqlg.test.edges.TestCreateEdgeBetweenVertices;
import org.umlg.sqlg.test.edges.TestForeignKeysAreOptional;
import org.umlg.sqlg.test.schema.TestLazyLoadSchema;
import org.umlg.sqlg.test.vertex.TestNewVertex;

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
