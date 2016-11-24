package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.schema.TestSchemaEagerCreation;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestRepeatStepGraphOut.class,
//        TestStreamVertex.class
//        TestIndex.class,
        TestSchemaEagerCreation.class,
//        TestTraversalPerformance.class,
//        TestMultiThreadedBatch.class
})
public class AnyTest {
}
