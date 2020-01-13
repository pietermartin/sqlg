package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.TestBatchJson;
import org.umlg.sqlg.test.batch.TestBatchNormalUpdate;
import org.umlg.sqlg.test.batch.TestBatchUpdatePartitioning;
import org.umlg.sqlg.test.process.dropstep.TestDropStep;
import org.umlg.sqlg.test.process.dropstep.TestDropStepPartition;
import org.umlg.sqlg.test.properties.TestEscapedValues;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestBatchJson.class,
        TestEscapedValues.class,
        TestDropStepPartition.class,
        TestDropStep.class,
        TestBatchUpdatePartitioning.class,
        TestBatchNormalUpdate.class
})
public class AnyTest {
}
