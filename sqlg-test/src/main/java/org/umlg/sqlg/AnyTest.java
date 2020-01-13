package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.properties.TestEscapedValues;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestEscapedValues.class
//        TestDropStepPartition.class,
//        TestDropStep.class,
//        TestBatchUpdatePartitioning.class
//        TestBatchNormalUpdate.class
})
public class AnyTest {
}
