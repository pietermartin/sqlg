package org.umlg.sqlg.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.datasource.TestAdditionalPools;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestReducing.class,
//        TestRecursiveRepeatAll.class,
//        TestRecursiveRepeatWithNotStep.class,
//        TestRecursiveRepeatWithoutNotStep.class,
//        TestRepeatStepIncludeEdgeWithoutNotStep.class,
//        TestRepeatStepIncludeEdgeWithNotStep.class,
//        TestTopologyRecursiveRepeat.class
//        TestTest.class
//        TestTinkerPopSchemaProposal.class
//        TestRepeatStepGraphBoth.class
        TestAdditionalPools.class
//        TestTopologyUpgrade.class,
//        TestForeignKeysAreOptional.class,
//        TestPostgresLtree.class

})
public class AnyTest {
}
