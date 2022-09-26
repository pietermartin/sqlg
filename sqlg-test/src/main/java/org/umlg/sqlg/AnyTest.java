package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestGetById;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestGetById.class,
//        TestForeignSchema.class,
//        TestColumnNameTranslation.class,
//        TestPathStep.class
//        TestEdgeMultiplicityUnique.class,
//        TestEdgeMultiplicityUniqueDistributed.class
})
public class AnyTest {
}
