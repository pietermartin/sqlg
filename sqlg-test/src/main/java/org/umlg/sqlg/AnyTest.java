package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.roles.TestReadOnlyRole;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestReadOnlyRole.class
//        TestUserSuppliedPKTopology.class,
//        TestAddVertexViaMap.class,
//        TestSchema.class,
//        TestIndex.class
//        TestLargeSchemaPerformance.class,
//        TestGroovy.class
})
public class AnyTest {
}
