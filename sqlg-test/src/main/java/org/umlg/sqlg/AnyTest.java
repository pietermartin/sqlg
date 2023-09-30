package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.union.TestUnion;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestHasLabelAndId.class
        TestUnion.class
//        TestTest.class
//        TestBatch.class
//        TestLoadSchemaViaNotify.class,
//        TestTopologyEdgeLabelRename.class,
//        TestTopologyActionEventsDistributed.class
//        TestTopologyEdgeLabelRenameMultipleRoles.class
//        TestTopologyPropertyColumnUpdate.class
//        TestTopologyPropertyColumnRename.class
})
public class AnyTest {
}
