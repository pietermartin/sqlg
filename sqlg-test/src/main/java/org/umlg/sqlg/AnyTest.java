package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.ltree.TestPostgresLtree;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestHashPartitioning.class
//        TestUpgrade301.class
//        TestTopologyPropertyColumnUpdate.class,
//        TestTopologyPropertyColumnUpdateDistributed.class
//        TestTopologyDelete.class,
//        TestTopology.class,
//        TestPropertyCheckConstraint.class
//        DocTests.class
//        TestTest.class
//        TestTopology.class,
//        TestTopologyChangeListener.class,
//        TestTopologyDelete.class,
//        TestTopologyDeleteEdgeRole.class,
//        TestTopologyDeleteSpecific.class,
//        TestTopologyEdgeLabelRename.class,
//        TestTop
//        TestTopologyVertexLabelRename.class,
//        TestTopologyVertexLabelWithIdentifiersRename.class,
        TestPostgresLtree.class
})
public class AnyTest {
}
