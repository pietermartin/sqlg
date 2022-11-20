package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.doc.DocTests;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestIoEdge.class,
//        TestNullProperties.class,
//        TestBatchedStreaming.class,
//        TestBatchServerSideEdgeCreation.class
//        TestSetProperty.class,
//        TestBatchNormalNullUpdate.class,
//        TestBatchNormalUpdate.class
//        TestBatchServerSideEdgeCreation.class
//        TestGroupCount.class,
//        TestTinkerPopFeatureTests.class,
//        TestGraphStepOrderBy.class
//        TestRepeatWithOrderAndRange.class,
        DocTests.class
})
public class AnyTest {
}
