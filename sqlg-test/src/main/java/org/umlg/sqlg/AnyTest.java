package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestArrayProperties;
import org.umlg.sqlg.test.TestLoadArrayProperties;
import org.umlg.sqlg.test.batch.TestBatchNormalPrimitiveArrays;
import org.umlg.sqlg.test.batch.TestBatchStreamVertex;
import org.umlg.sqlg.test.gremlincompile.TestBulkWithin;
import org.umlg.sqlg.test.topology.TestTopologyUpgrade;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestArrayProperties.class,
        TestLoadArrayProperties.class,
        TestBatchNormalPrimitiveArrays.class,
        TestBatchStreamVertex.class,
        TestBulkWithin.class,
        TestTopologyUpgrade.class
})
public class AnyTest {
}
