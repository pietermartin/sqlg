package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestAddVertexViaMap;
import org.umlg.sqlg.test.TestAllEdges;
import org.umlg.sqlg.test.TestEdgeToDifferentLabeledVertexes;
import org.umlg.sqlg.test.TestLoadElementProperties;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestLoadElementProperties.class
})
public class AnyTest {
}
