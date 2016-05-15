package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestAddVertexViaMap;
import org.umlg.sqlg.test.TestAllEdges;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestAddVertexViaMap.class,
        TestAllEdges.class
})
public class AnyTest {
}
