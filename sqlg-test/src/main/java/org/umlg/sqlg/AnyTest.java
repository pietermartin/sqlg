package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.topology.TestTopologyMultipleGraphs;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestTopologyMultipleGraphs.class,
//        TestNotifyJson.class,
//        TestIndexTopologyTraversal.class,
})
public class AnyTest {
}
