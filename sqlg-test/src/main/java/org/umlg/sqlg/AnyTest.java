package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.batch.TestBatchServerSideEdgeCreation;
import org.umlg.sqlg.test.usersuppliedpk.topology.TestSimpleJoinGremlin;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestSimpleJoinGremlin.class,
        TestBatchServerSideEdgeCreation.class,
//        TestPartitioning.class,
//        TestLoadEdge.class
})
public class AnyTest {
}
