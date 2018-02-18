package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileTextPredicate;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestGremlinCompileTextPredicate.class
//        TestSubSubPartition.class,
//        TestPartitioning.class,
//        TestPartitionMultipleGraphs.class,
//        TestIndexOnPartition.class
})
public class AnyTest {
}
