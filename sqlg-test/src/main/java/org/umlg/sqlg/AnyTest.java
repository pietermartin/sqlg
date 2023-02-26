package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithHas;
import org.umlg.sqlg.test.process.dropstep.TestDropStep;
import org.umlg.sqlg.test.process.dropstep.TestDropStepPartition;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestSelect.class
//        TestGremlinCompileTextPredicate.class
//        TestPostgresLtree.class
        TestGremlinCompileWithHas.class,
        TestDropStep.class,
        TestDropStepPartition.class
//        TestIdStep.class
})
public class AnyTest {
}
