package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.gremlincompile.TestColumnNameTranslation;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWithHas;
import org.umlg.sqlg.test.gremlincompile.TestPathStep;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestAggregate.class,
        TestPathStep.class
//        TestColumnNameTranslation.class
        })
public class AnyTest {
}
