package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.complex.TestComplex;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//        TestLocalVertexStepOptional.class,
//        TestGremlinCompileChoose.class,
//        TestLocalStepCompile.class,
//        TestHas.class,
        TestComplex.class
})
public class AnyTest {
}
