package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.docs.DocumentationUsecases;
import org.umlg.sqlg.test.gremlincompile.TestGremlinCompileWhere;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        DocumentationUsecases.class
        })
public class AnyTest {
}
