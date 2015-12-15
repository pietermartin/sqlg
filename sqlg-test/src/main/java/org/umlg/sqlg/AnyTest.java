package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.TestVertexNavToEdges;
import org.umlg.sqlg.test.batch.*;
import org.umlg.sqlg.test.docs.DocumentationUsecases;
import org.umlg.sqlg.test.gremlincompile.*;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestRepeatStepGraphBoth.class
        })
public class AnyTest {
}
