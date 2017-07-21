package org.umlg.sqlg.test.tp3.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgMSSqlServerProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using SqlG.
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = SqlgMSSqlServerProvider.class, graph = SqlgGraph.class)
public class SqlgGraphMSSqlServerProcessStandardTest {
}
