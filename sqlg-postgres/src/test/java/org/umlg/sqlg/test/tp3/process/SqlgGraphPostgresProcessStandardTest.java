package org.umlg.sqlg.test.tp3.process;

import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgPostgresProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using SqlG.
 */
@RunWith(ProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = SqlgPostgresProvider.class, graph = SqlgGraph.class)
public class SqlgGraphPostgresProcessStandardTest {
}
