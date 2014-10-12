package org.umlg.sqlg.test.tp3;

import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = SqlGMariaDBProvider.class, graph = SqlgGraph.class)
public class SqlgGraphMariaDBProcessStandardTest {
}
