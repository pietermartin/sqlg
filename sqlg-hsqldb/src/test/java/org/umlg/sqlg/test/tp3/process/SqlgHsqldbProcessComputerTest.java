package org.umlg.sqlg.test.tp3.process;

import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlG;
import org.umlg.sqlg.test.tp3.SqlgHsqldbProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerSuite.class)
@Ignore
@ProcessComputerSuite.GraphProviderClass(provider = SqlgHsqldbProvider.class, graph = SqlG.class)
//@ProcessComputerSuite.GraphProviderClass(SqlgHsqldbProvider.class)
public class SqlgHsqldbProcessComputerTest {
}
