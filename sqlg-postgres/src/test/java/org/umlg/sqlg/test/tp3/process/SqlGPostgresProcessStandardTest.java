package org.umlg.sqlg.test.tp3.process;

import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlG;
import org.umlg.sqlg.test.tp3.SqlgPostgresProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessStandardSuite.class)
//@ProcessStandardSuite.GraphProviderClass(SqlgPostgresProvider.class)
@ProcessStandardSuite.GraphProviderClass(provider = SqlgPostgresProvider.class, graph = SqlG.class)
public class SqlGPostgresProcessStandardTest {
}
