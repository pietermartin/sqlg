package org.umlg.sqlg.test.tp3.process;

import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.umlg.sqlg.test.tp3.SqlGPostgresProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerSuite.class)
@Ignore
@ProcessComputerSuite.GraphProviderClass(SqlGPostgresProvider.class)
public class SqlGPostgresProcessComputerTest {
}
