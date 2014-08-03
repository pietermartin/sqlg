package org.umlg.sqlg.test.tp3;

import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerSuite.class)
@Ignore
@ProcessComputerSuite.GraphProviderClass(SqlGMariaDBProvider.class)
public class SqlGMariaDBProcessComputerTest {
}
