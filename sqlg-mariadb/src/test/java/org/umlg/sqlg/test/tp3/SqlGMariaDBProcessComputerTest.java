package org.umlg.sqlg.test.tp3;

import com.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlG;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerSuite.class)
@Ignore
@ProcessComputerSuite.GraphProviderClass(provider = SqlGMariaDBProvider.class, graph = SqlG.class)
public class SqlGMariaDBProcessComputerTest {
}
