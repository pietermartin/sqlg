package org.umlg.sqlg.test.tp3.process;

import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.test.tp3.SqlGHsqldbProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(SqlGHsqldbProvider.class)
public class SqlGHsqldbProcessStandardTest {
}
