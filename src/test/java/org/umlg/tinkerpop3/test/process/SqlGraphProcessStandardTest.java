package org.umlg.tinkerpop3.test.process;

import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.tinkerpop3.test.SqlGraphProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(SqlGraphProvider.class)
public class SqlGraphProcessStandardTest {
}
