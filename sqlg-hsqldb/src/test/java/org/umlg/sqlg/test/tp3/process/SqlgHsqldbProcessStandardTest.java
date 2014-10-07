package org.umlg.sqlg.test.tp3.process;

import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlG;
import org.umlg.sqlg.test.tp3.SqlgHsqldbProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Hsqldb.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = SqlgHsqldbProvider.class, graph = SqlG.class)
public class SqlgHsqldbProcessStandardTest {
}
