package org.umlg.sqlg.test.tp3.structure;

import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlG;
import org.umlg.sqlg.test.tp3.SqlgHsqldbProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Neo4j.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = SqlgHsqldbProvider.class, graph = SqlG.class)
public class SqlgHsqldbStructureStandardTest {
}
