package org.umlg.sqlg.test.tp3.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgPostgresProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using SqlG.
 */
@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = SqlgPostgresProvider.class, graph = SqlgGraph.class)
public class SqlgPostgresStructureStandardTest {
}
