package org.umlg.sqlg.test.tp3.structure;

import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.tp3.SqlgHsqldbProvider;


/**
 * Executes the Standard Gremlin Structure Test Suite using Hsqldb.
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = SqlgHsqldbProvider.class, graph = SqlgGraph.class)
public class SqlgHsqldbStructureStandardTest {
}
