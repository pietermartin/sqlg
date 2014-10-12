package org.umlg.sqlg.test.tp3;

import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.umlg.sqlg.structure.SqlgGraph;


/**
 * Executes the Standard Gremlin Structure Test Suite using SqlG.
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = SqlGMariaDBProvider.class, graph = SqlgGraph.class)
public class SqlgGraphMariaDbStructureStandardTest {
}
